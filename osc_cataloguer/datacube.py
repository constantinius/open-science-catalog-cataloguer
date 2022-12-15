from os.path import basename, join, dirname
from datetime import datetime, timedelta
from urllib.parse import urlparse
import numpy as np
import re
from typing import Tuple, Optional, Any
import mimetypes
import asyncio
import logging

from osgeo import gdal
import pystac
from dateutil.parser import parse as parse_datetime

from pystac.common_metadata import CommonMetadata
from pystac.extensions.datacube import (
    DatacubeExtension,
    VerticalSpatialDimension,
    HorizontalSpatialDimension,
    TemporalDimension,
    HorizontalSpatialDimensionAxis,
    VerticalSpatialDimensionAxis,
    AdditionalDimension,
    Variable,
    VariableType,
    DimensionType,
)
from pystac import Asset


LOGGER = logging.getLogger(__name__)

gdal.UseExceptions()


async def run_in_executor(func: Any, *args) -> Any:
    """Shorthand for
    asyncio.get_running_loop().run_in_executor(None, func, *args)

    Args:
        func (Any): function to invoke

    Returns:
        Any: the result of the function
    """
    return await asyncio.get_running_loop().run_in_executor(None, func, *args)


UNIT_RE = re.compile(r"(\w+) since (.*)")


def get_time_offset_and_step(unit: str) -> Tuple[datetime, timedelta]:
    match = UNIT_RE.match(unit)
    if match:
        step_unit, offset = match.groups()
        offset = datetime.fromisoformat(offset)
        step = timedelta(**{step_unit: 1})
        return offset, step
    raise ValueError("Failed to parse time unit")


def get_dimension_type(dimension: dict) -> str:
    typ = dimension.get("type")
    if typ:
        return typ

    name = dimension["name"].lower()
    if name == "lon":
        return "HORIZONTAL_X"
    elif name == "lat":
        return "HORIZONTAL_Y"
    elif name in ("z", "elevation"):
        return "VERTICAL"
    elif name == "time":
        return "TEMPORAL"


def get_dimension_array(ds: gdal.Dataset, dimension_name: str):
    root = ds.GetRootGroup()
    md_arr = root.OpenMDArrayFromFullname(dimension_name)
    return md_arr.ReadAsArray()


async def gdal_mdiminfo_to_datacube(
    url: str,
    info: dict,
    datacube: DatacubeExtension,
    throttler: asyncio.Semaphore,
):
    """Fills the datacube information for the given GDAL multidimensional
    dataset info.

    Args:
        url (str): The path to the dataset
        info (dict): The multi dimensional information
        datacube (DatacubeExtension): The pystac datacube extension
    """

    LOGGER.debug(f"getting mdiminfo for {url}")
    dimensions = {}
    async with throttler:
        ds = gdal.OpenEx(f"/vsicurl/{url}", gdal.OF_MULTIDIM_RASTER)

    for dim in info["dimensions"]:
        typ = get_dimension_type(dim)
        # async with throttler:
        #     ds = await run_in_executor(
        #         gdal.Open,
        #         f'{info["driver"].upper()}:/vsicurl/"{url}":'
        #         f'{dim["indexing_variable"]}',
        #     )

        # indexing variables are always 1D
        async with throttler:
            data = await run_in_executor(
                get_dimension_array, ds, dim["indexing_variable"]
            )

            print(dim, data, data.shape)

        diff = np.diff(data)
        if len(diff) > 1:
            evenly_spaced = np.all(diff == diff[0])
            step = float(data[1] - data[0])
            values = [float(v) for v in data] if evenly_spaced else []
        else:
            evenly_spaced = False
            step = None
            values = [float(v) for v in data]

        extent = [float(data[0]), float(data[-1])]

        #
        array_info = info["arrays"][dim["indexing_variable"][1:]]
        unit = array_info.get("unit")

        properties = {}
        if typ in ("HORIZONTAL_X", "HORIZONTAL_Y"):
            cls = HorizontalSpatialDimension
            properties["axis"] = (
                HorizontalSpatialDimensionAxis.X
                if typ == "HORIZONTAL_X"
                else HorizontalSpatialDimensionAxis.Y
            )
        elif typ == "VERTICAL":
            cls = VerticalSpatialDimension
            properties["axis"] = VerticalSpatialDimensionAxis.Z
        elif typ == "TEMPORAL":
            cls = TemporalDimension
            # translate extent, values, step according to units
            offset, step_unit = get_time_offset_and_step(unit)
            extent = [(offset + v * step_unit).isoformat() for v in extent]
            values = [(offset + v * step_unit).isoformat() for v in values]
            if step is not None:
                # TODO: maybe refine, using days
                step = f"PT{(step_unit * step).total_seconds()}S"

            # set unit to null deliberately, as we already translated to ISO
            unit = None
        else:
            cls = AdditionalDimension
            typ = "OTHER"

        properties["type"] = typ
        properties["extent"] = extent
        properties["step"] = step
        if not evenly_spaced:
            properties["values"] = values
        if unit:
            properties["unit"] = unit

        dimensions[dim["name"]] = cls(properties)

    datacube.dimensions = dimensions

    variables = {}
    for array_name, array_info in info["arrays"].items():
        variables[array_name] = Variable(
            {
                "var_type": VariableType.DATA,
                "unit": array_info.get("unit"),
                "dimensions": [
                    dim_name[1:] for dim_name in array_info["dimensions"]
                ],
                # TODO: description
            }
        )
    datacube.variables = variables


async def extend_item(
    url: str, item: pystac.Item, throttler: asyncio.Semaphore
) -> pystac.Item:
    async with throttler:
        info = await run_in_executor(gdal.MultiDimInfo, f"/vsicurl/{url}")

    common = CommonMetadata(item)
    now = datetime.now()
    item.datetime = None


    common.created = common.updated = now

    media_type, _ = mimetypes.guess_type(url)
    asset = Asset(
        url,
        media_type=media_type,
        roles=["data"],
        extra_fields={"cube:dimensions": {}},
    )
    item.add_asset("asset", asset)
    # datacube = DatacubeExtension.ext(template, add_if_missing=True)
    datacube = DatacubeExtension.ext(asset, add_if_missing=True)

    await gdal_mdiminfo_to_datacube(url, info, datacube, throttler)
    datacube.add_to(item)

    time_dimension: Optional[TemporalDimension] = next(
        (
            dim
            for dim in datacube.dimensions.values()
            if dim.dim_type == DimensionType.TEMPORAL
        ),
        None
    )

    if "time_coverage_start" in info["attributes"]:
        # TODO: fixup wrong iso-formats '2010-1-1T12:00:00Z' -> '2010-01-01T12:00:00Z'
        common.start_datetime = parse_datetime(
            info["attributes"]["time_coverage_start"]
        )
    elif time_dimension:
        common.start_datetime = time_dimension.extent[0]

    if "time_coverage_end" in info["attributes"]:
        common.end_datetime = parse_datetime(
            info["attributes"]["time_coverage_end"]
        )
    elif time_dimension:
        common.end_datetime = time_dimension.extent[1]

    return item


# url = "https://data-cersat.ifremer.fr/projects/woc/products/theme3/ocean_currents/woc-l4-cureul-natl-1h/v2.0/2011/365/20111231-WOC-L4-CUReul-ENATL_1H-v2.0-fv2.0.nc"

# template = pystac.Item.from_file(join(dirname(__file__), "test.json"))

# result = extend_item(url, template)


# # pprint(result.to_dict())

# result.save_object(dest_href="out.json")

# links = asyncio.run(wrapper("https://data-cersat.ifremer.fr/projects/woc/products/theme3/ocean_currents/woc-l4-cureul-natl-1h/v2.0/2011/"))
# print(list(links))
