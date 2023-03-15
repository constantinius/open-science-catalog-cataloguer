from typing import Optional, TextIO
import logging
import asyncio
from datetime import datetime


import click
import pystac

from .scraper import scrape_links
from .build import build_catalog


@click.command()
@click.argument("base_href", type=str)
@click.option("--root-href", type=str)
@click.option("--item-template", type=str)
@click.option("--directory-template", type=str)
@click.option(
    "--directory-catalogs",
    "directory_mode",
    flag_value="catalogs",
    default=True,
)
@click.option(
    "--directory-collections", "directory_mode", flag_value="collections"
)
@click.option("--out-dir", "-o", default="data", type=str)
@click.option("--directory-pattern", default=".*/$", type=str)
@click.option(
    "--file-pattern",
    "-f",
    multiple=True,
    default=[(".*", "basic")],
    type=(str, str),
)
@click.option("--request-throttle", "-t", default=10, type=int)
@click.option("--single-file-items", is_flag=True)
@click.option("--debug", is_flag=True)
def catalog(
    base_href: str,
    root_href: str,
    item_template: Optional[TextIO] = None,
    directory_template: Optional[TextIO] = None,
    directory_mode: str = "catalogs",
    out_dir: str = "data",
    directory_pattern: str = ".*/$",
    file_pattern: str = [(".*", "basic")],
    request_throttle: int = 10,
    single_file_items: bool = False,
    debug: bool = False,
):
    print(single_file_items)
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
    links = asyncio.run(
        scrape_links(
            base_href,
            request_throttle,
            directory_pattern,
            file_pattern,
        )
    )

    if item_template:
        item_template = pystac.Item.from_file(item_template)
    else:
        item_template = pystac.Item("", None, None, datetime.min, {})

    if directory_template:
        dir_template = pystac.read_file(directory_template)
    elif directory_mode == "collections":
        dir_template = pystac.Collection(
            "",
            "",
            pystac.Extent(
                pystac.SpatialExtent([[]]), pystac.TemporalExtent([[]])
            ),
        )
    elif directory_mode == "catalogs":
        dir_template = pystac.Catalog("", "")

    catalog = asyncio.run(
        build_catalog(
            links,
            item_template,
            dir_template,
            request_throttle,
            single_file_items,
        )
    )
    catalog.normalize_and_save(
        out_dir, catalog_type=pystac.CatalogType.SELF_CONTAINED
    )


if __name__ == "__main__":
    catalog()
