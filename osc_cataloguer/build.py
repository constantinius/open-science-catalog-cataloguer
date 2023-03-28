from typing import Union, List
import logging
import asyncio
import mimetypes
from os.path import basename, normpath

import pystac

from .node import ScrapeNode, FileMatch
from .datacube import extend_item


LOGGER = logging.getLogger(__name__)


async def create_directory(
    node: ScrapeNode,
    item_template: pystac.Item,
    dir_template: pystac.Catalog,
    throttler: asyncio.Semaphore,
    single_file_items: bool,
) -> Union[pystac.Catalog, pystac.Item]:
    LOGGER.info(f"Creating collection for {node.href}")
    collection = dir_template.full_copy()
    collection.id = basename(normpath(node.href))

    sub_collections = await asyncio.gather(
        *[
            create_directory(
                sub_node,
                item_template,
                dir_template,
                throttler,
                single_file_items,
            )
            for sub_node in node.sub_nodes
            if sub_node.sub_nodes or single_file_items
        ]
    )
    collection.add_children(sub_collections)

    if single_file_items:

        items = await asyncio.gather(
            *[
                create_item(
                    file_match.file_url, [file_match], item_template, throttler
                )
                for file_match in node.file_matches
            ]
        )
    else:
        items = await asyncio.gather(
            *[
                create_item(
                    sub_node.href,
                    sub_node.file_matches,
                    item_template,
                    throttler,
                )
                for sub_node in node.sub_nodes
                if not sub_node.sub_nodes
            ]
        )
        # Add potential files as assets
        for file_match in node.file_matches:
            collection.add_asset(
                basename(file_match.file_url),
                pystac.Asset(
                    file_match.file_url,
                    media_type=mimetypes.guess_type(file_match.file_url)[0],
                ),
            )
        collection.add_link(pystac.Link("via", node.href, "text/html"))

    collection.add_items(items)
    collection.extent = pystac.Extent.from_items(collection.get_items())
    return collection


async def apply_file_match(
    file_match: FileMatch,
    item: pystac.Item,
    asset_name: str,
    throttler: asyncio.Semaphore,
):
    if file_match.parser == "basic":
        item.add_asset(
            asset_name,
            pystac.Asset(
                file_match.file_url,
                media_type=mimetypes.guess_type(file_match.file_url)[0],
                roles=["data"],
            )
        )
        return
    elif file_match.parser == "json":
        pass
    elif file_match.parser == "datacube":
        return await extend_item(
            file_match.file_url, item, asset_name, throttler
        )
    else:
        raise ValueError(f"Unknown file parser {file_match.parser}")


async def create_item(
    href: str,
    file_urls: List[FileMatch],
    template: pystac.Item,
    throttler: asyncio.Semaphore,
) -> pystac.Item:
    LOGGER.info(f"Creating item for {href}")
    item = template.full_copy()
    item.id = basename(normpath(href))
    await asyncio.gather(
        *[
            apply_file_match(
                file_url,
                item,
                "data" if len(file_urls) == 1 else f"data_{i}",
                throttler,
            )
            for i, file_url in enumerate(file_urls)
        ]
    )

    item.add_link(pystac.Link("via", href, "text/html"))

    return item


async def build_catalog(
    root: ScrapeNode,
    item_template: pystac.Item,
    dir_template: pystac.Catalog,
    throttle_requests: int = 10,
    single_file_items: bool = False,
) -> Union[pystac.Catalog, pystac.Item]:
    throttler = asyncio.Semaphore(throttle_requests)
    return await create_directory(
        root,
        item_template,
        dir_template,
        throttler,
        single_file_items,
    )
