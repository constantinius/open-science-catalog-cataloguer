from typing import Optional, List
import logging
import asyncio
from os.path import basename, normpath

import pystac

from .node import ScrapeNode
from .datacube import extend_item


LOGGER = logging.getLogger(__name__)


async def create_collection(
    node: ScrapeNode,
    item_template: pystac.Item,
    collection_template: pystac.Collection,
    throttler: asyncio.Semaphore,
    single_file_items: bool,
) -> pystac.Catalog | pystac.Item:
    LOGGER.info(f"Creating collection for {node.href}")
    collection = collection_template.full_copy()
    collection.id = basename(normpath(node.href))

    sub_collections = await asyncio.gather(
        *[
            create_collection(
                sub_node,
                item_template,
                collection_template,
                throttler,
            )
            for sub_node in node.sub_nodes
            if sub_node.sub_nodes
        ]
    )
    collection.add_children(sub_collections)

    if single_file_items:
        items = await asyncio.gather(
            *[
                create_item(file_, [file_], item_template, throttler)
                for file_ in node.files
            ]
        )
    else:
        items = await asyncio.gather(
            *[
                create_item(
                    sub_node.href, sub_node.files, item_template, throttler
                )
                for sub_node in node.sub_nodes
                if not sub_node.sub_nodes
            ]
        )
    collection.add_items(items)
    collection.extent = pystac.Extent.from_items(collection.get_items())
    return collection


async def create_item(
    href: str,
    file_urls: List[str],
    template: pystac.Item,
    throttler: asyncio.Semaphore,
) -> pystac.Item:

    LOGGER.info(f"Creating item for {href}")
    item = template.full_copy()
    item.id = basename(normpath(href))
    await asyncio.gather(
        *[extend_item(file_url, item, throttler) for file_url in file_urls]
    )

    return item


# async def _build_catalog(
#     node: ScrapeNode,
#     item_template: pystac.Item,
#     collection_template: pystac.Collection,
#     parent: Optional[pystac.Collection],
#     throttler: asyncio.Semaphore,
# ) -> pystac.Collection:

#     if node.sub_nodes:
#         LOGGER.info(f"Creating collection for {node.href}")
#         collection = collection_template.full_copy()
#         collection.id = basename(normpath(node.href))

#         if parent:
#             parent.add_child(collection)

#         await asyncio.gather(
#             *[
#                 _build_catalog(
#                     sub_node,
#                     item_template,
#                     collection_template,
#                     collection,
#                     throttler,
#                 )
#                 for sub_node in node.sub_nodes
#             ]
#         )
#         collection.extent = pystac.Extent.from_items(collection.get_items())
#         return collection

#     elif node.files:
#         item = item_template.full_copy()
#         item.id = basename(normpath(node.href))

#         await asyncio.gather(
#             *[extend_item(file_, item, throttler) for file_ in node.files]
#         )

#         if parent:
#             parent.add_item(item)

#         return item


async def build_catalog(
    root: ScrapeNode,
    item_template: pystac.Item,
    collection_template: pystac.Collection,
    throttle_requests: int = 10,
    single_file_items: bool = False,
) -> pystac.Catalog | pystac.Item:
    throttler = asyncio.Semaphore(throttle_requests)
    return await create_collection(
        root,
        item_template,
        collection_template,
        throttler,
        single_file_items,
    )
