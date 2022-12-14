from typing import Optional
import logging
import asyncio
from os.path import basename, normpath

import pystac

from .node import ScrapeNode
from .datacube import extend_item


LOGGER = logging.getLogger(__name__)


async def _build_catalog(
    node: ScrapeNode,
    item_template: pystac.Item,
    collection_template: pystac.Collection,
    parent: Optional[pystac.Collection],
    throttler: asyncio.Semaphore,
) -> pystac.Catalog | pystac.Item:

    if node.sub_nodes:
        LOGGER.info(f"Creating collection for {node.href}")
        collection = collection_template.full_copy()
        collection.id = basename(normpath(node.href))

        if parent:
            parent.add_child(collection)

        await asyncio.gather(
            *[
                _build_catalog(
                    sub_node,
                    item_template,
                    collection_template,
                    collection,
                    throttler,
                )
                for sub_node in node.sub_nodes
            ]
        )
        collection.extent = pystac.Extent.from_items(collection.get_items())
        return collection

    elif node.files:
        item = item_template.full_copy()
        item.id = basename(normpath(node.href))

        await asyncio.gather(
            *[extend_item(file_, item, throttler) for file_ in node.files]
        )

        if parent:
            parent.add_item(item)

        return item


async def build_catalog(
    root: ScrapeNode,
    item_template: pystac.Item,
    collection_template: pystac.Collection,
    throttle_requests: int = 10,
) -> pystac.Catalog | pystac.Item:
    throttler = asyncio.Semaphore(throttle_requests)
    return await _build_catalog(
        root,
        item_template,
        collection_template,
        None,
        throttler,
    )
