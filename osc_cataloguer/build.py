from typing import Optional
import logging
import asyncio
from os.path import splitext, basename, dirname

import pystac

from .types import ScrapeNode
from .datacube import extend_item


LOGGER = logging.getLogger(__name__)


async def _build_catalog(
    base_href: str,
    node: ScrapeNode,
    out_dir: str,
    item_template: pystac.Item,
    collection_template: pystac.Collection,
    parent: Optional[pystac.Collection],
    throttler: asyncio.Semaphore,
) -> pystac.Catalog | pystac.Item:

    if node.sub_nodes:
        LOGGER.info(f"Creating collection for {node.href}")
        collection = collection_template.full_copy()
        collection.id = node.href

        relpath = f"./{node.href[len(base_href):]}"
        collection.set_self_href(f"{relpath}/collection.json")

        if parent:
            parent.add_child(collection)

        await asyncio.gather(
            *[
                _build_catalog(
                    base_href,
                    sub_node,
                    out_dir,
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
        relpath = f"./{dirname(node.files[0][len(base_href):])}"
        if len(node.files) == 1:
            id_ = splitext(basename(node.files[0]))[0]
            href = f"{relpath}/{id_}.json"
        else:
            relpath = dirname(relpath)
            id_ = relpath
            href = f"{relpath}/item.json"

        item = item_template.full_copy()
        item.id = id_
        item.set_self_href(href)

        await asyncio.gather(
            *[extend_item(file_, item, throttler) for file_ in node.files]
        )

        if parent:
            parent.add_item(item)

        # TODO
        return item


async def build_catalog(
    base_href: str,
    node: ScrapeNode,
    out_dir: str,
    item_template: pystac.Item,
    collection_template: pystac.Collection,
    throttle_requests: int = 10,
) -> pystac.Catalog | pystac.Item:
    throttler = asyncio.Semaphore(throttle_requests)

    return await _build_catalog(
        base_href,
        node,
        out_dir,
        item_template,
        collection_template,
        None,
        throttler,
    )
