import asyncio
from os.path import basename, dirname, join
from typing import List, Optional, Iterable, Tuple
from urllib.parse import urlparse
import logging

from lxml.html import fromstring as html_fromstring
import httpx

from .types import ScrapeNode


LOGGER = logging.getLogger(__name__)


async def expand_links(
    href: str, client: httpx.AsyncClient, throttler: asyncio.Semaphore
) -> List[str]:
    async with throttler:
        response = await client.get(href)
    doc = html_fromstring(response.content, href)
    doc.make_links_absolute()
    return doc.xpath("//a/@href")


def organize_links(
    base_href: str, links: List[str]
) -> Tuple[List[str], List[str]]:
    directories = []
    files = []
    for link in links:
        if base_href not in link:
            continue
        if urlparse(link).query:
            continue
        elif link.endswith("/"):
            directories.append(link)
        else:
            # TODO: better heuristic
            files.append(link)

    return directories, files


async def _scrape_links(
    base_href: str,
    visited: set,
    client: httpx.AsyncClient,
    throttler: asyncio.Semaphore,
) -> ScrapeNode:
    visited.add(base_href)
    LOGGER.info(f"scraping {base_href}")

    links = await expand_links(base_href, client, throttler)
    sub_directories, leaf_files = organize_links(base_href, links)

    # links = [
    #     (base_href, link)
    #     for link in await expand_links(base_href, client, throttler)
    #     if base_href in link
    #     and not urlparse(link).query
    #     and link not in visited
    # ]
    sub_nodes = await asyncio.gather(
        *[
            _scrape_links(link, visited, client, throttler)
            for link in sub_directories
            if link not in visited
        ]
    )

    return ScrapeNode(base_href, sub_nodes, leaf_files)


async def scrape_links(
    base_href: str, throttle_requests: int = 10
) -> List[Tuple[str, str]]:
    visited = set()
    throttler = asyncio.Semaphore(throttle_requests)
    async with httpx.AsyncClient() as client:
        return await _scrape_links(base_href, visited, client, throttler)


# (path, sub_catalogs, leaf_items)


# ("/2011", [
#     ("01", [], ["...nc"]),
#     ("02", [], ["...nc"]),
#     ...
# ], [])
