import asyncio
from typing import List, Tuple
import logging
import re

from lxml.html import fromstring as html_fromstring
import httpx

from .node import ScrapeNode, FileMatch


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
    base_href: str,
    links: List[str],
    directory_re: re.Pattern,
    file_res: List[Tuple[re.Pattern, str]],
) -> Tuple[List[str], List[FileMatch]]:
    directories = []
    files = []
    for link in links:
        # only follow links into the archive not outside
        if directory_re.match(link) and base_href in link:
            directories.append(link)
        else:
            for file_re, parser in file_res:
                if file_re.match(link):
                    files.append(FileMatch(link, parser))
                    break

    return directories, files


async def _scrape_links(
    base_href: str,
    visited: set,
    client: httpx.AsyncClient,
    directory_re: re.Pattern,
    file_res: List[Tuple[re.Pattern, str]],
    throttler: asyncio.Semaphore,
) -> ScrapeNode:
    visited.add(base_href)
    LOGGER.info(f"scraping {base_href}")

    links = await expand_links(base_href, client, throttler)
    sub_directories, file_matches = organize_links(
        base_href, links, directory_re, file_res
    )

    sub_nodes = await asyncio.gather(
        *[
            _scrape_links(
                link, visited, client, directory_re, file_res, throttler
            )
            for link in sub_directories
            if link not in visited
        ]
    )
    LOGGER.info(f"Found {len(sub_nodes)} directories")

    return ScrapeNode(base_href, sub_nodes, file_matches)


async def scrape_links(
    base_href: str,
    throttle_requests: int = 10,
    directory_pattern: str = r"/$",
    file_patterns: List[Tuple[str, str]] = r".*",
) -> List[ScrapeNode]:
    visited = set()
    throttler = asyncio.Semaphore(throttle_requests)
    transport = httpx.AsyncHTTPTransport(retries=5)
    async with httpx.AsyncClient(transport=transport) as client:
        return await _scrape_links(
            base_href,
            visited,
            client,
            re.compile(directory_pattern),
            [
                (re.compile(file_pattern), parser)
                for file_pattern, parser in file_patterns
            ],
            throttler,
        )
