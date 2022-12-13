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
@click.option("--collection-template", type=str)
@click.option("--out-dir", "-o", default="data", type=str)
# TODO: relevant link regex
@click.option("--debug", is_flag=True)
def catalog(
    base_href: str,
    root_href: str,
    item_template: Optional[TextIO] = None,
    collection_template: Optional[TextIO] = None,
    out_dir: str = "data",
    debug: bool = False,
):
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
    links = asyncio.run(scrape_links(base_href))

    if item_template:
        item = pystac.Item.from_file(item_template)
    else:
        item = pystac.Item("", None, None, datetime.min, {})

    if collection_template:
        collection = pystac.Collection.from_file(collection_template)
    else:
        collection = pystac.Collection(
            "",
            "",
            pystac.Extent(pystac.SpatialExtent([[]]), pystac.TemporalExtent([[]])),
        )

    catalog = asyncio.run(build_catalog(base_href, links, out_dir, item, collection))
    catalog.normalize_and_save(out_dir)


if __name__ == "__main__":
    catalog()
