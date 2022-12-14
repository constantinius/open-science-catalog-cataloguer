from typing import NamedTuple, List


class ScrapeNode(NamedTuple):
    href: str
    sub_nodes: List["ScrapeNode"]
    files: List[str]
