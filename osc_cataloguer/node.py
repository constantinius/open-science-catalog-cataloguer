from dataclasses import dataclass
from typing import List


@dataclass
class FileMatch:
    file_url: str
    parser: str


@dataclass
class ScrapeNode:
    href: str
    sub_nodes: List["ScrapeNode"]
    file_matches: List[FileMatch]
