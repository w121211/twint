from __future__ import annotations
import datetime
import dataclasses
import json
from typing import Iterable, List, Tuple, Optional


@dataclasses.dataclass
class TickerText:
    '''<p>...some text...<a href='$AAA'>...some anchor text</a>...<a>....</a>...</p>'''
    # p_text
    text: str
    # [(anchor_text, ticker), (...), (...), ...]
    labels: List[Tuple[str, str]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Parsed:
    keywords: List[str] = None
    tickers: List[TickerText] = None


@dataclasses.dataclass
class Page:
    from_url: str  # required
    resolved_url: str = None
    http_status: int = None
    entry_urls: List[str] = None
    entry_tickers: List[str] = None
    entry_title: str = None
    entry_summary: str = None
    entry_published_at: datetime.datetime = None
    entry_meta: dict = None
    article_metadata: dict = None
    article_published_at: datetime.datetime = None
    article_title: str = None
    article_text: str = None
    article_html: str = None  # clean_top_node
    article_summary: str = None  # meta.description
    parsed: Parsed = None  # JSON for flexible data format
    # created_at: datetime.datetime = None
    fetched_at: datetime.datetime = None
