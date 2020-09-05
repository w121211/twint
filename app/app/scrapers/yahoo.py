import dataclasses
import datetime
import json
import logging
from typing import Tuple, List, Iterable

from newspaper import Article

from ..store import es, model
from .base import BasePageScraper

log = logging.getLogger(__name__)


class YahooPageScraper(BasePageScraper):
    domain = 'finance.yahoo.com'

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[model.Page]:
        a = Article(resolved_url)
        a.set_html(html)
        a.parse()
        try:
            parsed = model.Parsed(
                keywords=[s.strip() for s in a.meta_data['news_keywords'].split(",")])
        except:
            parsed = None
        return [
            model.Page(
                from_url=from_url,
                resolved_url=resolved_url,
                http_status=http_status,
                article_metadata=dict(a.meta_data),
                article_published_at=a.publish_date,
                article_title=a.title,
                article_text=a.text,
                article_summary=a.meta_data['description'],
                parsed=parsed,
                fetched_at=datetime.datetime.now(),
            )
        ]
