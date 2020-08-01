import dataclasses
import datetime
import json
import logging
from typing import Tuple, List, Iterable

from newspaper import Article

from ..store import es
from .base import BasePageScraper, TickerText, Parsed

log = logging.getLogger(__name__)


class YahooPageScraper(BasePageScraper):
    domain = 'finance.yahoo.com'

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[es.Page]:
        a = Article(resolved_url)
        a.set_html(html)
        a.parse()

        parsed = Parsed(
            keywords=[s.strip() for s in a.meta_data['news_keywords'].split(",")])
        p = es.Page(
            from_url=from_url,
            resolved_url=resolved_url,
            http_status=http_status,
            article_metadata=json.dumps(a.meta_data, ensure_ascii=False),
            article_published_at=a.publish_date,
            article_title=a.title,
            article_text=a.text,
            article_summary=a.meta_data['description'],
            # article_html=etree.tostring(
            #     article.clean_top_node, encoding='utf-8').decode('utf-8'),
            parsed=json.dumps(dataclasses.asdict(parsed), ensure_ascii=False),
            fetched_at=datetime.datetime.now(),)
        p.save()
        return [p]
