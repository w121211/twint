import datetime
import json
import logging
from typing import Tuple, Iterable, List, Any, Optional, Union, Callable, Type

import aiohttp
from newspaper import Article

from .base import ua, BaseScraper, BasePageScraper
from ..store import model

log = logging.getLogger(__name__)


class MegabankApiScraper(BaseScraper):
    def startpoints(self) -> List[str]:
        try:
            for u in super().startpoints():
                yield u
        except:
            start = datetime.datetime(*self.cfg.scraper.megabank_api.start)
            until = self.cfg.scraper.megabank_api.until
            until = datetime.datetime(
                *until) if until is not None else datetime.datetime.now()

            while start <= until:
                url = f"https://fund.megabank.com.tw/ETFData/djjson/ETNEWSjson.djjson?a=1&b={start.date()}&P1=mega&P2=&P3=true&P4=false&P5=false"
                print(url)
                yield url
                start += datetime.timedelta(days=3)

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[model.Page]:
        j = json.loads(html)
        pages = [
            model.Page(
                from_url=f"https://fund.megabank.com.tw/ETFData/djhtm/ETNEWSContentMega.djhtm?TYPE=2&DATE=&PAGE=1&A={e['V3']}",
                entry_title=e['V2'],
                entry_published_at=datetime.datetime.strptime(e['V1'], '%Y/%m/%d'),)
            for e in j['ResultSet']['Result']
        ]
        return pages


class MegabankPageScraper(BasePageScraper):
    domain = "megabank.com.tw"

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[model.Page]:
        a = Article(resolved_url, language='zh', fetch_images=False)
        a.set_html(html)
        a.parse()
        return [
            model.Page(
                from_url=from_url,
                resolved_url=resolved_url,
                http_status=http_status,
                article_text=a.text,
                fetched_at=datetime.datetime.now(),
            )
        ]
