from typing import Tuple, Iterable, List, Any, Optional, Union, Callable, Type

from omegaconf import DictConfig

from . import base, cnbc, cnyes, moneydj, yahoo, megabank
from ..store import es


class OndemandScraper(base.BaseScraper):
    parsers = (
        cnbc.CnbcPageScraper,
        cnyes.CnyesPageScraper,
        moneydj.MoneydjPageScraper,
        yahoo.YahooPageScraper,
        megabank.MegabankPageScraper,
    )
    excludes = (
        cnbc.CnbcPageScraper,
        cnyes.CnyesPageScraper,
        moneydj.MoneydjPageScraper,
        yahoo.YahooPageScraper,
        megabank.MegabankPageScraper,
    )

    def startpoints(self) -> Iterable[str]:
        excluded = [scp.domain for scp in self.excludes]
        for i, u in enumerate(es.Page.scan_urls()):
            if self.max_startpoints > 0 and i > self.max_startpoints:
                break
            print(u)
            yield u

    @classmethod
    def select_parser(cls, from_url: str) -> Type[base.BasePageScraper]:
        for p in cls.parsers:
            if p.domain in from_url:
                return p
        return base.BasePageScraper

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[es.Page]:
        return cls.select_parser(from_url).parse(from_url, resolved_url, http_status, html)
