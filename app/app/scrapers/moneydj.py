import asyncio
import datetime
import dataclasses
import json
import logging
import re
from typing import Tuple, List, Iterable
from urllib.parse import urljoin

import aiohttp
import pandas as pd
import elasticsearch
from bs4 import BeautifulSoup
from dateutil.parser import parse as dateparse
from newspaper import Article
from omegaconf import DictConfig
from fake_useragent import UserAgent
from lxml import etree

from .base import BaseScraper, BasePageScraper
from ..store import model

log = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class MoneydjIndexScraper(BaseScraper):
    # domain = "www.moneydj.com/KMDJ/News/NewsRealList"

    def startpoints(self) -> List[str]:
        start = self.cfg.scraper.moneydj_index.start
        until = self.cfg.scraper.moneydj_index.until
        urls = [f'https://www.moneydj.com/KMDJ/News/NewsRealList.aspx?index1={i}&a=MB010000'
                for i in range(start, until)]
        return urls

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[model.Page]:
        base = "https://www.moneydj.com"
        soup = BeautifulSoup(html, 'html.parser')
        return [
            model.Page(
                from_url=urljoin(base, e['href']),
                entry_title=e['title'],)
            for e in soup.select('table.forumgrid tr td a')]


class MoneydjPageScraper(BasePageScraper):
    # domain = "www.moneydj.com/KMDJ/News/NewsViewer"
    domain = "www.moneydj.com"

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[model.Page]:
        soup = BeautifulSoup(html, 'html.parser')
        e = soup.select_one('#MainContent_Contents_mainArticle')
        published_at = e.select_one('meta[itemprop=datePublished]')['content']
        meta = {f['itemprop']: f['content'] for f in e.select('meta')}

        article = Article(resolved_url, language='zh')
        article.set_html(html)
        article.parse()
        try:
            parsed = model.Parsed(
                keywords=article.meta_data['keywords'].split(','))
        except AttributeError:
            parsed = None
        return [
            model.Page(
                from_url=from_url,
                resolved_url=resolved_url,
                http_status=http_status,
                entry_published_at=dateparse(published_at),
                entry_meta=meta,
                article_metadata=dict(article.meta_data),
                article_published_at=article.publish_date,
                article_title=article.title,
                article_text=article.text,
                parsed=parsed,
                fetched_at=datetime.datetime.now(),
            )
        ]
