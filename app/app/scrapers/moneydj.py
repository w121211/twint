import asyncio
import datetime
import dataclasses
import json
import logging
import re
from typing import Tuple, List, Iterable

import aiohttp
import pandas as pd
import elasticsearch
from bs4 import BeautifulSoup
from dateutil.parser import parse
from newspaper import Article
from omegaconf import DictConfig
from fake_useragent import UserAgent
from lxml import etree

from ..store import es
from .. import fetch
from .base import BaseScraper, BasePageScraper, TickerText, Parsed

log = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class MoneydjIndexScraper(BaseScraper):
    domain = "www.moneydj.com/KMDJ/News/NewsRealList"

    def startpoints(self) -> List[str]:
        start = self.cfg.scraper.moneydj_index.start
        until = self.cfg.scraper.moneydj_index.until
        urls = [
            f'https://www.moneydj.com/KMDJ/News/NewsRealList.aspx?index1={i}&a=MB010000' for i in range(start, until)]
        return urls

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[es.Page]:
        soup = BeautifulSoup(html, 'html.parser')
        parsed = []
        for e in soup.select('table.forumgrid tr td a'):
            p = es.Page(
                from_url=f"https://www.moneydj.com{e['href']}",
                entry_title=e['title'],)
            p.save()
            parsed.append(p)
        return parsed


class MoneydjPageScraper(BasePageScraper):
    domain = "www.moneydj.com/KMDJ/News/NewsViewer"

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> Tuple[List[es.Page], List[str]]:
        soup = BeautifulSoup(html, 'html.parser')
        e = soup.select_one('#MainContent_Contents_mainArticle')
        published_at = e.select_one('meta[itemprop=datePublished]')['content']
        meta = {f['itemprop']: f['content'] for f in e.select('meta')}

        article = Article(resolved_url, language='zh')
        article.set_html(html)
        article.parse()
        try:
            parsed = json.dumps(dataclasses.asdict(
                Parsed(keywords=article.meta_data['keywords'].split(','))), ensure_ascii=False),
        except AttributeError:
            parsed = None

        # print(json.dumps(meta, ensure_ascii=False))

        p = es.Page(
            from_url=from_url,
            resolved_url=resolved_url,
            http_status=http_status,
            entry_published_at=parse(published_at),
            entry_meta=json.dumps(meta, ensure_ascii=False),
            article_metadata=json.dumps(
                article.meta_data, ensure_ascii=False),
            article_published_at=article.publish_date,
            article_title=article.title,
            article_text=article.text,
            # article_html=etree.tostring(
            #     article.clean_top_node, encoding='utf-8').decode('utf-8'),
            parsed=parsed,
            fetched_at=datetime.datetime.now(),
        )
        p.save()

        return [p]
