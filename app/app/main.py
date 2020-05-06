import asyncio
import datetime
import json
import logging
import sys
import time
from typing import Tuple, List

import aiohttp
import feedparser
import hydra
import pandas as pd
from hydra import utils
from omegaconf import DictConfig
from newspaper import Article, ArticleException

# from .scrapers import cnbc, rss
from . import scrapers

# log = logging.getLogger(__name__)
# log.addHandler(logging.StreamHandler(sys.stdout))


@hydra.main(config_path="config.yaml")
def main(cfg: DictConfig) -> None:
    # scp = scrapers.RssScraper(cfg.scraper.rss)
    scp = scrapers.CnyesApiScraper()

    asyncio.run(
        scp.run(start=datetime.datetime.now() - datetime.timedelta(days=1),
                n_workers=cfg.run.n_workers,)
    )


if __name__ == "__main__":
    main()
