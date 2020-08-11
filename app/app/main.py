"""
$ cd .../twint/app
$ python -m app.main run.scraper=rss run.n_workers=1 run.loop_every=86400 
$ python -m app.main run.scraper=multi run.n_workers=1 run.loop_every=86400
$ python -m app.main run.scraper=cnyes_api run.n_workers=1
$ python -m app.main run.scraper=cnyes_page run.n_workers=1
$ python -m app.main run.scraper=cnbc run.n_workers=1 run.max_startpoints=1000 run.loop_every=3600 
$ python -m app.main run.scraper=moneydj_index run.n_workers=1 scraper.moneydj_index.until=3500 run.startpoints_csv=./outputs/2020-08-09/17-13-53/error_urls.csv
$ python -m app.main run.scraper=moneydj_page run.n_workers=1
"""

import asyncio
import datetime
import logging

import hydra
from omegaconf import DictConfig
import pandas as pd

from .scrapers import cnbc, rss, cnyes, moneydj, multi
from .store import es


@hydra.main(config_path="config.yaml")
def main(cfg: DictConfig) -> None:
    print(cfg)
    logging.getLogger("elasticsearch").setLevel(logging.CRITICAL)

    scrapers = {
        "rss": rss.RssScraper,
        "multi": multi.MultiDomainPageScraper,
        "cnbc": cnbc.CnbcPageScraper,
        "cnyes_api": cnyes.CnyesApiScraper,
        "cnyes_page": cnyes.CnyesPageScraper,
        "moneydj_index": moneydj.MoneydjIndexScraper,
        "moneydj_page": moneydj.MoneydjPageScraper,
    }

    # read proxies
    data = pd.read_csv(hydra.utils.to_absolute_path(
        cfg.proxy.csv), sep=" ", header=None)
    proxies = list(data[0])

    es.connect()
    scp = scrapers[cfg.run.scraper](cfg, proxies)
    asyncio.run(
        scp.run(cfg.run.n_workers, cfg.run.loop_every)
    )


if __name__ == "__main__":
    main()
