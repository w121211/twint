import asyncio
import datetime
import logging

import hydra
from omegaconf import DictConfig
import pandas as pd

from .scrapers import cnbc, rss, cnyes, moneydj

# log = logging.getLogger(__name__)
# log.addHandler(logging.StreamHandler(sys.stdout))

"""
$ cd .../twint/app
$ python -m app.main run.scraper=rss run.n_workers=1 run.loop_every=86400 
$ python -m app.main run.scraper=cnyes_api run.n_workers=1
$ python -m app.main run.scraper=cnyes_page run.n_workers=1
$ python -m app.main run.scraper=cnbc run.n_workers=1
$ python -m app.main run.scraper=moneydj_index run.n_workers=1
$ python -m app.main run.scraper=moneydj_page run.n_workers=1
"""


@hydra.main(config_path="config.yaml")
def main(cfg: DictConfig) -> None:
    logging.getLogger("elasticsearch").setLevel(logging.CRITICAL)

    print(cfg)
    scrapers = {
        "rss": rss.RssScraper,
        "cnyes_api": cnyes.CnyesApiScraper,
        "cnyes_page": cnyes.CnyesPageScraper,
        "cnbc": cnbc.CnbcScraper,
        "moneydj_index": moneydj.MoneydjIndexScraper,
        "moneydj_page": moneydj.MoneydjPageScraper,
    }

    # data = pd.read_csv('./resource/proxies.txt', sep=" ", header=None)
    data = pd.read_csv(hydra.utils.to_absolute_path(
        './resource/proxies.txt'), sep=" ", header=None)
    proxies = list(data[0])

    scp = scrapers[cfg.run.scraper](cfg, proxies)
    asyncio.run(
        scp.run(cfg.run.n_workers, cfg.run.loop_every)
    )


if __name__ == "__main__":
    main()
