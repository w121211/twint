import asyncio
import datetime
import logging

import hydra
from omegaconf import DictConfig

from .scrapers import cnbc, rss, cnyes

# log = logging.getLogger(__name__)
# log.addHandler(logging.StreamHandler(sys.stdout))

"""
$ cd .../twint/app
$ python -m app.main run.scraper=rss run.n_workers=1 run.loop_every=86400 
$ python -m app.main run.scraper=cnyes_api run.n_workers=1
$ python -m app.main run.scraper=cnyes_page run.n_workers=1
$ python -m app.main run.scraper=cnbc run.n_workers=1
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
    }

    scp = scrapers[cfg.run.scraper](cfg)
    asyncio.run(
        scp.run(cfg.run.n_workers, cfg.run.loop_every)
    )


if __name__ == "__main__":
    main()
