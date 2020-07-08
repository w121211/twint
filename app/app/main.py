import asyncio
import datetime
import logging

import hydra
from omegaconf import DictConfig

from .scrapers import cnbc, rss, cnyes

# log = logging.getLogger(__name__)
# log.addHandler(logging.StreamHandler(sys.stdout))

"""
cd .../twint/app
python -m app.main
"""


@hydra.main(config_path="config.yaml")
def main(cfg: DictConfig) -> None:
    logging.getLogger("elasticsearch").setLevel(logging.CRITICAL)

    # scp = scrapers.RssScraper(cfg.scraper.rss)
    scp = rss.RssScraper(cfg)
    # scp = cnyes.CnyesApiScraper()
    # scp = cnyes.CnyesPageScraper(cfg)
    # scp = cnbc.CnbcScraper(cfg, use_requests=False)

    # asyncio.run(
    #     scp.run(
    #         start=datetime.datetime(2020, 5, 5),
    #         # until=datetime.datetime(2010, 3, 1),
    #         n_workers=cfg.run.n_workers,
    #     )
    # )

    asyncio.run(scp.run(cfg.run.n_workers))


if __name__ == "__main__":
    main()
