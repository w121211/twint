#!/bin/bash
python -m app.main run.scraper=rss run.loop_every=86400 scraper.rss.entry=./resource/rss_yahoo_us_stock.csv & \
python -m app.main run.scraper=rss run.loop_every=43200 scraper.rss.entry=./resource/rss_yahoo_us_indicies.csv & \
python -m app.main run.scraper=rss run.loop_every=3600 scraper.rss.entry=./resource/rss_news_us.csv & \
python -m app.main run.scraper=multi run.n_workers=3 run.loop_every=1800 run.max_startpoints=1000  &
# python -m app.main run.scraper=rss run.n_workers=1 run.loop_every=43200 scraper.rss.entry=./resource/rss_yahoo_tw.csv