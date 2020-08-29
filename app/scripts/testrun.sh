#!/bin/bash
trap "exit" INT
python -m app.main run.scraper=rss run.loop_every=60 scraper.rss.entry=./resource/rss_news_us.csv & \
python -m app.main run.scraper=yahoo run.n_workers=1 run.loop_every=1800 run.max_startpoints=1000  &
# python -m app.main run.scraper=rss run.n_workers=1 run.loop_every=43200 scraper.rss.entry=./resource/rss_yahoo_tw.csv