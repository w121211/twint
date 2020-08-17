# set up

Tools:
* docker
* elastic-search
    * https://chrome.google.com/webstore/detail/elasticsearch-head/ffmkiejjmecolpfloofpjologoblkegm

```bash
cd .../twint/app

# install required modules
pip install -r requirements.txt

# install src project as local module (for pytest)
pip install -e .

# setup elasticsearch (one-time only)
python ./app/store/es.py
```

