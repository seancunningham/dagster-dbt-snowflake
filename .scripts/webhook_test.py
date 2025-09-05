import requests

# http://127.0.0.1:63446/assets/facebook_ads/src/campaigns

DAGSTER_WEBSERVER_HOST = "http://127.0.0.1:63446"
ASSET_KEY = "facebook_ads/src/campaigns"

url = f"{DAGSTER_WEBSERVER_HOST}/report_asset_materialization/{ASSET_KEY}"
response = requests.request("POST", url)
response.raise_for_status()
