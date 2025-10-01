"""Utility script for manually triggering Dagster webhook endpoints during testing.

The script iterates through a curated list of asset keys and sends a POST request to
each asset's ``report_asset_materialization`` endpoint on a locally running Dagster
webserver.  This is useful when you want to exercise automation conditions or test a
Dagster deployment without running the full orchestration stack.
"""

import requests

# http://127.0.0.1:63446/assets/facebook_ads/src/campaigns

DAGSTER_WEBSERVER_HOST = "http://127.0.0.1:63446"
DAGSTER_WEBSERVER_HOST = "http://127.0.0.1:3000"
# Representative asset key for ad-hoc experimentation with a single endpoint.
ASSET_KEY = "exchange_rate / src / usd".replace(" ", "")

# Curated sample of external asset keys that are useful when exercising webhook based
# event reporting locally.  The strings are normalized later to match Dagster's asset key
# expectations.
external_asset_keys = [
    "accounts_db / src / accounts",
    "adobe_experience / src / app_hits",
    "adobe_experience / src / web_hits",
    "entity_resolution / src / individual_party_keys",
    "exchange_rate / src / cad",
    "exchange_rate / src / usd",
    "exchange_rate / src / usd",
    "facebook_ads / src / campaigns",
    "google_ads / src / campaigns",
    "google_ads / src / criterion",
    "inventory_db / src / products",
    "transaction_db / src / transactions",
]

# Trigger each asset specific endpoint sequentially to mimic multiple external systems
# emitting materialization events.
for asset_key in external_asset_keys:
    asset_key = asset_key.replace(" ", "")
    url = f"{DAGSTER_WEBSERVER_HOST}/report_asset_materialization/{asset_key}"
    response = requests.request("POST", url)
    response.raise_for_status()
