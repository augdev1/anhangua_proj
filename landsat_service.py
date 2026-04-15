import os
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://data-api.globalforestwatch.org"
DATASET = "umd_glad_landsat_alerts"
TOKEN = (os.getenv("GFW_API_TOKEN") or os.getenv("API_TOKEN") or "").strip()

headers = {"Content-Type": "application/json"}
if TOKEN:
    headers["Authorization"] = f"Bearer {TOKEN}"
    headers["x-api-key"] = TOKEN

# get latest version
resp = requests.get(f"{BASE_URL}/dataset/{DATASET}", headers=headers, timeout=10)
resp.raise_for_status()
version = resp.json().get("data", {}).get("versions", [])[-1]

# create a geostore for our crude Amazon polygon
amazon_poly = {
    "type": "Polygon",
    "coordinates": [[
        [-75.0, -15.0],
        [-75.0, -5.0],
        [-70.0, 0.0],
        [-68.0, 5.0],
        [-62.0, 7.0],
        [-55.0, 5.0],
        [-50.0, 0.0],
        [-52.0, -10.0],
        [-58.0, -12.0],
        [-65.0, -14.0],
        [-75.0, -15.0],
    ]]
}

geo_resp = requests.post(f"{BASE_URL}/geostore/", headers=headers, json={"geometry": amazon_poly})
geo_resp.raise_for_status()
geostore_id = geo_resp.json().get("data", {}).get("gfw_geostore_id")

# Use a safer query to avoid potential "SELECT *" issues with raster datasets.
sql = (
    "SELECT latitude, longitude, umd_glad_landsat_alerts__date AS alert_date, "
    "umd_glad_landsat_alerts__confidence AS confidence "
    "FROM data "
    "LIMIT 1"
)
query_url = f"{BASE_URL}/dataset/{DATASET}/{version}/query/json"
# The API may time out on large queries. Try using pagination params to limit work.
params = {"geostore_id": geostore_id, "sql": sql, "page": 1, "page_size": 1}

import sys

for attempt in range(1, 4):
    try:
        resp = requests.get(query_url, headers=headers, params=params, timeout=90)
        resp.raise_for_status()
        break
    except requests.exceptions.RequestException as e:
        if hasattr(e, 'response') and e.response is not None:
            msg = e.response.text or str(e)
            print(f"Attempt {attempt} failed: {e.response.status_code} {msg}")
        else:
            print(f"Attempt {attempt} failed: {e}")
        if attempt == 3:
            print("Não foi possível consultar a API (erro interno no servidor). Use gfw_alerts.py para gerar KML.")
            sys.exit(0)
        print("Tentando novamente...")


data = resp.json().get("data", [])

print("rows:", len(data))
if data:
    print("keys:", list(data[0].keys()))
    import json
    print(json.dumps(data[0], indent=2))
