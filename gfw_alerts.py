import json
import requests
from dotenv import load_dotenv
import os

load_dotenv()


BASE_URL = "https://data-api.globalforestwatch.org"
DATASET = "umd_glad_landsat_alerts"  # GLAD-L alerts (covers Amazon)
VERSION = None  # use latest available version if None or "latest"
# The API key can be set as GFW_API_TOKEN or API_TOKEN in the .env file.
TOKEN = (os.getenv("GFW_API_TOKEN") or os.getenv("API_TOKEN") or "").strip()


def resolve_dataset_version(dataset, version=None, token=None):
    """Resolve the latest available dataset version from the GFW API."""
    if version and version != "latest":
        return version

    url = f"{BASE_URL}/dataset/{dataset}"
    headers = get_headers(token) if token else {}
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()

    data = response.json().get("data", {})
    versions = data.get("versions") or []
    if not versions:
        raise RuntimeError(f"No versions available for dataset {dataset}")

    return versions[-1]


def get_headers(token):
    headers = {"Content-Type": "application/json"}
    if token:
        # The GFW DATA API accepts API keys via either Authorization: Bearer or x-api-key.
        # Send both to maximize compatibility.
        headers["Authorization"] = f"Bearer {token}"
        headers["x-api-key"] = token
    return headers


def get_alerts_tile(lat, lng, z, token=TOKEN):
    """Fetch deforestation alerts for a given map tile (lat/lng/z).

    This is the standard parameters used by the GFW DATA API for /features.

    Note: a number of datasets (especially raster-based ones) do not implement
    /features, and will return 501 Not Implemented (as returned by the API).
    """

    version = resolve_dataset_version(DATASET, VERSION, token)
    url = f"{BASE_URL}/dataset/{DATASET}/{version}/features"

    params = {"lat": lat, "lng": lng, "z": z}

    try:
        response = requests.get(url, headers=get_headers(token), params=params)
        if not response.ok:
            print(f"Erro HTTP ao buscar alertas ({response.status_code}): {response.text}")
            return []

        data = response.json()
        
        alerts = []
        features = data.get("data", [])
        
        for feature in features:
            props = feature.get("attributes", feature)
            
            alert = {
                "id": props.get("id") or props.get("objectid"),
                "alert_date": props.get("alert__date") or props.get("alert_date"),
                "latitude": props.get("lat") or props.get("latitude"),
                "longitude": props.get("long") or props.get("longitude"),
                "confidence": props.get("confidence__cat") or props.get("confidence"),
                "source": "GLAD-Landsat",
                "alert_type": "GLAD-S2",
                "area_ha": props.get("area__ha") or props.get("area_ha")
            }
            alerts.append(alert)
            
        return alerts

    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar alertas: {e}")
        return []


def create_amazon_geostore(token=TOKEN):
    """Create (or retrieve) a Geostore representing the Amazon basin.

    The GFW Data API allows spatial filtering in queries via a geostore_id. This
    is more reliable than using a bounding box, since it can follow the actual
    Amazon basin shape.
    """

    if not token:
        return None

    url = f"{BASE_URL}/geostore/"
    # A more restrictive polygon that stays inside Brazil (Amazônia Legal),
    # avoiding adjacent countries (Peru/Colômbia/Venezuela) as much as possible.
    #
    # NOTE: This is a simplified approximation; some fringe areas of the Brazilian
    # Amazon may be excluded for safety.
    amazon_poly = {
        "type": "Polygon",
        "coordinates": [[
            [-72.0, -15.0],
            [-72.0,  -7.0],
            [-69.0,   2.0],
            [-61.0,   5.5],
            [-52.0,   5.0],
            [-44.0,   0.0],
            [-48.0,  -6.0],
            [-56.0,  -9.0],
            [-64.0, -12.0],
            [-72.0, -15.0],
        ]]
    }

    payload = {"geometry": amazon_poly}

    try:
        response = requests.post(url, headers=get_headers(token), json=payload, timeout=30)
        if not response.ok:
            print(f"Erro ao criar geostore da Amazônia ({response.status_code}): {response.text}")
            return None

        data = response.json().get("data") or {}
        return data.get("gfw_geostore_id")

    except requests.exceptions.RequestException as e:
        print(f"Erro ao criar geostore da Amazônia: {e}")
        return None


def get_alerts_amazon(token=TOKEN, days=14):
    """Fetch recent GLAD alerts limited to the Amazon basin.

    This uses the /query/json endpoint (requires API key) and filters the query
    using a geostore_id representing the Amazon basin.
    """

    if not token:
        print("Precisa definir GFW_API_TOKEN no .env para usar /query/json (API key).")
        return []

    version = resolve_dataset_version(DATASET, VERSION, token)
    url = f"{BASE_URL}/dataset/{DATASET}/{version}/query/json"

    from datetime import date, timedelta

    since = (date.today() - timedelta(days=days)).isoformat()

    # For raster-based datasets the query must refer to pixel layers (fields).
    # The fields for GLAD Landsat alerts include:
    #  - umd_glad_landsat_alerts__date
    #  - umd_glad_landsat_alerts__confidence
    #  - latitude, longitude (reserved fields)
    sql = (
        "SELECT latitude, longitude, umd_glad_landsat_alerts__date AS alert_date, "
        "umd_glad_landsat_alerts__confidence AS confidence "
        "FROM data "
        f"WHERE umd_glad_landsat_alerts__date >= '{since}' "
        "LIMIT 500"
    )

    geostore_id = create_amazon_geostore(token)
    if not geostore_id:
        return []

    params = {
        "geostore_id": geostore_id,
        "sql": sql,
    }

    try:
        response = requests.get(url, headers=get_headers(token), params=params, timeout=60)
        if not response.ok:
            print(f"Erro HTTP ao buscar alertas (query/json) ({response.status_code}): {response.text}")
            return []

        data = response.json().get("data") or []

        # Normalize records so we always include a `source` column in exports.
        alerts = []
        for row in data:
            alerts.append({
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "alert_date": row.get("alert_date"),
                "confidence": row.get("confidence"),
                "source": "GLAD-Landsat",
            })

        return alerts

    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar alertas (query/json): {e}")
        return []


def save_geojson(alerts, output_path="alerts.geojson"):
    """Save alerts as a GeoJSON FeatureCollection (Point) for easy mapping/import."""
    features = []
    for alert in alerts:
        lat = alert.get("latitude")
        lon = alert.get("longitude")
        if lat is None or lon is None:
            continue
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {k: v for k, v in alert.items() if k not in ("latitude", "longitude")},
        })

    geojson = {"type": "FeatureCollection", "features": features}
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    print(f"GeoJSON salvo em: {output_path}")


def save_csv(alerts, output_path="alerts.csv"):
    """Save alerts as CSV with latitude/longitude columns for mapping tools."""
    import csv

    if not alerts:
        print("Nenhum alerta para salvar em CSV.")
        return

    # Determine column order: keep latitude/longitude first, then remaining keys.
    keys = ["latitude", "longitude"]
    keys += [k for k in alerts[0].keys() if k not in keys]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for alert in alerts:
            writer.writerow({k: alert.get(k, "") for k in keys})

    print(f"CSV salvo em: {output_path}")


def save_kml_polygons(alerts, output_path="alerts.kml", radius_m=30, points=72):
    """Save alerts as KML with small polygon buffers to show area.

    The generated KML creates a small polygon around each point (approximate circle)
    so you can visualize the alert area instead of only points.

    A higher `points` value makes the polygon appear more circular.
    """
    import math

    def meters_to_degrees(lat, meters):
        # approximate conversion; 1 deg lat ~ 111km, 1 deg lon varies with cos(lat)
        dlat = meters / 111000
        dlon = meters / (111000 * math.cos(math.radians(lat)))
        return dlat, dlon

    def point_buffer(lat, lon, radius_m, steps):
        dlat, dlon = meters_to_degrees(lat, radius_m)
        coords = []
        for i in range(steps):
            angle = 2 * math.pi * i / steps
            dy = math.sin(angle) * dlat
            dx = math.cos(angle) * dlon
            coords.append((lon + dx, lat + dy))
        coords.append(coords[0])
        return coords

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        f.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n")
        f.write("<Document>\n")
        for i, alert in enumerate(alerts, start=1):
            lat = alert.get("latitude")
            lon = alert.get("longitude")
            if lat is None or lon is None:
                continue
            alert_date = alert.get("alert_date", "")
            confidence = alert.get("confidence", "")
            name = f"Alert {i}"
            desc = f"date: {alert_date}, confidence: {confidence}"
            coords = point_buffer(lat, lon, radius_m, points)
            coord_text = " ".join(f"{x},{y},0" for x, y in coords)

            f.write("  <Placemark>\n")
            f.write(f"    <name>{name}</name>\n")
            f.write(f"    <description>{desc}</description>\n")
            f.write("    <Polygon>\n")
            f.write("      <outerBoundaryIs>\n")
            f.write("        <LinearRing>\n")
            f.write("          <coordinates>\n")
            f.write(f"            {coord_text}\n")
            f.write("          </coordinates>\n")
            f.write("        </LinearRing>\n")
            f.write("      </outerBoundaryIs>\n")
            f.write("    </Polygon>\n")
            f.write("  </Placemark>\n")
        f.write("</Document>\n")
        f.write("</kml>\n")

    print(f"KML salvo em: {output_path}")


if __name__ == "__main__":
    print("Buscando alertas de desmatamento na Amazônia (últimos 14 dias)...")
    resultados = get_alerts_amazon(TOKEN, days=14)
    
    if resultados:
        print(f"{len(resultados)} alertas encontrados. Exibindo os 5 primeiros:")
        for alerta in resultados[:5]:
            print(alerta)

        save_csv(resultados, output_path="alerts.csv")
        # Usamos um polígono com mais vértices para ficar mais redondo.
        save_kml_polygons(resultados, output_path="alerts.kml", radius_m=30, points=72)

        print("\nPara ver no Google My Maps (ou outros mapas que aceitem CSV):\n 1) Abra https://www.google.com/mymaps\n 2) Crie um novo mapa\n 3) Clique em 'Importar' e selecione 'alerts.csv' gerado")
        print("\nPara ver áreas aproximadas em KML (exibe pequenos polígonos ao redor de cada ponto):\n 1) Abra https://www.google.com/mymaps\n 2) Crie/abra um mapa\ 3) Clique em 'Importar' e selecione 'alerts.kml'")
    else:
        print("Nenhum alerta encontrado ou ocorreu um erro.")
