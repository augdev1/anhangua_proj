import json
import logging
import requests
from dotenv import load_dotenv
import os
import time
from datetime import datetime
from functools import wraps

load_dotenv()


def ttl_cache(ttl_seconds: float):
    """Simple TTL cache decorator.

    Keeps results in memory for `ttl_seconds`. This speeds up repeated calls
    during a demo without changing return values.
    """

    def decorator(func):
        cache = {}

        @wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            now = time.time()
            if key in cache:
                value, expires_at = cache[key]
                if now < expires_at:
                    return value

            value = func(*args, **kwargs)
            cache[key] = (value, now + ttl_seconds)
            return value

        return wrapper

    return decorator


BASE_URL = "https://data-api.globalforestwatch.org"
DATASET = "umd_glad_landsat_alerts"  # GLAD-L alerts (covers Amazon)
VERSION = None  # use latest available version if None or "latest"
# The API key can be set as GFW_API_TOKEN or API_TOKEN in the .env file.
TOKEN = (os.getenv("GFW_API_TOKEN") or os.getenv("API_TOKEN") or "").strip()

logger = logging.getLogger(__name__)


def _normalize_coord(val):
    try:
        return round(float(val), 6)
    except Exception:
        return val


def dedupe_alerts(alerts):
    """Remove duplicate alerts using (lat, lon, alert_date)."""
    seen = set()
    unique = []
    for a in alerts:
        key = (
            _normalize_coord(a.get("latitude")),
            _normalize_coord(a.get("longitude")),
            str(a.get("alert_date")),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(a)
    return unique


def filter_alerts_by_confidence(alerts, confidence):
    """Return only alerts whose confidence matches the given value."""
    if not confidence:
        return alerts

    conf = str(confidence).strip().lower()
    if not conf:
        return alerts

    filtered = []
    for a in alerts:
        if str(a.get("confidence", "")).strip().lower() == conf:
            filtered.append(a)
    return filtered


def filter_alerts_by_bbox(alerts, min_lon, min_lat, max_lon, max_lat):
    """Return only alerts within the provided bounding box."""
    out = []
    for a in alerts:
        try:
            lon = float(a.get("longitude"))
            lat = float(a.get("latitude"))
        except Exception:
            continue
        if min_lon <= lon <= max_lon and min_lat <= lat <= max_lat:
            out.append(a)
    return out


@ttl_cache(ttl_seconds=60 * 60)
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


@ttl_cache(ttl_seconds=60 * 5)
def get_alerts_tile(lat, lng, z, token=TOKEN, confidence=None):
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
            logger.error("Erro HTTP ao buscar alertas (%s): %s", response.status_code, response.text)
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
            
        alerts = filter_alerts_by_confidence(alerts, confidence)
        alerts = dedupe_alerts(alerts)
        logger.info("get_alerts_tile: %d alerts returned (lat=%s lng=%s z=%s confidence=%s)", len(alerts), lat, lng, z, confidence)
        return alerts

    except requests.exceptions.RequestException as e:
        logger.error("Erro ao buscar alertas (tile): %s", e)
        return []


@ttl_cache(ttl_seconds=60 * 60)
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


@ttl_cache(ttl_seconds=60 * 5)
def get_alerts_amazon(token=TOKEN, days=14, confidence=None, limit=1000):
    """Fetch recent GLAD alerts limited to the Amazon basin.

    This uses the /query/json endpoint (requires API key) and filters the query
    using a geostore_id representing the Amazon basin.
    """

    if not token:
        logger.error("GFW_API_TOKEN não definido: não é possível usar /query/json.")
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
        f"LIMIT {min(max(limit, 100), 10000)}"
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
            logger.error("Erro HTTP ao buscar alertas (query/json) (%s): %s", response.status_code, response.text)
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

        alerts = filter_alerts_by_confidence(alerts, confidence)
        alerts = dedupe_alerts(alerts)
        logger.info("get_alerts_amazon: %d alerts returned (days=%s confidence=%s)", len(alerts), days, confidence)
        return alerts

    except requests.exceptions.RequestException as e:
        logger.error("Erro ao buscar alertas (query/json): %s", e)
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


def parse_alert_date(val):
    """Parseia uma string de data (ISO) e retorna objeto date.

    Se não conseguir parsear, retorna None.
    """
    if not val:
        return None
    try:
        # Tenta ISO (com ou sem tempo)
        return datetime.fromisoformat(str(val)).date()
    except Exception:
        try:
            # Fallback para formato simples YYYY-MM-DD
            return datetime.strptime(str(val), "%Y-%m-%d").date()
        except Exception:
            return None


def cluster_alerts(alerts, eps_km=1.0, min_samples=1):
    """Agrupa alertas próximos geograficamente em áreas de desmatamento.

    Usa DBSCAN com métrica haversine (distância em graus convertida para rad).
    """
    if not alerts:
        return []

    try:
        import numpy as np
        from sklearn.cluster import DBSCAN
    except ImportError:
        print("Erro: scikit-learn (e numpy) são necessários para clusterização. Instale com: pip install scikit-learn numpy")
        return []

    coords = []
    for a in alerts:
        lat = a.get("latitude")
        lon = a.get("longitude")
        if lat is None or lon is None:
            continue
        coords.append((lat, lon))

    if not coords:
        return []

    coords_rad = np.radians(np.array(coords, dtype=float))
    eps_rad = eps_km / 6371.0088  # raio médio da Terra em km

    db = DBSCAN(eps=eps_rad, min_samples=min_samples, metric="haversine")
    labels = db.fit_predict(coords_rad)

    clusters = {}
    for label, alert in zip(labels, alerts):
        # Cada label representa um grupo; -1 são pontos isolados (ruído)
        if label == -1:
            label = f"noise_{len(clusters)}"
        clusters.setdefault(label, []).append(alert)

    clustered_alerts = []
    for group in clusters.values():
        lats = [a.get("latitude") for a in group if a.get("latitude") is not None]
        lons = [a.get("longitude") for a in group if a.get("longitude") is not None]
        if not lats or not lons:
            continue

        avg_lat = sum(lats) / len(lats)
        avg_lon = sum(lons) / len(lons)

        latest = None
        for a in group:
            d = parse_alert_date(a.get("alert_date"))
            if d is None:
                continue
            if latest is None or d > latest:
                latest = d

        clustered_alerts.append({
            "lat": avg_lat,
            "lon": avg_lon,
            "alert_count": len(group),
            "latest_alert": latest.isoformat() if latest else None,
        })

    return clustered_alerts


def save_clustered_csv(clusters, output_path="clustered_alerts.csv"):
    """Salva clusters de alertas como CSV."""
    import csv

    if not clusters:
        print("Nenhuma área agrupada para salvar em CSV.")
        return

    keys = ["lat", "lon", "alert_count", "latest_alert"]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for c in clusters:
            writer.writerow({k: c.get(k, "") for k in keys})

    print(f"CSV de clusters salvo em: {output_path}")


def save_clustered_kml(clusters, output_path="clustered_alerts.kml"):
    """Salva clusters como um KML com marcadores (Placemarks)."""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        f.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n")
        f.write("<Document>\n")

        for i, c in enumerate(clusters, start=1):
            lat = c.get("lat")
            lon = c.get("lon")
            if lat is None or lon is None:
                continue

            name = "Área possível de desmatamento"
            desc = (
                f"Alertas detectados: {c.get('alert_count', 0)}\n"
                f"Último alerta: {c.get('latest_alert', '')}"
            )

            f.write("  <Placemark>\n")
            f.write(f"    <name>{name}</name>\n")
            f.write(f"    <description>{desc}</description>\n")
            f.write("    <Point>\n")
            f.write("      <coordinates>" + f"{lon},{lat},0" + "</coordinates>\n")
            f.write("    </Point>\n")
            f.write("  </Placemark>\n")

        f.write("</Document>\n")
        f.write("</kml>\n")

    print(f"KML de clusters salvo em: {output_path}")


def save_clustered_json(clusters, output_path="clustered_alerts.json"):
    """Salva clusters como um JSON simples (lista de objetos)."""
    if not clusters:
        print("Nenhuma área agrupada para salvar em JSON.")
        return

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(clusters, f, ensure_ascii=False, indent=2)

    print(f"JSON de clusters salvo em: {output_path}")


HISTORY_PATH = "alerts_history.json"
CHECK_INTERVAL_SECONDS = 30 * 60  # 30 minutos


def load_history(path=HISTORY_PATH):
    """Lê o histórico de alertas já processados."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except FileNotFoundError:
        return []
    except Exception as e:
        print(f"Erro ao ler histórico ({path}): {e}")
    return []


def save_history(history, path=HISTORY_PATH):
    """Salva o histórico de alertas processados."""
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erro ao salvar histórico ({path}): {e}")


def make_alert_key(alert):
    """Cria uma chave única para um alerta (lat, lon, alert_date)."""
    lat = alert.get("latitude")
    lon = alert.get("longitude")
    date = alert.get("alert_date")

    # Normaliza valores (strings/float) para comparação consistente.
    try:
        lat = round(float(lat), 6)
        lon = round(float(lon), 6)
    except Exception:
        lat = str(lat)
        lon = str(lon)

    return (lat, lon, str(date))


def run_monitor(interval_seconds=CHECK_INTERVAL_SECONDS):
    """Loop de monitoramento contínuo que busca novos alertas a cada intervalo."""
    history = load_history()
    seen = {make_alert_key(a) for a in history}

    while True:
        print("\nBuscando alertas de desmatamento na Amazônia (últimos 14 dias)...")
        resultados = get_alerts_amazon(TOKEN, days=14)

        if not resultados:
            print("Nenhum alerta encontrado ou ocorreu um erro.")
            time.sleep(interval_seconds)
            continue

        novos = []
        for alerta in resultados:
            if make_alert_key(alerta) not in seen:
                novos.append(alerta)

        # Exibe detalhes de cada novo alerta.
        for alerta in novos:
            print("\n⚠ NOVO ALERTA DETECTADO")
            print(f"Latitude: {alerta.get('latitude')}")
            print(f"Longitude: {alerta.get('longitude')}")
            print(f"Data: {alerta.get('alert_date')}")
            print(f"Confiança: {alerta.get('confidence')}")

        # Atualiza histórico
        for alerta in novos:
            key = make_alert_key(alerta)
            if key not in seen:
                seen.add(key)
                history.append({
                    "latitude": alerta.get("latitude"),
                    "longitude": alerta.get("longitude"),
                    "alert_date": alerta.get("alert_date"),
                })

        if novos:
            save_history(history)

        # Gera saídas de arquivos (CSV/KML/clusterizados/JSON)
        save_csv(resultados, output_path="alerts.csv")
        save_kml_polygons(resultados, output_path="alerts.kml", radius_m=30, points=72)

        clustered = cluster_alerts(resultados, eps_km=1.0)
        print(f"\nResumo do monitoramento")
        print(f"Alertas encontrados na API: {len(resultados)}")
        print(f"Alertas novos detectados: {len(novos)}")
        print(f"Alertas já conhecidos: {len(resultados) - len(novos)}")

        save_clustered_csv(clustered, output_path="clustered_alerts.csv")
        save_clustered_kml(clustered, output_path="clustered_alerts.kml")
        save_clustered_json(clustered, output_path="clustered_alerts.json")

        time.sleep(interval_seconds)


if __name__ == "__main__":
    try:
        run_monitor()
    except KeyboardInterrupt:
        print("\nMonitoramento interrompido pelo usuário.")
