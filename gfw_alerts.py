"""NASA FIRMS alerts fetching and normalization.

This module fetches fire alerts (FIRMS) and exposes a simple API for the FastAPI
server layer. It returns GeoJSON-ready data with mandatory fields for map
rendering (latitude, longitude, date, confidence, brightness).
"""

import os
import csv
import io
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

FIRMS_API_BASE = "https://firms.modaps.eosdis.nasa.gov/api/"
FIRMS_API_TOKEN = os.getenv("FIRMS_API_TOKEN") or os.getenv("API_KEY") or os.getenv("API_TOKEN") or ""
FIRMS_SOURCE = os.getenv("FIRMS_SOURCE") or "VIIRS_SNPP_NRT"
FIRMS_AREA = os.getenv("FIRMS_AREA") or "world"

# BBox aproximado da Amazônia para filtrar o retorno "world"
AMAZON_MIN_LON = -75.0
AMAZON_MIN_LAT = -20.0
AMAZON_MAX_LON = -44.0
AMAZON_MAX_LAT = 8.0


def _parse_float(value) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _confidence_to_score(value) -> Optional[float]:
    if value is None:
        return None
    txt = str(value).strip().lower()
    if txt in ("l", "low"):
        return 25.0
    if txt in ("n", "nominal", "medium"):
        return 60.0
    if txt in ("h", "high"):
        return 90.0
    try:
        return float(txt)
    except Exception:
        return None


def _within_amazon(lat: float, lon: float) -> bool:
    return (
        AMAZON_MIN_LON <= lon <= AMAZON_MAX_LON
        and AMAZON_MIN_LAT <= lat <= AMAZON_MAX_LAT
    )


def _normalize_record(rec: Dict) -> Optional[Dict]:
    """Normalize a single FIRMS record to expected output shape."""
    if rec is None:
        return None

    lat = rec.get("latitude") or rec.get("lat")
    lon = rec.get("longitude") or rec.get("lon")

    if lat is None or lon is None or rec.get("acq_date") is None:
        return None

    confidence_raw = rec.get("confidence", "")
    confidence = str(confidence_raw).lower()
    brightness = rec.get("brightness")

    lat_f = _parse_float(lat)
    lon_f = _parse_float(lon)
    if lat_f is None or lon_f is None:
        return None

    return {
        "latitude": lat_f,
        "longitude": lon_f,
        "date": rec.get("acq_date"),
        "confidence": confidence,
        "confidence_score": _confidence_to_score(confidence_raw),
        "brightness": float(brightness) if brightness is not None else None,
    }


def _to_geojson(features: List[Dict]) -> Dict:
    """Convert normalized records to GeoJSON FeatureCollection."""
    pts = []
    for rec in features:
        lat = rec.get("latitude")
        lon = rec.get("longitude")
        if lat is None or lon is None:
            continue

        pts.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "date": rec.get("date"),
                    "confidence": rec.get("confidence"),
                    "brightness": rec.get("brightness"),
                },
            }
        )
    return {"type": "FeatureCollection", "features": pts}


def fetch_firms_alerts(
    days: int = 1,
    min_confidence: Optional[float] = None,
    limit: int = 500,
) -> Dict:
    """Fetch and normalize FIRMS alerts, then return GeoJSON-ready dict."""

    if not FIRMS_API_TOKEN:
        # fallback: return empty with reason in metadata
        return {"type": "FeatureCollection", "features": []}

    day_range = max(1, min(int(days), 5))
    url = f"{FIRMS_API_BASE}area/csv/{FIRMS_API_TOKEN}/{FIRMS_SOURCE}/{FIRMS_AREA}/{day_range}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        text = response.text or ""
        reader = csv.DictReader(io.StringIO(text))

        normalized: List[Dict] = []
        for row in reader:
            item = _normalize_record(row)
            if item is None:
                continue

            lat = item.get("latitude")
            lon = item.get("longitude")
            if lat is None or lon is None or not _within_amazon(lat, lon):
                continue

            if min_confidence is not None:
                score = item.get("confidence_score")
                if score is None or score < float(min_confidence):
                    continue

            normalized.append(item)
            if len(normalized) >= limit:
                break

        geojson = _to_geojson(normalized)
        return geojson

    except requests.RequestException:
        pass

    # fallback para quando falhar
    return {"type": "FeatureCollection", "features": []}


def fetch_firms_alerts_as_dict(
    days: int = 1,
    limit: int = 500,
) -> List[Dict]:
    """Fetch and normalize FIRMS alerts, then return a list of dicts."""
    geojson = fetch_firms_alerts(days=days, limit=limit)
    
    features = geojson.get("features", [])
    
    alerts_list = []
    for feature in features:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coordinates = geometry.get("coordinates", [None, None])
        
        alerts_list.append({
            "latitude": coordinates[1],
            "longitude": coordinates[0],
            "alert_date": properties.get("date"),
            "confidence": properties.get("confidence"),
            "source": "firms",
        })
        
    return alerts_list

