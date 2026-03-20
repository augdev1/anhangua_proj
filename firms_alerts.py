"""NASA FIRMS alerts fetching and normalization.

This module fetches fire alerts (FIRMS) and exposes a simple API for the FastAPI
server layer. It returns GeoJSON-ready data with mandatory fields for map
rendering (latitude, longitude, date, confidence, brightness).
"""

import os
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

FIRMS_API_BASE = "https://firms.modaps.eosdis.nasa.gov/api/"
FIRMS_API_TOKEN = os.getenv("FIRMS_API_TOKEN") or os.getenv("API_KEY") or os.getenv("API_TOKEN") or ""


def _normalize_record(rec: Dict) -> Optional[Dict]:
    """Normalize a single FIRMS record to expected output shape."""
    if rec is None:
        return None

    lat = rec.get("latitude") or rec.get("lat")
    lon = rec.get("longitude") or rec.get("lon")

    if lat is None or lon is None or rec.get("acq_date") is None:
        return None

    confidence = str(rec.get("confidence", "")).lower()
    brightness = rec.get("brightness")

    return {
        "latitude": float(lat),
        "longitude": float(lon),
        "date": rec.get("acq_date"),
        "confidence": confidence,
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

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days)

    # Exemplo de endpoint (base). Ajuste conforme a especificação real.
    url = f"{FIRMS_API_BASE}area/csv/"
    params = {
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "limit": limit,
        "token": FIRMS_API_TOKEN,
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        # A API pode retornar CSV ou JSON; tentamos JSON primeiro.
        data = response.json() if response.headers.get("Content-Type", "").startswith("application/json") else []

        if isinstance(data, list):
            normalized = [_normalize_record(r) for r in data]
            normalized = [r for r in normalized if r is not None]

            if len(normalized) > limit:
                normalized = normalized[:limit]

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

