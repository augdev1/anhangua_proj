"""FastAPI server exposing alerts endpoints for frontend consumption.

This server wraps the existing `gfw_alerts.py` logic and provides simple
endpoints to fetch alerts and clustered alerts as JSON.
"""

import logging
import os

from datetime import date, datetime
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any, Dict, List, Optional

import alerts_service
import firms_alerts
import gfw_alerts

# Setup basic logging to file (and console via uvicorn if running)
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "alerts_api.log")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("anhangua_api")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)


def _standard_response(data: Any) -> Dict[str, Any]:
    """Wrap response data in a consistent envelope."""
    if isinstance(data, list):
        count = len(data)
    elif isinstance(data, dict) and "features" in data:
        # GeoJSON format, leave untouched (FeatureCollection)
        return data
    else:
        count = 1

    return {"status": "ok", "count": count, "data": data}


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except Exception:
        return None

# Simple in-memory rate-limiting by client IP + endpoint
_rate_limits = {}
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 20


def _check_rate_limit(request: Request, endpoint: str):
    now = int(datetime.now().timestamp())
    client_ip = request.client.host if request.client else "unknown"
    key = f"{client_ip}:{endpoint}"

    entry = _rate_limits.get(key, {"count": 0, "first_at": now})
    if now - entry["first_at"] >= RATE_LIMIT_WINDOW:
        entry = {"count": 1, "first_at": now}
    else:
        entry["count"] += 1

    _rate_limits[key] = entry

    if entry["count"] > RATE_LIMIT_MAX:
        return False
    return True


app = FastAPI(
    title="Anhangua GFW Alerts API",
    description="Serves deforestation alert data fetched from Global Forest Watch.",
    version="0.1.0",
)


class Alert(BaseModel):
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    alert_date: Optional[str] = None
    confidence: Optional[str] = None
    source: Optional[str] = None
    alert_type: Optional[str] = None
    area_ha: Optional[float] = None


class Cluster(BaseModel):
    lat: float
    lon: float
    alert_count: int
    latest_alert: Optional[str]


@app.get("/alertas/tile", response_model=List[Alert])
def alerts_tile(
    lat: float = Query(..., description="Latitude of the tile"),
    lng: float = Query(..., description="Longitude of the tile"),
    z: int = Query(..., description="Zoom level"),
    confidence: Optional[str] = Query(None, description="Filter by confidence (low/medium/high)"),
):
    """Fetch alerts from GFW Data API for a given map tile (lat/lng/z)."""
    logger.info("GET /alerts/tile lat=%s lng=%s z=%s confidence=%s", lat, lng, z, confidence)

    alerts = gfw_alerts.get_alerts_tile(lat, lng, z, confidence=confidence)
    if alerts is None:
        logger.error("/alerts/tile returned None")
        raise HTTPException(status_code=502, detail="Erro ao consultar a API do GFW")

    logger.info("/alerts/tile returned %d alerts", len(alerts))
    return alerts


@app.get("/alertas/amazonas", response_model=List[Alert])
def alerts_amazon(
    days: int = Query(14, ge=1, description="Últimos dias"),
    confidence: Optional[str] = Query(None, description="Filter by confidence (low/medium/high)"),
):
    """Fetch recent alerts for the Amazon basin using GFW /query/json endpoint."""
    logger.info("GET /alerts/amazon days=%s confidence=%s", days, confidence)

    alerts = gfw_alerts.get_alerts_amazon(gfw_alerts.TOKEN, days=days, confidence=confidence)
    if alerts is None:
        logger.error("/alerts/amazon returned None")
        raise HTTPException(status_code=502, detail="Erro ao consultar a API do GFW")

    logger.info("/alerts/amazon returned %d alerts", len(alerts))
    return alerts


@app.get("/alertas/amazonas.geojson")
def alerts_amazon_geojson(
    days: int = Query(14, ge=1, description="Últimos dias"),
    confidence: Optional[str] = Query(None, description="Filter by confidence (low/medium/high)"),
):
    """Fetch alerts for the Amazon basin and return as GeoJSON FeatureCollection."""
    logger.info("GET /alerts/amazon.geojson days=%s confidence=%s", days, confidence)

    alerts = gfw_alerts.get_alerts_amazon(gfw_alerts.TOKEN, days=days, confidence=confidence)
    if alerts is None:
        logger.error("/alerts/amazon.geojson returned None")
        raise HTTPException(status_code=502, detail="Erro ao consultar a API do GFW")

    features = []
    for a in alerts:
        lat = a.get("latitude")
        lon = a.get("longitude")
        if lat is None or lon is None:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "alert_date": a.get("alert_date"),
                    "confidence": a.get("confidence"),
                    "source": a.get("source"),
                },
            }
        )

    geojson = {"type": "FeatureCollection", "features": features}
    logger.info("/alerts/amazon.geojson returned %d features", len(features))
    return JSONResponse(content=geojson)


@app.get("/alertas/mapa")
def alerts_map(
    request: Request,
    days: int = Query(14, ge=1, description="Últimos dias"),
    confidence: Optional[str] = Query(None, description="Filter by confidence (low/medium/high)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    """Return alerts ready to be rendered on a map (GeoJSON-like format)."""
    if not _check_rate_limit(request, "/alertas/mapa"):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    logger.info(
        "GET /alertas/mapa days=%s confidence=%s start_date=%s end_date=%s",
        days,
        confidence,
        start_date,
        end_date,
    )

    sd = _parse_date(start_date)
    ed = _parse_date(end_date)

    alerts = alerts_service.get_map_alerts(
        days=days, confidence=confidence, start_date=sd, end_date=ed
    )

    geojson = alerts_service.alerts_to_geojson(alerts)
    logger.info("/alertas/mapa returned %d features", len(geojson.get("features", [])))
    return JSONResponse(content=_standard_response(geojson))


@app.get("/alertas/firms")
def alerts_firms(
    request: Request,
    days: int = Query(1, ge=1, le=30, description="Últimos dias"),
    min_confidence: Optional[float] = Query(None, ge=0, le=100, description="Confiança mínima"),
    limit: int = Query(500, ge=1, le=2000, description="Máximo de alertas"),
):
    """Fetch NASA FIRMS fire alerts and return a standardized payload."""
    if not _check_rate_limit(request, "/alertas/firms"):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    logger.info(
        "GET /alertas/firms days=%s min_confidence=%s limit=%s",
        days,
        min_confidence,
        limit,
    )

    geojson = firms_alerts.fetch_firms_alerts(days=days, min_confidence=min_confidence, limit=limit)

    payload = {
        "status": "ok",
        "count": len(geojson.get("features", [])),
        "data": geojson,
    }

    logger.info("/alertas/firms returned %d features", payload["count"])
    return JSONResponse(status_code=200, content=payload)


@app.get("/alertas/mapa/clusters")
def alerts_map_clusters(
    days: int = Query(14, ge=1, description="Últimos dias"),
    confidence: Optional[str] = Query(None, description="Filter by confidence (low/medium/high)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    eps_km: float = Query(1.0, gt=0, description="Raio do cluster em km"),
    min_samples: int = Query(1, ge=1, description="Mínimo de pontos por cluster"),
):
    """Return cluster data ready to render on a map."""
    logger.info(
        "GET /alerts/map/clusters days=%s confidence=%s start_date=%s end_date=%s eps_km=%s min_samples=%s",
        days,
        confidence,
        start_date,
        end_date,
        eps_km,
        min_samples,
    )

    sd = _parse_date(start_date)
    ed = _parse_date(end_date)

    clusters = alerts_service.get_map_clusters(
        days=days,
        confidence=confidence,
        start_date=sd,
        end_date=ed,
        eps_km=eps_km,
        min_samples=min_samples,
    )
    response = _standard_response(clusters)
    logger.info("/alerts/map/clusters returned %d clusters", response.get("count"))
    return JSONResponse(content=response)


@app.get("/alertas/bbox", response_model=List[Alert])
def alerts_bbox(
    bbox: str = Query(..., description="Bounding box: minLon,minLat,maxLon,maxLat"),
    days: int = Query(14, ge=1, description="Últimos dias"),
    confidence: Optional[str] = Query(None, description="Filter by confidence (low/medium/high)"),
):
    """Fetch alerts for the Amazon basin and filter by a bounding box."""
    logger.info("GET /alerts/bbox bbox=%s days=%s confidence=%s", bbox, days, confidence)

    parts = bbox.split(",")
    if len(parts) != 4:
        raise HTTPException(status_code=400, detail="bbox must be minLon,minLat,maxLon,maxLat")

    try:
        min_lon, min_lat, max_lon, max_lat = [float(p) for p in parts]
    except ValueError:
        raise HTTPException(status_code=400, detail="bbox values must be floats")

    alerts = gfw_alerts.get_alerts_amazon(gfw_alerts.TOKEN, days=days, confidence=confidence)
    if alerts is None:
        logger.error("/alerts/bbox returned None")
        raise HTTPException(status_code=502, detail="Erro ao consultar a API do GFW")

    filtered = gfw_alerts.filter_alerts_by_bbox(alerts, min_lon, min_lat, max_lon, max_lat)
    logger.info("/alerts/bbox returned %d alerts", len(filtered))
    return filtered


@app.get("/alertas/amazonas/clusterizado", response_model=List[Cluster])
def alerts_amazon_clustered(
    days: int = Query(14, ge=1, description="Últimos dias"),
    confidence: Optional[str] = Query(None, description="Filter by confidence (low/medium/high)"),
    eps_km: float = Query(1.0, gt=0, description="Raio do cluster em km"),
    min_samples: int = Query(1, ge=1, description="Mínimo de pontos por cluster"),
):
    """Fetch alerts for the Amazon and return them clusterizados."""
    logger.info(
        "GET /alerts/amazon/clustered days=%s confidence=%s eps_km=%s min_samples=%s",
        days,
        confidence,
        eps_km,
        min_samples,
    )

    alerts = gfw_alerts.get_alerts_amazon(
        gfw_alerts.TOKEN, days=days, confidence=confidence
    )
    if alerts is None:
        logger.error("/alerts/amazon/clustered returned None")
        raise HTTPException(status_code=502, detail="Erro ao consultar a API do GFW")

    clusters = gfw_alerts.cluster_alerts(alerts, eps_km=eps_km, min_samples=min_samples)
    logger.info("/alerts/amazon/clustered returned %d clusters", len(clusters))
    return clusters
