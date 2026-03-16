"""FastAPI server exposing alerts endpoints for frontend consumption.

This server wraps the existing `gfw_alerts.py` logic and provides simple
endpoints to fetch alerts and clustered alerts as JSON.
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional

import gfw_alerts

app = FastAPI(
    title="Anhangua GFW Alerts API",
    description="Serves deforestation alert data fetched from Global Forest Watch.",
    version="0.1.0",
)


class Alert(BaseModel):
    latitude: Optional[float]
    longitude: Optional[float]
    alert_date: Optional[str]
    confidence: Optional[float]
    source: Optional[str]
    alert_type: Optional[str]
    area_ha: Optional[float]


class Cluster(BaseModel):
    lat: float
    lon: float
    alert_count: int
    latest_alert: Optional[str]


@app.get("/alerts/tile", response_model=List[Alert])
def alerts_tile(lat: float = Query(..., description="Latitude of the tile"),
                lng: float = Query(..., description="Longitude of the tile"),
                z: int = Query(..., description="Zoom level")):
    """Fetch alerts from GFW Data API for a given map tile (lat/lng/z)."""
    alerts = gfw_alerts.get_alerts_tile(lat, lng, z)
    if alerts is None:
        raise HTTPException(status_code=502, detail="Erro ao consultar a API do GFW")
    return alerts


@app.get("/alerts/amazon", response_model=List[Alert])
def alerts_amazon(days: int = Query(14, ge=1, description="Últimos dias")):
    """Fetch recent alerts for the Amazon basin using GFW /query/json endpoint."""
    alerts = gfw_alerts.get_alerts_amazon(gfw_alerts.TOKEN, days=days)
    if alerts is None:
        raise HTTPException(status_code=502, detail="Erro ao consultar a API do GFW")
    return alerts


@app.get("/alerts/amazon/clustered", response_model=List[Cluster])
def alerts_amazon_clustered(
    days: int = Query(14, ge=1, description="Últimos dias"),
    eps_km: float = Query(1.0, gt=0, description="Raio do cluster em km"),
    min_samples: int = Query(1, ge=1, description="Mínimo de pontos por cluster"),
):
    """Fetch alerts for the Amazon and return them clusterizados."""
    alerts = gfw_alerts.get_alerts_amazon(gfw_alerts.TOKEN, days=days)
    if alerts is None:
        raise HTTPException(status_code=502, detail="Erro ao consultar a API do GFW")

    clusters = gfw_alerts.cluster_alerts(alerts, eps_km=eps_km, min_samples=min_samples)
    return clusters
