"""Landsat-specific service layer for Anhangua project.

This module wraps gfw_alerts methods and provides helper utilities for
frontend-ready data.
"""

from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import gfw_alerts


def get_landsat_alerts_amazon(
    days: int = 14,
    confidence: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Fetch and normalize Amazon GLAD-Landsat alerts for frontend use."""

    alerts = gfw_alerts.get_alerts_amazon(
        gfw_alerts.TOKEN,
        days=days,
        confidence=confidence,
        _no_cache=True,
    )
    if not alerts:
        return []

    def _parse_date_entry(dt_str):
        try:
            return date.fromisoformat(str(dt_str))
        except Exception:
            return None

    filtered: List[Dict[str, Any]] = []
    for a in alerts:
        alert_date = _parse_date_entry(a.get("alert_date"))
        if start_date and (alert_date is None or alert_date < start_date):
            continue
        if end_date and (alert_date is None or alert_date > end_date):
            continue
        filtered.append({
            "latitude": a.get("latitude"),
            "longitude": a.get("longitude"),
            "alert_date": a.get("alert_date"),
            "confidence": a.get("confidence"),
            "source": a.get("source", "GLAD-Landsat"),
            "alert_type": a.get("alert_type", "GLAD-S2"),
        })

    return filtered


def get_landsat_alerts_tile(
    lat: float,
    lng: float,
    z: int,
    confidence: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch alerts for a given tile (lat/lng/z)."""
    return gfw_alerts.get_alerts_tile(lat, lng, z, token=gfw_alerts.TOKEN, confidence=confidence)


def get_landsat_clusters(
    days: int = 14,
    confidence: Optional[str] = None,
    eps_km: float = 1.0,
    min_samples: int = 1,
) -> List[Dict[str, Any]]:
    """Fetch clusterized alerts for the Amazon basin."""
    alerts = get_landsat_alerts_amazon(days=days, confidence=confidence)
    if not alerts:
        return []

    clusters = gfw_alerts.cluster_alerts(alerts, eps_km=eps_km, min_samples=min_samples)
    return clusters
