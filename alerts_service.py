"""Service layer with helper functions to prepare alert data for frontend consumption.

This module keeps the backend logic (filtering, formatting, clustering) separate from
FastAPI routing, ensuring the frontend can consume only ready-to-render data.
"""

from datetime import date
from typing import Any, Dict, Iterable, List, Optional

import gfw_alerts


CONFIDENCE_MAP = {
    "low": 1,
    "nominal": 2,
    "medium": 3,
    "high": 4,
}

CONFIDENCE_LABELS = ["low", "nominal", "medium", "high"]


def _confidence_score(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    return CONFIDENCE_MAP.get(str(value).strip().lower())


def _confidence_from_score(score: float) -> str:
    """Convert a score (1-4) back to the nearest confidence label."""
    if score is None:
        return "unknown"
    idx = int(round(score)) - 1
    if idx < 0:
        idx = 0
    if idx >= len(CONFIDENCE_LABELS):
        idx = len(CONFIDENCE_LABELS) - 1
    return CONFIDENCE_LABELS[idx]


def _normalize_alert(alert: Dict[str, Any]) -> Dict[str, Any]:
    """Return only the fields the frontend needs for map rendering."""
    return {
        "latitude": alert.get("latitude"),
        "longitude": alert.get("longitude"),
        "alert_date": alert.get("alert_date"),
        "confidence": alert.get("confidence"),
        "source": alert.get("source"),
    }


def _filter_by_date_range(
    alerts: Iterable[Dict[str, Any]],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Filter alerts by alert_date within [start_date, end_date]."""

    if start_date is None and end_date is None:
        return list(alerts)

    out: List[Dict[str, Any]] = []
    for a in alerts:
        d = gfw_alerts.parse_alert_date(a.get("alert_date"))
        if d is None:
            continue
        if start_date and d < start_date:
            continue
        if end_date and d > end_date:
            continue
        out.append(a)
    return out


def _cluster_confidence(clusters: List[Dict[str, Any]], alerts: List[Dict[str, Any]]) -> None:
    """Add a `confidence` field to each cluster based on its members."""

    # Build index of points to confidence score.
    point_to_conf = {}
    for a in alerts:
        lat = a.get("latitude")
        lon = a.get("longitude")
        score = _confidence_score(a.get("confidence"))
        if lat is None or lon is None or score is None:
            continue
        point_to_conf[(lat, lon)] = score

    for c in clusters:
        # cluster is expected to have `lat`/`lon` and `alert_count`.
        # We approximate confidence by averaging nearby points in the cluster.
        # Since clustering is already done, we just approximate using nearest points.
        lat = c.get("lat")
        lon = c.get("lon")
        if lat is None or lon is None:
            c["confidence"] = "unknown"
            continue

        # Find points within 0.01 degrees (~1km) of cluster center.
        scores = []
        for (plat, plon), score in point_to_conf.items():
            if abs(plat - lat) < 0.01 and abs(plon - lon) < 0.01:
                scores.append(score)

        if not scores:
            c["confidence"] = "unknown"
        else:
            avg = sum(scores) / len(scores)
            c["confidence"] = _confidence_from_score(avg)


def get_map_alerts(
    days: int = 14,
    confidence: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Return alerts ready to be rendered on a map.

    This function applies:
    - query to GFW (/query/json)
    - confidence filtering
    - deduplication
    - optional date filtering
    - keeps only the fields needed for frontend rendering
    """

    alerts = gfw_alerts.get_alerts_amazon(gfw_alerts.TOKEN, days=days, confidence=confidence)
    if not alerts:
        return []

    alerts = _filter_by_date_range(alerts, start_date=start_date, end_date=end_date)
    return [_normalize_alert(a) for a in alerts]


def get_map_clusters(
    days: int = 14,
    confidence: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    eps_km: float = 1.0,
    min_samples: int = 1,
) -> List[Dict[str, Any]]:
    """Return cluster data ready to be plotted on a map."""

    alerts = get_map_alerts(days=days, confidence=confidence, start_date=start_date, end_date=end_date)
    if not alerts:
        return []

    clusters = gfw_alerts.cluster_alerts(alerts, eps_km=eps_km, min_samples=min_samples)
    # Add a confidence estimate per cluster.
    _cluster_confidence(clusters, alerts)

    return clusters


def alerts_to_geojson(alerts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convert alerts to GeoJSON FeatureCollection."""
    features: List[Dict[str, Any]] = []
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
    return {"type": "FeatureCollection", "features": features}
