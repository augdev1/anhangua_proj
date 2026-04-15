import json
import os
from datetime import datetime
from typing import Any, Dict, List

LOG_FILE = os.path.join("logs", "alerts_api.log")
OUT_DIR = os.path.join("logs", "exports")


def _extract_json_part(line: str, marker: str) -> Dict[str, Any]:
    idx = line.find(marker)
    if idx < 0:
        return {}
    payload = line[idx + len(marker):].strip()
    if not payload:
        return {}
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return {}


def parse_unified_summaries() -> List[Dict[str, Any]]:
    if not os.path.exists(LOG_FILE):
        return []

    rows: List[Dict[str, Any]] = []
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if "UNIFIED_SUMMARY" not in line:
                continue

            row = _extract_json_part(line, "UNIFIED_SUMMARY ")
            if not row:
                continue

            # tenta capturar timestamp do início da linha de log padrão
            # formato esperado: YYYY-MM-DD HH:MM:SS,ms LEVEL ...
            ts = None
            try:
                ts_raw = line[:23]
                ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S,%f").isoformat()
            except Exception:
                pass

            if ts:
                row["logged_at"] = ts

            rows.append(row)

    return rows


def parse_unified_data_snapshots() -> List[Dict[str, Any]]:
    if not os.path.exists(LOG_FILE):
        return []

    rows: List[Dict[str, Any]] = []
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if "UNIFIED_DATA" not in line:
                continue

            row = _extract_json_part(line, "UNIFIED_DATA ")
            if not row:
                continue

            ts = None
            try:
                ts_raw = line[:23]
                ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S,%f").isoformat()
            except Exception:
                pass

            if ts:
                row["logged_at"] = ts

            rows.append(row)

    return rows


def build_report(rows: List[Dict[str, Any]], data_snapshots: List[Dict[str, Any]]) -> Dict[str, Any]:
    totals = {
        "requests": len(rows),
        "alerts_total": 0,
        "avg_latency_ms": 0.0,
        "by_source_raw": {"gfw": 0, "firms": 0, "landsat": 0},
        "by_source_final": {"gfw": 0, "firms": 0, "landsat": 0, "unknown": 0},
        "by_confidence": {},
        "source_errors": {"gfw": 0, "firms": 0, "landsat": 0},
    }

    latest_alerts: List[Dict[str, Any]] = []
    latest_snapshot_meta: Dict[str, Any] = {}
    alerts_pool: List[Dict[str, Any]] = []
    if data_snapshots:
        latest = data_snapshots[-1]
        latest_alerts = latest.get("alerts", []) or []
        latest_snapshot_meta = {
            "logged_at": latest.get("logged_at"),
            "days": latest.get("days"),
            "confidence": latest.get("confidence"),
            "start_date": latest.get("start_date"),
            "end_date": latest.get("end_date"),
            "limit": latest.get("limit"),
            "count_total": latest.get("count_total"),
        }

        seen = set()
        for snapshot in data_snapshots:
            for alert in snapshot.get("alerts", []) or []:
                key = (
                    alert.get("alert_date"),
                    alert.get("latitude"),
                    alert.get("longitude"),
                    alert.get("source"),
                    alert.get("confidence"),
                )
                if key in seen:
                    continue
                seen.add(key)
                alerts_pool.append(alert)

    if not rows:
        return {
            "summary": totals,
            "records": [],
            "latest_snapshot": latest_snapshot_meta,
            "latest_alerts": latest_alerts,
            "alerts_pool": alerts_pool,
        }

    latency_sum = 0.0

    for row in rows:
        totals["alerts_total"] += int(row.get("count_total", 0) or 0)
        latency_sum += float(row.get("latency_ms", 0.0) or 0.0)

        raw = row.get("source_counts_raw", {}) or {}
        for key in ("gfw", "firms", "landsat"):
            totals["by_source_raw"][key] += int(raw.get(key, 0) or 0)

        final = row.get("source_counts_final", {}) or {}
        for key in ("gfw", "firms", "landsat", "unknown"):
            totals["by_source_final"][key] += int(final.get(key, 0) or 0)

        conf = row.get("confidence_counts", {}) or {}
        for key, val in conf.items():
            label = str(key).strip().lower() or "unknown"
            totals["by_confidence"][label] = totals["by_confidence"].get(label, 0) + int(val or 0)

        errors = row.get("source_errors", {}) or {}
        for key in ("gfw", "firms", "landsat"):
            if key in errors and errors[key]:
                totals["source_errors"][key] += 1

    totals["avg_latency_ms"] = round(latency_sum / len(rows), 2)

    return {
        "summary": totals,
        "records": rows,
        "latest_snapshot": latest_snapshot_meta,
        "latest_alerts": latest_alerts,
        "alerts_pool": alerts_pool,
    }


def main() -> int:
    rows = parse_unified_summaries()
    data_snapshots = parse_unified_data_snapshots()
    report = build_report(rows, data_snapshots)

    os.makedirs(OUT_DIR, exist_ok=True)
    out_file = os.path.join(OUT_DIR, "unified_logs_report.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"Registros UNIFIED_SUMMARY encontrados: {len(rows)}")
    print(f"Registros UNIFIED_DATA encontrados: {len(data_snapshots)}")
    print(f"Arquivo gerado: {out_file}")
    if rows:
        print(f"Média de latência (ms): {report['summary']['avg_latency_ms']}")
        print(f"Total alertas agregados: {report['summary']['alerts_total']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
