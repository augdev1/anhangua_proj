import json
from dataclasses import dataclass
from typing import Dict, List

from fastapi.testclient import TestClient

from api import app


@dataclass
class EndpointCase:
    name: str
    path: str
    params: Dict[str, object]
    expected_statuses: List[int]


CASES = [
    EndpointCase(
        name="alertas_tile",
        path="/alertas/tile",
        params={"lat": -3.1, "lng": -60.0, "z": 8},
        expected_statuses=[200, 502],
    ),
    EndpointCase(
        name="alertas_amazonas",
        path="/alertas/amazonas",
        params={"days": 3},
        expected_statuses=[200, 502],
    ),
    EndpointCase(
        name="alertas_landsat",
        path="/alertas/landsat",
        params={"days": 3, "limit": 20},
        expected_statuses=[200, 502],
    ),
    EndpointCase(
        name="alertas_amazonas_geojson",
        path="/alertas/amazonas.geojson",
        params={"days": 3},
        expected_statuses=[200, 502],
    ),
    EndpointCase(
        name="alertas_mapa",
        path="/alertas/mapa",
        params={"days": 3, "limit": 50},
        expected_statuses=[200],
    ),
    EndpointCase(
        name="alertas_unificado",
        path="/alertas/unificado",
        params={"days": 3, "limit": 50},
        expected_statuses=[200],
    ),
    EndpointCase(
        name="alertas_firms",
        path="/alertas/firms",
        params={"days": 1, "limit": 50},
        expected_statuses=[200],
    ),
    EndpointCase(
        name="alertas_mapa_clusters",
        path="/alertas/mapa/clusters",
        params={"days": 3, "eps_km": 1.0, "min_samples": 1},
        expected_statuses=[200],
    ),
    EndpointCase(
        name="alertas_bbox",
        path="/alertas/bbox",
        params={"bbox": "-62,-5,-58,-1", "days": 3},
        expected_statuses=[200, 502],
    ),
    EndpointCase(
        name="alertas_amazonas_clusterizado",
        path="/alertas/amazonas/clusterizado",
        params={"days": 3, "eps_km": 1.0, "min_samples": 1},
        expected_statuses=[200, 502],
    ),
]


def run_checks() -> int:
    client = TestClient(app)
    total = len(CASES)
    ok_count = 0
    unexpected_count = 0

    print("\n=== Verificação de Endpoints ===")

    for case in CASES:
        response = client.get(case.path, params=case.params)
        status = response.status_code

        if status in case.expected_statuses:
            ok_count += 1
            result = "OK"
        else:
            unexpected_count += 1
            result = "FALHA"

        preview = ""
        try:
            body = response.json()
            if isinstance(body, dict):
                if "detail" in body:
                    preview = f"detail={body['detail']}"
                elif "count" in body:
                    preview = f"count={body['count']}"
                elif "features" in body:
                    preview = f"features={len(body.get('features', []))}"
            elif isinstance(body, list):
                preview = f"itens={len(body)}"
        except Exception:
            preview = response.text[:120].replace("\n", " ")

        print(
            f"[{result}] {case.name:30s} {case.path:30s} status={status} "
            f"esperado={case.expected_statuses} {preview}"
        )

    print("\n--- Resumo ---")
    print(f"Total: {total}")
    print(f"OK: {ok_count}")
    print(f"Falhas inesperadas: {unexpected_count}")

    if unexpected_count:
        print("\nResultado final: FALHOU")
        return 1

    print("\nResultado final: SUCESSO")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_checks())
