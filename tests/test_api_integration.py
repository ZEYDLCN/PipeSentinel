"""Faz 5 REST API — uçtan uca integration testi (canlı PostgreSQL).

    export DATABASE_URL=postgresql+psycopg://sentinel:sentinel@localhost:5432/pipeline_sentinel
    pytest -m integration
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from pipeline_sentinel import db as db_module

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def client():
    eng = db_module.get_engine()
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError:
        pytest.skip("DATABASE_URL'e erişilemiyor; integration testi atlanıyor")

    db_module.reset_schema(eng)
    db_module.run_migrations(eng)

    # apps.api.deps.get_engine process-level cache tutuyor; testte de aynı
    # engine'i kullanmak için modülü DB hazır olduktan sonra import ediyoruz.
    from apps.api.main import app

    with TestClient(app) as c:
        yield c


def test_health(client):
    resp = client.get("/api/v1/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_faults_catalog(client):
    resp = client.get("/api/v1/faults")
    assert resp.status_code == 200
    fault_ids = {f["fault_id"] for f in resp.json()}
    assert fault_ids == {"F01", "F02", "F03", "F04", "F05", "F06"}


def test_datasets_empty_initially(client):
    resp = client.get("/api/v1/datasets")
    assert resp.status_code == 200
    assert resp.json() == []


def test_trigger_pipeline_run_invalid_fault_returns_400(client):
    resp = client.post("/api/v1/pipeline-runs", json={"fault_id": "F99"})
    assert resp.status_code == 400


def test_bootstrap_then_healthy_run_then_datasets_populated(client):
    resp = client.post("/api/v1/bootstrap", json={"runs": 6, "n_customers": 20, "n_orders": 200})
    assert resp.status_code == 200
    assert resp.json()["runs_completed"] == 6

    resp = client.post("/api/v1/pipeline-runs", json={"n_customers": 20, "n_orders": 200})
    assert resp.status_code == 200
    body = resp.json()
    assert set(body["run_ids"]) == {"customers", "orders", "payments"}
    for report in body["reports"].values():
        assert report["severity"] in {"low", "medium"}

    resp = client.get("/api/v1/datasets")
    assert resp.status_code == 200
    datasets = resp.json()
    assert len(datasets) == 3
    for d in datasets:
        assert d["latest_run_id"] is not None


def test_full_f05_flow_run_analyze_incident_lineage(client):
    resp = client.post("/api/v1/pipeline-runs", json={"fault_id": "F05", "n_customers": 20, "n_orders": 200})
    assert resp.status_code == 200
    run_id = resp.json()["run_ids"]["orders"]
    assert resp.json()["reports"]["orders"]["severity"] == "critical"

    resp = client.post(f"/api/v1/pipeline-runs/{run_id}/analyze")
    assert resp.status_code == 200
    analyze_body = resp.json()
    incident_id = analyze_body["incident_id"]
    assert analyze_body["llm_used"] is False
    assert analyze_body["report"]["severity"] == "critical"
    assert analyze_body["report"]["root_causes"]

    resp = client.get("/api/v1/incidents", params={"dataset": "orders"})
    assert resp.status_code == 200
    incidents = resp.json()
    assert any(i["id"] == incident_id for i in incidents)

    resp = client.get(f"/api/v1/incidents/{incident_id}")
    assert resp.status_code == 200
    detail = resp.json()
    assert detail["run_id"] == run_id
    assert detail["signals"]
    assert set(detail["affected_assets"]) >= {"payments.amount", "finance_dashboard", "revenue_forecast_model"}

    # re-analyze via the incident-scoped shortcut endpoint (§13) — idempotent
    resp = client.post(f"/api/v1/incidents/{incident_id}/analyze")
    assert resp.status_code == 200
    assert resp.json()["incident_id"] == incident_id

    resp = client.get("/api/v1/lineage/orders.total_amount")
    assert resp.status_code == 200
    lineage = resp.json()
    downstream_names = {h["name"] for h in lineage["downstream"]}
    assert downstream_names == {"payments.amount", "finance_dashboard", "revenue_forecast_model"}

    # detail'in recommended_actions'ı artık `actions` tablosundan gelir —
    # stabil id + status taşır (Faz 7, §14.3)
    detail = client.get(f"/api/v1/incidents/{incident_id}").json()
    assert len(detail["recommended_actions"]) == 2
    for a in detail["recommended_actions"]:
        assert a["status"] == "pending"
        assert a["incident_id"] == incident_id

    resp = client.get(f"/api/v1/incidents/{incident_id}/actions")
    assert resp.status_code == 200
    assert len(resp.json()) == 2

    action_id = detail["recommended_actions"][0]["id"]
    resp = client.post(f"/api/v1/actions/{action_id}/approve", json={"actor": "frank", "note": "gitti"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "approved"

    resp = client.get(f"/api/v1/actions/{action_id}/approvals")
    assert resp.status_code == 200
    approvals = resp.json()
    assert len(approvals) == 1
    assert approvals[0]["actor"] == "local-dev"  # client-supplied actor is not trusted

    other_action_id = [a["id"] for a in detail["recommended_actions"] if a["id"] != action_id][0]
    resp = client.post(f"/api/v1/actions/{other_action_id}/reject", json={"actor": "grace"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "rejected"

    # re-analyzing the same incident must not reset the decisions already made
    resp = client.post(f"/api/v1/incidents/{incident_id}/analyze")
    assert resp.status_code == 200
    detail_after = client.get(f"/api/v1/incidents/{incident_id}").json()
    statuses = {a["id"]: a["status"] for a in detail_after["recommended_actions"]}
    assert statuses[action_id] == "approved"
    assert statuses[other_action_id] == "rejected"


def test_get_incident_404_for_unknown_id(client):
    resp = client.get("/api/v1/incidents/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


def test_incident_actions_404_for_unknown_incident(client):
    resp = client.get("/api/v1/incidents/00000000-0000-0000-0000-000000000000/actions")
    assert resp.status_code == 404


def test_approve_unknown_action_404(client):
    resp = client.post(
        "/api/v1/actions/00000000-0000-0000-0000-000000000000/approve", json={"actor": "x"}
    )
    assert resp.status_code == 404


def test_reject_unknown_action_404(client):
    resp = client.post(
        "/api/v1/actions/00000000-0000-0000-0000-000000000000/reject", json={"actor": "x"}
    )
    assert resp.status_code == 404


def test_approval_uses_server_identity_without_actor_field(client):
    resp = client.post(
        "/api/v1/actions/00000000-0000-0000-0000-000000000000/approve", json={}
    )
    assert resp.status_code == 404  # identity is server-owned; unknown action still returns 404


def test_get_pipeline_run_404_for_unknown_id(client):
    resp = client.get("/api/v1/pipeline-runs/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404


def test_lineage_404_for_unknown_asset(client):
    resp = client.get("/api/v1/lineage/does_not_exist")
    assert resp.status_code == 404


def test_incidents_filter_by_severity(client):
    resp = client.get("/api/v1/incidents", params={"severity": "critical"})
    assert resp.status_code == 200
    for inc in resp.json():
        assert inc["severity"] == "critical"
