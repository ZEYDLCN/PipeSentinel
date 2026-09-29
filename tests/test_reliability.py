from copy import deepcopy
from datetime import datetime, timezone
import json

import pandas as pd
import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.pool import StaticPool

from pipeline_sentinel import baseline, reliability as svc, reliability_store as store
from pipeline_sentinel.connectors import ScanResult, validate_source
from pipeline_sentinel.contract_io import ContractError, load_contract
from pipeline_sentinel.ingestion import dbt_graph, openlineage_graph, reachable
from pipeline_sentinel.repair import data_diff, evaluate, restore, sandbox, snapshot


@pytest.fixture
def engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    store.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def config():
    return {"connection_env": "SENTINEL_SOURCE_TEST", "schema": "public", "table": "orders",
            "allowlist": ["public.orders"], "retain_snapshot": True, "order_by": ["id"],
            "contract": {"expected_schema": {"id": "integer", "batch": "string", "amount": "float"},
                         "rules": [{"type": "uniqueness", "column": "id"}, {"type": "range", "column": "amount", "min": 0, "max": 1000}]}}


@pytest.fixture
def frame():
    return pd.DataFrame({"id": [1, 2, 3], "batch": ["good", "bad", "bad"], "amount": [100., 200., 300.]})


class FakeConnector:
    def __init__(self, frame):
        self.frame = frame

    def scan(self, config):
        return ScanResult(self.frame.copy(), {"complete": True, "total_rows": len(self.frame), "scanned_rows": len(self.frame), "method": "test", "duration_seconds": 0})


def analyze(engine, source, frame, key):
    svc.enqueue(engine, source["id"], key, "operator")
    result = svc.work_once(engine, FakeConnector(frame))
    assert "body" in result, result
    return result


def test_contract_validation_rejects_silent_typos_and_duplicate_yaml_keys():
    for value in ("rules: []", "expected_schema: {x: mystery}", "expected_schema: {x: float}\nrules: [{type: ragne, column: x}]",
                  "expected_schema: {x: float}\nrules: [{type: range, column: x, min: 3, max: 1}]",
                  "expected_schema: {x: float}\nexpected_schema: {y: integer}", "expected_schema: ["):
        with pytest.raises(ContractError):
            load_contract(value)
    assert load_contract("expected_schema: {id: integer}")["version"] == 1


@pytest.mark.parametrize("change", [{"connection_env": "DATABASE_URL"}, {"allowlist": []}, {"max_rows": -1},
                                    {"timeout_seconds": 1000}, {"segment_by": ["missing"]},
                                    {"sensitive_columns": ["batch"]}, {"sql": "DELETE FROM orders"}])
def test_source_boundary(config, change):
    with pytest.raises(ValueError):
        validate_source({**config, **change})


def test_durable_jobs_baseline_replay_and_idempotency(engine, config, frame):
    source = svc.register_source(engine, "orders", config, "alice")
    job = svc.enqueue(engine, source["id"], "same-key", "alice")
    assert svc.enqueue(engine, source["id"], "same-key", "alice")["id"] == job["id"]
    first = svc.work_once(engine, FakeConnector(frame))
    assert first["quality"] == "candidate"
    assert first["body"]["baseline"]["state"] == "cold_start"
    svc.accept_baseline(engine, first["id"], "alice")
    second = analyze(engine, source, frame, "second")
    assert second["body"]["baseline"]["ids"] == [first["id"]]
    assert svc.replay(engine, second["id"])["matches"]
    assert svc.work_once(engine, FakeConnector(frame)) is None
    detail = svc.observation_detail(engine, second["id"])
    assert detail["body"]["snapshot_available"]
    assert "snapshot" not in detail["body"]
    with engine.connect() as conn:
        assert store.get(conn, store.jobs, job["id"])["status"] == "succeeded"


def test_bad_baseline_excluded_and_contract_versions_isolated(engine, config, frame):
    source = svc.register_source(engine, "orders", config, "alice")
    bad = frame.copy(); bad["amount"] *= 100
    incident = analyze(engine, source, bad, "bad")
    with pytest.raises(ValueError, match="baseline"):
        svc.accept_baseline(engine, incident["id"], "alice")
    good = analyze(engine, source, frame, "good")
    svc.accept_baseline(engine, good["id"], "alice")
    changed = deepcopy(config); changed["contract"]["version"] = 2
    svc.register_source(engine, "orders", changed, "alice")
    new = analyze(engine, source, frame, "new-contract")
    assert new["body"]["baseline"]["state"] == "cold_start"


def test_worker_retry_and_credentials_not_logged(engine, config):
    class Broken:
        def scan(self, config):
            raise RuntimeError("postgres://user:secret@db")
    source = svc.register_source(engine, "orders", config, "alice")
    job = svc.enqueue(engine, source["id"], "retry", "alice")
    for i in range(3):
        result = svc.work_once(engine, Broken())
        assert "secret" not in result["error"]
        if i < 2:
            svc.retry_job(engine, job["id"], "alice")
    with pytest.raises(ValueError):
        svc.retry_job(engine, job["id"], "alice")


def test_expired_job_can_be_retried(engine, config):
    source = svc.register_source(engine, "orders", config, "alice")
    job = svc.enqueue(engine, source["id"], "lease", "alice")
    svc.claim(engine)
    with engine.begin() as conn:
        conn.execute(update(store.jobs).values(lease_until="2000-01-01"))
    assert svc.claim(engine) is None
    svc.retry_job(engine, job["id"], "alice")
    assert svc.claim(engine)["attempts"] == 2


def test_seasonal_history_requires_matching_cohort():
    def observation(id, date, amount, quality="accepted"):
        return {"id": id, "created_at": date, "quality": quality,
                "body": {"profile": {"amount": {"mean": amount}}, "coverage": {"total_rows": 100}}}
    date = "2026-09-29T09:00:00+00:00"
    history = [observation("a", "2026-09-22T09:00:00+00:00", 10),
               observation("b", "2026-09-15T09:00:00+00:00", 12),
               observation("c", "2026-09-08T09:00:00+00:00", 11),
               observation("d", "2026-09-28T09:00:00+00:00", 1000)]
    result = baseline.choose(history, date, True)
    assert result["profile"]["amount"]["mean"] == 11
    assert baseline.choose(history[:1], date, True)["state"] == "insufficient_history"


def test_segment_loss_detected(frame):
    old = baseline.segment_profiles(frame, ["batch"])
    new = baseline.segment_profiles(frame[frame.batch == "good"], ["batch"])
    assert any(s.type == "segment_missing" for s in baseline.compare_segments(old, new))


def test_sensitive_profiles_and_evidence_are_masked(engine, config, frame):
    config.update(retain_snapshot=False, sensitive_columns=["batch"])
    config["contract"]["rules"].append({"type": "allowed_values", "column": "batch", "values": ["good"]})
    source = svc.register_source(engine, "private", config, "alice")
    result = analyze(engine, source, frame, "private")
    assert "top_values" not in result["body"]["profile"]["batch"]
    assert result["body"]["snapshot"] is None
    for signal in result["body"]["signals"]:
        if signal["column"] == "batch":
            assert "bad" not in json.dumps(signal["evidence"])


def test_snapshot_roundtrip_and_repair_validation(config, frame):
    bad = frame.copy(); bad.loc[bad.batch == "bad", "amount"] *= 100
    data = snapshot(bad)
    pd.testing.assert_frame_equal(restore(data), bad)
    action = {"type": "scale", "column": "amount", "factor": .01, "where": {"batch": "bad"}}
    timestamp = datetime.now(timezone.utc).isoformat()
    correct = evaluate(data, config["contract"], action, timestamp)
    assert correct["passed"] and correct["changed_rows"] == 2
    assert correct["unchanged_control_passed"] and not correct["production_executed"]
    wrong = evaluate(data, config["contract"], {**action, "factor": 10}, timestamp)
    assert not wrong["passed"]
    with pytest.raises(ValueError):
        evaluate(data, config["contract"], {**action, "max_changed_rows": 1}, timestamp)
    assert sandbox(data, config["contract"], action, timestamp)["passed"]
    pd.testing.assert_frame_equal(restore(data), bad)


def test_deduplication_refuses_conflicting_records(config, frame):
    duplicate = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    action = {"type": "deduplicate", "keys": ["id"]}
    assert evaluate(snapshot(duplicate), config["contract"], action, store.now())["passed"]
    duplicate.loc[3, "amount"] = 900
    with pytest.raises(ValueError, match="belirsiz"):
        evaluate(snapshot(duplicate), config["contract"], action, store.now())


def test_repair_approval_and_staleness(engine, config, frame):
    source = svc.register_source(engine, "orders", config, "alice")
    frame.loc[frame.batch == "bad", "amount"] *= 100
    item = analyze(engine, source, frame, "one")
    action = {"type": "scale", "column": "amount", "factor": .01, "where": {"batch": "bad"}}
    attempt = svc.try_repair(engine, item["id"], action, "alice")
    assert svc.decide_repair(engine, attempt["id"], "approve", "bob")["status"] == "approved"
    with pytest.raises(ValueError):
        svc.decide_repair(engine, attempt["id"], "approve", "bob")
    second = svc.try_repair(engine, item["id"], action, "alice")
    analyze(engine, source, frame, "two")
    with pytest.raises(ValueError, match="güncel"):
        svc.decide_repair(engine, second["id"], "approve", "bob")


def test_dbt_graph_replaces_stale_edges_and_explains_business_impact(engine, config, frame):
    manifest = {"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"},
                "nodes": {"model.p.orders": {"name": "orders", "raw_code": "select 1", "depends_on": {"nodes": []}}},
                "exposures": {"exposure.p.finance": {"name": "Finance", "depends_on": {"nodes": ["model.p.orders"]}}}}
    svc.ingest(engine, "p", manifest, "alice")
    svc.set_policy(engine, "exposure.p.finance", "finance", 5, 30, "alice")
    config["lineage_asset"] = "model.p.orders"
    source = svc.register_source(engine, "orders", config, "alice")
    frame["amount"] *= 100
    item = analyze(engine, source, frame, "one")
    assert item["body"]["impact"]["components"]["asset_criticality"] == 20
    assert item["body"]["downstream"] == ["exposure.p.finance"]
    svc.record_feedback(engine, item["id"], "resolved", "Fixed unit mapping", "alice")
    second = analyze(engine, source, frame, "two")
    assert second["body"]["group_id"] == item["body"]["group_id"]
    assert svc.observation_detail(engine, second["id"])["similar_incidents"][0]["observation_id"] == item["id"]
    manifest["exposures"] = {}
    svc.ingest(engine, "p", manifest, "alice")
    with engine.connect() as conn:
        assert svc.graph_context(conn, "model.p.orders")[1] == []


def test_openlineage_keeps_job_boundaries():
    event = {"eventType": "COMPLETE", "eventTime": store.now(), "run": {"runId": "r"},
             "job": {"namespace": "airflow", "name": "load"},
             "inputs": [{"namespace": "db", "name": "raw"}], "outputs": [{"namespace": "db", "name": "clean"}]}
    graph = openlineage_graph(event)
    assert "db::clean" in reachable(graph["edges"], "db::raw")
    assert ["db::raw", "db::clean"] not in graph["edges"]


def test_ci_diff_catches_changes_additions_and_schema(frame):
    same = frame.iloc[::-1].copy()
    assert data_diff(frame, same, ["id"])["passed"]
    same.loc[1, "amount"] = 999
    result = data_diff(frame, same, ["id"])
    assert result["changed_rows"] == 1 and not result["passed"]
    assert data_diff(frame, same, ["id"], .5)["passed"]
    with pytest.raises(ValueError):
        data_diff(frame, pd.concat([frame, frame]), ["id"])


def test_api_auth_and_async_flow(engine, config, monkeypatch):
    from fastapi.testclient import TestClient
    from apps.api.main import app
    from apps.api.deps import get_engine
    app.dependency_overrides[get_engine] = lambda: engine
    reader, operator = "r" * 24, "o" * 24
    monkeypatch.setenv("SENTINEL_MODE", "pilot")
    monkeypatch.setenv("SENTINEL_API_TOKENS", json.dumps({reader: {"actor": "reader", "role": "reader"}, operator: {"actor": "alice", "role": "operator"}}))
    try:
        with TestClient(app) as client:
            root = "/api/v1/reliability"
            assert client.get(root + "/sources").status_code == 401
            client.headers["Authorization"] = "Bearer " + reader
            assert client.get(root + "/sources").status_code == 200
            assert client.post(root + "/sources", json={"name": "x", "config": config}).status_code == 403
            client.headers["Authorization"] = "Bearer " + operator
            source = client.post(root + "/sources", json={"name": "x", "config": config}).json()
            assert source["actor"] == "alice"
            response = client.post(root + "/sources/" + source["id"] + "/analyze", headers={"Idempotency-Key": "test"})
            assert response.status_code == 202
            assert response.json()["status"] == "pending"
            assert client.post("/api/v1/pipeline-runs", json={}).status_code == 403
            assert client.get(root + "/jobs/missing").status_code == 404
    finally:
        app.dependency_overrides.clear()


def test_empty_data_cannot_become_baseline(engine, config, frame):
    source = svc.register_source(engine, "empty", config, "alice")
    item = analyze(engine, source, frame.iloc[:0], "empty")
    assert any(s["type"] == "empty_dataset" for s in item["body"]["signals"])
    with pytest.raises(ValueError):
        svc.accept_baseline(engine, item["id"], "alice")


def test_rule_version_change_blocks_replay(engine, config, frame, monkeypatch):
    source = svc.register_source(engine, "orders", config, "alice")
    item = analyze(engine, source, frame, "one")
    monkeypatch.setattr(svc, "rule_fingerprint", lambda: "different-code")
    with pytest.raises(ValueError, match="sürüm"):
        svc.replay(engine, item["id"])


def test_failed_repair_cannot_be_approved(engine, config, frame):
    source = svc.register_source(engine, "orders", config, "alice")
    frame["amount"] *= 100
    item = analyze(engine, source, frame, "one")
    action = {"type": "scale", "column": "amount", "factor": 10, "where": {"batch": "bad"}}
    attempt = svc.try_repair(engine, item["id"], action, "alice")
    assert attempt["status"] == "failed"
    with pytest.raises(ValueError):
        svc.decide_repair(engine, attempt["id"], "approve", "alice")


def test_outbox_is_opt_in_and_signed_without_raw_data(engine, config, frame, monkeypatch):
    from pipeline_sentinel import notifications
    import hashlib
    import hmac
    source = svc.register_source(engine, "orders", config, "alice")
    frame["amount"] *= 100
    analyze(engine, source, frame, "one")
    monkeypatch.delenv("SENTINEL_WEBHOOK_URL", raising=False)
    assert notifications.deliver_one(engine) is None
    monkeypatch.setenv("SENTINEL_WEBHOOK_URL", "https://example.invalid/webhook")
    monkeypatch.setenv("SENTINEL_WEBHOOK_SECRET", "s" * 24)
    requests = []
    class Response:
        status = 204
        def __enter__(self): return self
        def __exit__(self, *args): pass
    class Transport:
        def open(self, request, timeout):
            requests.append(request)
            return Response()
    monkeypatch.setattr(notifications, "build_opener", lambda *args: Transport())
    assert notifications.deliver_one(engine)["status"] == "sent"
    assert "amount" not in requests[0].data.decode()
    expected = hmac.new(b"s" * 24, requests[0].data, hashlib.sha256).hexdigest()
    assert requests[0].headers["X-sentinel-signature"] == expected
    assert notifications.deliver_one(engine) is None


def test_cli_gate_fails_on_regression(tmp_path):
    from typer.testing import CliRunner
    from pipeline_sentinel.cli import app
    before, after, contract, output = [tmp_path / p for p in ("before.csv", "after.csv", "contract.yml", "report.json")]
    before.write_text("id,amount\n1,100\n", encoding="utf-8")
    after.write_text("id,amount\n1,10000\n", encoding="utf-8")
    contract.write_text("expected_schema: {id: integer, amount: float}\nrules: [{type: range, column: amount, min: 0, max: 1000}]", encoding="utf-8")
    result = CliRunner().invoke(app, ["reliability", "ci-check", str(before), str(after), "--contract", str(contract), "--keys", "id", "--output", str(output)])
    assert result.exit_code == 1, result.output
    assert not json.loads(output.read_text(encoding="utf-8"))["passed"]
