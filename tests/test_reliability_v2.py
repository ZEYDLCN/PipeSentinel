"""Zamanlayıcı, beklenen değişim kabulü, standart hata tabanlı kayma ve CSV'den sözleşme önerisi."""
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from pipeline_sentinel import reliability as svc, reliability_store as store
from pipeline_sentinel.connectors import ScanResult, validate_source
from pipeline_sentinel.detector import compare_column_profiles
from pipeline_sentinel.profiler import profile_column


@pytest.fixture
def engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    store.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def config():
    return {"connection_env": "SENTINEL_SOURCE_TEST", "schema": "public", "table": "orders",
            "allowlist": ["public.orders"], "order_by": ["id"],
            "contract": {"expected_schema": {"id": "integer", "amount": "float"},
                         "rules": [{"type": "uniqueness", "column": "id"}]}}


def make_frame(mean=100.0, n=2000, seed=0):
    rng = np.random.default_rng(seed)
    return pd.DataFrame({"id": range(n), "amount": rng.normal(mean, 20, n)})


class FakeConnector:
    def __init__(self, frame):
        self.frame = frame

    def scan(self, config):
        return ScanResult(self.frame.copy(), {"complete": True, "total_rows": len(self.frame),
                                              "scanned_rows": len(self.frame), "method": "test", "duration_seconds": 0})


def analyze(engine, source, frame, key):
    svc.enqueue(engine, source["id"], key, "operator")
    result = svc.work_once(engine, FakeConnector(frame))
    assert "body" in result, result
    return result


# --- standart hata tabanlı ortalama kayması -------------------------------

def profile(frame):
    return profile_column(frame["amount"])


def test_moderate_shift_is_detected_with_large_samples():
    """%20 kayma satır bazlı stddev'in 3 katının çok altında: eski test kaçırır, yenisi yakalar."""
    old, new = profile(make_frame(100, seed=1)), profile(make_frame(120, seed=2))
    signals = compare_column_profiles(old, new, "amount")
    shift = [s for s in signals if s.type == "distribution"]
    assert len(shift) == 1 and shift[0].evidence["method"] == "standard_error"
    assert 0.15 < shift[0].evidence["relative_shift"] < 0.25
    json.dumps(shift[0].to_dict(), allow_nan=False)


def test_healthy_resampling_and_trivial_shifts_do_not_alert():
    base = profile(make_frame(100, seed=1))
    assert compare_column_profiles(base, profile(make_frame(100, seed=3)), "amount") == []
    # Çok büyük örneklemde istatistiksel olarak anlamlı ama pratik olarak önemsiz (%2) kayma
    big_old = profile(make_frame(100, n=200_000, seed=4))
    big_new = profile(make_frame(102, n=200_000, seed=5))
    assert compare_column_profiles(big_old, big_new, "amount") == []


def test_small_samples_and_constant_columns():
    # n<30: standart hata testi devreye girmez (eski satır bazlı z-testi bu kayma için eşiğin altında)
    small_old, small_new = profile(make_frame(100, n=10)), profile(make_frame(125, n=10, seed=9))
    assert compare_column_profiles(small_old, small_new, "amount") == []
    constant_old = {"row_count": 500, "null_ratio": 0.0, "mean": 10.0, "stddev": 0.0}
    constant_new = {"row_count": 500, "null_ratio": 0.0, "mean": 20.0, "stddev": 0.0}
    signal = compare_column_profiles(constant_old, constant_new, "fee")[0]
    json.dumps(signal.to_dict(), allow_nan=False)


def test_profiles_without_row_count_keep_legacy_behaviour():
    assert compare_column_profiles({"mean": 100.0, "stddev": 20.0}, {"mean": 120.0, "stddev": 20.0}, "amount") == []


# --- zamanlayıcı -----------------------------------------------------------

def test_schedule_validation(config):
    assert validate_source({**config, "schedule_minutes": 60})["schedule_minutes"] == 60
    for bad in (0, 4, 10081, True, "60", 1.5):
        with pytest.raises(ValueError):
            validate_source({**config, "schedule_minutes": bad})
    with pytest.raises(ValueError):
        validate_source({**config, "auto_baseline": "yes"})


def test_enqueue_due_is_slot_idempotent_and_does_not_pile_up(engine, config):
    scheduled = svc.register_source(engine, "scheduled", {**config, "schedule_minutes": 60}, "alice")
    svc.register_source(engine, "manual", {**config, "table": "other", "allowlist": ["public.other"]}, "alice")
    now = datetime(2026, 10, 7, 9, 30, tzinfo=timezone.utc)
    first = svc.enqueue_due(engine, now)
    assert [job["source_id"] for job in first] == [scheduled["id"]] and first[0]["actor"] == "scheduler"
    assert svc.enqueue_due(engine, now + timedelta(minutes=5)) == [], "bitmemiş iş varken yenisi eklenmez"
    assert svc.work_once(engine, FakeConnector(make_frame()))["source_id"] == scheduled["id"]
    assert svc.enqueue_due(engine, now + timedelta(minutes=10)) == [], "aynı dilimde ikinci iş üretilmez"
    next_slot = svc.enqueue_due(engine, now + timedelta(minutes=45))
    assert len(next_slot) == 1


def test_schedule_and_auto_baseline_do_not_reset_baseline_history(engine, config):
    source = svc.register_source(engine, "orders", config, "alice")
    first = analyze(engine, source, make_frame(), "one")
    svc.accept_baseline(engine, first["id"], "alice")
    svc.register_source(engine, "orders", {**config, "schedule_minutes": 30, "auto_baseline": True}, "alice")
    second = analyze(engine, source, make_frame(seed=1), "two")
    assert second["body"]["baseline"]["state"] == "ready"
    assert second["body"]["baseline"]["ids"] == [first["id"]]


# --- auto_baseline ---------------------------------------------------------

def test_auto_baseline_requires_a_human_accepted_anchor_and_clean_run(engine, config):
    source = svc.register_source(engine, "orders", {**config, "auto_baseline": True}, "alice")
    first = analyze(engine, source, make_frame(), "one")
    assert first["quality"] == "candidate", "insan onaylı referans yokken otomatik kabul olmaz"
    svc.accept_baseline(engine, first["id"], "alice")
    second = analyze(engine, source, make_frame(seed=1), "two")
    assert second["quality"] == "accepted"
    with engine.connect() as conn:
        assert store.get(conn, store.observations, second["id"])["quality"] == "accepted"
        assert {e["kind"] for e in store.rows(conn, store.events)} >= {"baseline_auto_accepted"}
    bad = analyze(engine, source, make_frame(mean=300, seed=2), "three")
    assert bad["body"]["signals"] and bad["quality"] == "rejected"


# --- beklenen değişim ------------------------------------------------------

def test_expected_change_becomes_the_new_baseline_and_silences_repeat_alerts(engine, config):
    source = svc.register_source(engine, "orders", config, "alice")
    svc.accept_baseline(engine, analyze(engine, source, make_frame(100), "one")["id"], "alice")
    shifted = analyze(engine, source, make_frame(130, seed=1), "campaign")
    assert any(s["type"] == "distribution" for s in shifted["body"]["signals"])
    with pytest.raises(ValueError, match="gerekçe"):
        svc.accept_expected_change(engine, shifted["id"], "kısa", "alice")
    result = svc.accept_expected_change(engine, shifted["id"], "Kasım kampanyası fiyatları yükseltti", "bob")
    assert result["quality"] == "accepted"
    again = analyze(engine, source, make_frame(130, seed=2), "after-campaign")
    assert again["body"]["signals"] == [] and again["body"]["baseline"]["ids"] == [shifted["id"]]
    with engine.connect() as conn:
        event = [e for e in store.rows(conn, store.events) if e["kind"] == "expected_change_accepted"][0]
        assert event["actor"] == "bob" and "Kasım" in event["body"]["note"]
        feedback = store.rows(conn, store.feedback, store.feedback.c.observation_id == shifted["id"])
        assert feedback[0]["verdict"] == "expected_change"


def test_expected_change_refuses_contract_violations_and_stale_observations(engine, config):
    source = svc.register_source(engine, "orders", config, "alice")
    svc.accept_baseline(engine, analyze(engine, source, make_frame(100), "one")["id"], "alice")
    broken = make_frame(100, seed=3)
    broken["id"] = 1  # uniqueness ihlali = sözleşme sinyali
    violation = analyze(engine, source, broken, "dupes")
    with pytest.raises(ValueError, match="sözleşmeyi güncelleyin"):
        svc.accept_expected_change(engine, violation["id"], "Bu gerçek bir değişim", "alice")
    shifted = analyze(engine, source, make_frame(130, seed=1), "shift")
    analyze(engine, source, make_frame(130, seed=2), "newer")
    with pytest.raises(ValueError, match="en son"):
        svc.accept_expected_change(engine, shifted["id"], "Bu gerçek bir değişim", "alice")


def test_expected_change_api_requires_operator(engine, config, monkeypatch):
    from fastapi.testclient import TestClient
    from apps.api.deps import get_engine
    from apps.api.main import app
    app.dependency_overrides[get_engine] = lambda: engine
    reader, operator = "r" * 24, "o" * 24
    monkeypatch.setenv("SENTINEL_MODE", "pilot")
    monkeypatch.setenv("SENTINEL_API_TOKENS", json.dumps({reader: {"actor": "r", "role": "reader"},
                                                           operator: {"actor": "alice", "role": "operator"}}))
    try:
        source = svc.register_source(engine, "orders", config, "alice")
        svc.accept_baseline(engine, analyze(engine, source, make_frame(100), "one")["id"], "alice")
        shifted = analyze(engine, source, make_frame(130, seed=1), "campaign")
        url = f"/api/v1/reliability/observations/{shifted['id']}/expected-change"
        with TestClient(app) as client:
            client.headers["Authorization"] = "Bearer " + reader
            assert client.post(url, json={"note": "Kampanya nedeniyle"}).status_code == 403
            client.headers["Authorization"] = "Bearer " + operator
            assert client.post(url, json={"note": "kısa"}).status_code == 422
            assert client.post(url, json={"note": "Kampanya nedeniyle fiyat arttı"}).json()["quality"] == "accepted"
    finally:
        app.dependency_overrides.clear()


# --- snapshot ayrı tabloda -------------------------------------------------

def test_snapshot_lives_outside_the_observation_body(engine, config):
    config["retain_snapshot"] = True
    source = svc.register_source(engine, "orders", config, "alice")
    frame = make_frame(100, n=50)
    frame.loc[frame.index[:5], "amount"] *= 100
    item = analyze(engine, source, frame, "one")
    with engine.connect() as conn:
        stored = store.get(conn, store.observations, item["id"])
        assert stored["body"]["snapshot"] is None and stored["body"]["snapshot_available"]
        assert len(store.rows(conn, store.snapshots)) == 1
    assert svc.observation_detail(engine, item["id"])["body"]["snapshot_available"]
    action = {"type": "scale", "column": "amount", "factor": .01, "where": {"id": 0}}
    attempt = svc.try_repair(engine, item["id"], action, "alice")
    assert attempt["body"]["validation"]["changed_rows"] == 1


def test_legacy_inline_snapshot_still_works(engine, config):
    from sqlalchemy import update
    from pipeline_sentinel.repair import snapshot
    config["retain_snapshot"] = True
    source = svc.register_source(engine, "orders", config, "alice")
    frame = make_frame(100, n=50)
    frame.loc[0, "amount"] = 100000.0
    item = analyze(engine, source, frame, "legacy")
    with engine.begin() as conn:
        body = {**store.get(conn, store.observations, item["id"])["body"], "snapshot": snapshot(frame)}
        conn.execute(update(store.observations).where(store.observations.c.id == item["id"]).values(body=body))
        conn.execute(store.snapshots.delete())
    assert svc.observation_detail(engine, item["id"])["body"]["snapshot_available"]
    attempt = svc.try_repair(engine, item["id"], {"type": "scale", "column": "amount", "factor": .01, "where": {"id": 0}}, "alice")
    assert attempt["body"]["validation"]["changed_rows"] == 1


# --- webhook biçimi --------------------------------------------------------

def test_slack_webhook_format_is_valid_and_free_of_raw_data(engine, config, monkeypatch):
    from pipeline_sentinel import notifications
    source = svc.register_source(engine, "orders", config, "alice")
    analyze(engine, source, make_frame(100).assign(id=1), "duplicate-ids")
    monkeypatch.setenv("SENTINEL_WEBHOOK_URL", "https://hooks.example.invalid/x")
    monkeypatch.setenv("SENTINEL_WEBHOOK_SECRET", "s" * 24)
    monkeypatch.setenv("SENTINEL_WEBHOOK_FORMAT", "teams")
    with pytest.raises(ValueError, match="SENTINEL_WEBHOOK_FORMAT"):
        notifications.deliver_one(engine)
    monkeypatch.setenv("SENTINEL_WEBHOOK_FORMAT", "slack")
    sent = []

    class Response:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *args): pass

    class Transport:
        def open(self, request, timeout):
            sent.append(json.loads(request.data))
            return Response()

    monkeypatch.setattr(notifications, "build_opener", lambda *args: Transport())
    assert notifications.deliver_one(engine)["status"] == "sent"
    assert list(sent[0]) == ["text"] and "orders" in sent[0]["text"] and "sinyal" in sent[0]["text"]


# --- değişiklik adayları ---------------------------------------------------

def manifest(orders_sql="select 1", with_staging=True):
    nodes = {"model.p.orders": {"name": "orders", "raw_code": orders_sql, "depends_on": {"nodes": ["model.p.staging"] if with_staging else []}}}
    if with_staging:
        nodes["model.p.staging"] = {"name": "staging", "raw_code": "select 2", "depends_on": {"nodes": []}}
    return {"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"}, "nodes": nodes,
            "exposures": {"exposure.p.finance": {"name": "Finance", "depends_on": {"nodes": ["model.p.orders"]}}}}


def test_upstream_changes_since_last_good_run_are_listed_as_correlation_only(engine, config):
    config["lineage_asset"] = "model.p.orders"
    svc.ingest(engine, "p", manifest(), "alice")
    source = svc.register_source(engine, "orders", config, "alice")
    svc.accept_baseline(engine, analyze(engine, source, make_frame(100), "good")["id"], "alice")
    clean = analyze(engine, source, make_frame(100, seed=1), "still-good")
    assert clean["body"]["change_candidates"] == []

    changed = manifest()
    changed["nodes"]["model.p.staging"]["raw_code"] = "select 2 * 100"
    svc.ingest(engine, "p", changed, "alice", results={"results": [{"unique_id": "model.p.staging", "status": "error"}]})
    bad = analyze(engine, source, make_frame(300, seed=2), "after-deploy")
    assert bad["body"]["signals"]
    kinds = {(c["asset"], c["kind"], c["relation"]) for c in bad["body"]["change_candidates"]}
    assert ("model.p.staging", "definition_changed", "upstream") in kinds
    assert ("model.p.staging", "run_failed", "upstream") in kinds
    assert all(c["asset"] != "exposure.p.finance" for c in bad["body"]["change_candidates"]), "yalnızca yukarı akış"
    assert "nedensellik değil" in bad["body"]["change_candidates_note"]
    assert svc.replay(engine, bad["id"])["matches"], "adaylar RCA raporunu ve replay'i etkilemez"


def test_initial_ingest_is_not_a_change(engine, config):
    config["lineage_asset"] = "model.p.orders"
    source = svc.register_source(engine, "orders", config, "alice")
    svc.accept_baseline(engine, analyze(engine, source, make_frame(100), "good")["id"], "alice")
    svc.ingest(engine, "p", manifest(), "alice")
    bad = analyze(engine, source, make_frame(300, seed=2), "bad")
    assert bad["body"]["signals"] and bad["body"]["change_candidates"] == []


# --- CSV'den sözleşme önerisi ----------------------------------------------

def test_suggest_api_and_cli(tmp_path, engine):
    from fastapi.testclient import TestClient
    from typer.testing import CliRunner
    from apps.api.deps import get_engine
    from apps.api.main import app
    from pipeline_sentinel.cli import app as cli
    rows = "\n".join(f"{i},{100 + i % 40},2026-09-{1 + i % 28:02d}T10:00:00Z" for i in range(1, 61))
    csv = "order_id,amount,created_at\n" + rows + "\n"
    path = tmp_path / "sample.csv"
    path.write_text(csv, encoding="utf-8")
    result = CliRunner().invoke(cli, ["reliability", "suggest-contract", str(path), "--output", str(tmp_path / "out.yml")])
    assert result.exit_code == 0, result.output
    assert "expected_schema" in (tmp_path / "out.yml").read_text(encoding="utf-8")
    app.dependency_overrides[get_engine] = lambda: engine
    try:
        with TestClient(app) as client:
            body = client.post("/api/v1/reliability/contracts/suggest", json={"csv": csv}).json()
            assert body["contract"]["expected_schema"]["created_at"] == "datetime"
            assert client.post("/api/v1/reliability/contracts/suggest", json={"csv": "a,b\n"}).status_code == 422
            validated = client.post("/api/v1/reliability/contracts/validate", json={"yaml": body["yaml"]})
            assert validated.json()["valid"]
    finally:
        app.dependency_overrides.clear()
