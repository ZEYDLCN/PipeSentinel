"""İzlemenin kendi sağlığı: worker kalp atışı, gecikmiş zamanlanmış kaynaklar, durum komutu."""
import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.pool import StaticPool

from pipeline_sentinel import reliability as svc, reliability_store as store
from pipeline_sentinel.connectors import ScanResult

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    store.metadata.create_all(engine)
    yield engine
    engine.dispose()


def config(**extra):
    return {"connection_env": "SENTINEL_SOURCE_TEST", "schema": "public", "table": "orders",
            "allowlist": ["public.orders"], "order_by": ["id"],
            "contract": {"expected_schema": {"id": "integer"}, "rules": []}, **extra}


class Fake:
    def scan(self, cfg):
        frame = pd.DataFrame({"id": range(5)})
        return ScanResult(frame, {"complete": True, "total_rows": 5, "scanned_rows": 5, "method": "t", "duration_seconds": 0})


def backdate(engine, table, **values):
    with engine.begin() as conn:
        conn.execute(update(table).values(**values))


def beat(engine, name="w1", seconds_ago=0):
    svc.heartbeat(engine, name, {"schedule": True})
    stamp = (NOW - timedelta(seconds=seconds_ago)).isoformat()
    with engine.begin() as conn:
        conn.execute(update(store.workers).where(store.workers.c.name == name).values(last_seen=stamp))


def test_empty_installation_is_healthy(engine):
    status = svc.monitoring_status(engine, NOW)
    assert status["status"] == "ok" and not status["worker_alive"] and status["notes"] == ["Canlı worker yok"]


def test_heartbeat_upserts_and_expires(engine):
    svc.heartbeat(engine, "host:1", {"a": 1})
    svc.heartbeat(engine, "host:1", {"a": 2})
    with engine.connect() as conn:
        rows = store.rows(conn, store.workers)
    assert len(rows) == 1 and rows[0]["details"] == {"a": 2} and rows[0]["started_at"] <= rows[0]["last_seen"]
    beat(engine, "host:1", seconds_ago=10)
    assert svc.monitoring_status(engine, NOW)["worker_alive"]
    beat(engine, "host:1", seconds_ago=120)
    assert not svc.monitoring_status(engine, NOW)["worker_alive"]
    ancient = (NOW - timedelta(days=8)).isoformat()
    backdate(engine, store.workers, last_seen=ancient)
    svc.heartbeat(engine, "host:2")  # temizlik tetiklenir
    with engine.connect() as conn:
        assert [w["name"] for w in store.rows(conn, store.workers)] == ["host:2"]


def test_pending_job_without_worker_is_down(engine):
    source = svc.register_source(engine, "orders", config(), "alice")
    svc.enqueue(engine, source["id"], "k", "alice")
    status = svc.monitoring_status(engine)
    assert status["status"] == "down" and status["queue"]["pending"] == 1
    svc.heartbeat(engine, "w")
    assert svc.monitoring_status(engine)["status"] == "ok"


def test_old_backlog_with_live_worker_is_degraded(engine):
    source = svc.register_source(engine, "orders", config(), "alice")
    svc.enqueue(engine, source["id"], "k", "alice")
    backdate(engine, store.jobs, created_at=(datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat())
    svc.heartbeat(engine, "w")
    status = svc.monitoring_status(engine)
    assert status["status"] == "degraded" and "dakikadır işlenmedi" in status["notes"][0]


def test_scheduled_source_goes_stalled_after_two_intervals_plus_grace(engine):
    source = svc.register_source(engine, "orders", config(schedule_minutes=60), "alice")
    svc.enqueue(engine, source["id"], "k", "alice")
    assert svc.work_once(engine, Fake())["quality"] == "candidate"
    created = datetime(2026, 10, 7, 9, 0, tzinfo=timezone.utc)
    backdate(engine, store.jobs, created_at=created.isoformat())
    backdate(engine, store.sources, created_at=(created - timedelta(days=1)).isoformat())
    beat(engine, "w", 0)

    def at(minutes):
        return svc.monitoring_status(engine, created + timedelta(minutes=minutes))["sources"][0]

    assert at(130)["state"] == "ok"            # 2 × 60 + 10 dk içinde
    stalled = svc.monitoring_status(engine, created + timedelta(minutes=135))
    assert stalled["sources"][0]["state"] == "stalled" and stalled["stalled"] == ["orders"]
    assert stalled["status"] in {"degraded", "down"}
    assert "orders" in " ".join(stalled["notes"])
    fresh_beat = svc.monitoring_status(engine, created + timedelta(minutes=135))
    assert fresh_beat["sources"][0]["age_minutes"] > 130


def test_failing_scheduled_source_counts_as_stalled(engine):
    class Broken:
        def scan(self, cfg):
            raise RuntimeError("down")

    source = svc.register_source(engine, "orders", config(schedule_minutes=5), "alice")
    backdate(engine, store.sources, created_at=(datetime.now(timezone.utc) - timedelta(hours=3)).isoformat())
    svc.enqueue(engine, source["id"], "k", "alice")
    assert svc.work_once(engine, Broken())["status"] == "failed"
    svc.heartbeat(engine, "w")
    status = svc.monitoring_status(engine)
    assert status["status"] == "degraded" and status["sources"][0]["state"] == "stalled"
    assert status["sources"][0]["last_success_at"] is None


def test_manual_sources_are_never_stalled_and_new_scheduled_ones_wait(engine):
    svc.register_source(engine, "manual", config(table="a", allowlist=["public.a"]), "alice")
    svc.register_source(engine, "fresh", config(table="b", allowlist=["public.b"], schedule_minutes=60), "alice")
    svc.heartbeat(engine, "w")
    status = svc.monitoring_status(engine)
    assert {s["name"]: s["state"] for s in status["sources"]} == {"manual": "manual", "fresh": "waiting"}
    assert status["status"] == "ok"


def test_ttl_env_override(engine, monkeypatch):
    beat(engine, "w", seconds_ago=30)
    assert svc.monitoring_status(engine, NOW)["worker_alive"]
    monkeypatch.setenv("SENTINEL_WORKER_TTL_SECONDS", "15")
    assert not svc.monitoring_status(engine, NOW)["worker_alive"]


def test_status_command_exit_codes(engine, monkeypatch):
    from typer.testing import CliRunner
    from pipeline_sentinel import db
    from pipeline_sentinel.cli import app
    monkeypatch.setattr(db, "get_engine", lambda: engine)
    run = lambda *args: CliRunner().invoke(app, ["reliability", "status", *args])
    assert run().exit_code == 0
    source = svc.register_source(engine, "orders", config(), "alice")
    svc.enqueue(engine, source["id"], "k", "alice")
    result = run()
    assert result.exit_code == 2 and json.loads(result.output)["status"] == "down"
    svc.heartbeat(engine, "w")
    backdate(engine, store.jobs, created_at=(datetime.now(timezone.utc) - timedelta(minutes=40)).isoformat())
    assert run().exit_code == 1
    assert run("--fail-on", "down").exit_code == 0, "degraded yalnızca --fail-on degraded ile başarısız sayılır"
    assert run("--fail-on", "bogus").exit_code != 0


def test_monitoring_endpoint_is_readable_by_reader(engine, monkeypatch):
    from fastapi.testclient import TestClient
    from apps.api.deps import get_engine
    from apps.api.main import app
    app.dependency_overrides[get_engine] = lambda: engine
    reader = "r" * 24
    monkeypatch.setenv("SENTINEL_MODE", "pilot")
    monkeypatch.setenv("SENTINEL_API_TOKENS", json.dumps({reader: {"actor": "r", "role": "reader"}}))
    try:
        svc.heartbeat(engine, "w")
        with TestClient(app) as client:
            assert client.get("/api/v1/reliability/monitoring").status_code == 401
            client.headers["Authorization"] = "Bearer " + reader
            body = client.get("/api/v1/reliability/monitoring").json()
            assert body["status"] == "ok" and body["worker_alive"] and body["workers"][0]["alive"]
    finally:
        app.dependency_overrides.clear()
