"""Real PostgreSQL connector and durable workflow; isolated schema per test."""
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from pipeline_sentinel import db, reliability as svc, reliability_store as store
from pipeline_sentinel.connectors import PostgresConnector

pytestmark = pytest.mark.integration


@pytest.fixture
def source_setup(monkeypatch):
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL required")
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        engine.dispose()
        pytest.skip("Test PostgreSQL unavailable")
    db.run_migrations(engine)
    schema = "test_source_" + uuid4().hex[:12]
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        conn.execute(text(f'CREATE TABLE "{schema}".orders (id integer, batch text, amount double precision)'))
        conn.execute(text(f"INSERT INTO \"{schema}\".orders VALUES (1,'good',100),(2,'bad',200),(3,'bad',300)"))
    monkeypatch.setenv("SENTINEL_SOURCE_INTEGRATION", url)
    config = {"connection_env": "SENTINEL_SOURCE_INTEGRATION", "schema": schema, "table": "orders",
              "allowlist": [schema + ".orders"], "max_rows": 10, "order_by": ["id"], "retain_snapshot": True,
              "contract": {"expected_schema": {"id": "integer", "batch": "string", "amount": "float"},
                           "rules": [{"type": "uniqueness", "column": "id"}, {"type": "range", "column": "amount", "min": 0, "max": 1000}]}}
    yield engine, config
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def test_postgres_scan_full_bounded_partition_and_no_write(source_setup):
    engine, config = source_setup
    scanner = PostgresConnector()
    full = scanner.scan(config)
    assert full.coverage["complete"] and full.coverage["total_rows"] == 3
    partial = scanner.scan({**config, "max_rows": 1})
    assert not partial.coverage["complete"] and len(partial.frame) == 1
    partition = scanner.scan({**config, "partition": {"column": "id", "start": 2, "end": 4}})
    assert partition.coverage["total_rows"] == 2
    assert partition.frame.id.tolist() == [2, 3]
    with engine.connect() as conn:
        assert conn.execute(text(f'SELECT sum(amount) FROM "{config["schema"]}".orders')).scalar() == 600


def test_live_source_baseline_fault_sandbox_and_approval(source_setup):
    engine, config = source_setup
    source = svc.register_source(engine, config["schema"], config, "test")
    svc.enqueue(engine, source["id"], "baseline", "test")
    first = svc.work_once(engine)
    assert first["quality"] == "candidate", first
    svc.accept_baseline(engine, first["id"], "test")
    with engine.begin() as conn:
        conn.execute(text(f'UPDATE "{config["schema"]}".orders SET amount = amount * 100 WHERE batch = :batch'), {"batch": "bad"})
    svc.enqueue(engine, source["id"], "fault", "test")
    incident = svc.work_once(engine)
    assert incident["body"]["signals"], incident
    action = {"type": "scale", "column": "amount", "factor": .01, "where": {"batch": "bad"}}
    attempt = svc.try_repair(engine, incident["id"], action, "test")
    assert attempt["body"]["validation"]["passed"]
    assert svc.decide_repair(engine, attempt["id"], "approve", "reviewer")["status"] == "approved"
    assert svc.replay(engine, incident["id"])["matches"]
    with engine.connect() as conn:
        assert conn.execute(text(f'SELECT sum(amount) FROM "{config["schema"]}".orders')).scalar() == 50100


def test_live_schedule_expected_change_and_snapshot_table(source_setup):
    from datetime import datetime, timedelta, timezone
    engine, config = source_setup
    schema = config["schema"]
    with engine.begin() as conn:
        conn.execute(text(f'DELETE FROM "{schema}".orders'))
        conn.execute(text(f"INSERT INTO \"{schema}\".orders SELECT g, 'good', 100 + (g * 37) % 200 FROM generate_series(1, 300) g"))
    with engine.begin() as conn:  # paylaşılan test metadata'sında önceki koşulardan kalan zamanlanmış kaynak/iş olmasın
        conn.execute(store.jobs.delete().where(store.jobs.c.status == "pending"))
        for other in store.rows(conn, store.sources, limit=1000):
            if other["config"].get("schedule_minutes"):
                conn.execute(store.sources.delete().where(store.sources.c.id == other["id"]))
    source = svc.register_source(engine, schema, {**config, "max_rows": 1000, "schedule_minutes": 5}, "test")
    start = datetime(2026, 10, 7, 9, 0, tzinfo=timezone.utc)
    try:
        _scheduled_flow(engine, source, schema, start)
    finally:
        with engine.begin() as conn:  # zamanlanmış kaynak ve kalan işler, silinecek şemayı işaret etmesin
            conn.execute(store.jobs.delete().where(store.jobs.c.status == "pending"))
            conn.execute(store.sources.delete().where(store.sources.c.id == source["id"]))


def _scheduled_flow(engine, source, schema, start):
    from datetime import timedelta
    assert len(svc.enqueue_due(engine, start)) == 1
    first = svc.work_once(engine)
    assert first["quality"] == "candidate" and first["body"]["snapshot_available"]
    with engine.connect() as conn:
        assert len(store.rows(conn, store.snapshots, store.snapshots.c.observation_id == first["id"])) == 1
    svc.accept_baseline(engine, first["id"], "test")

    with engine.begin() as conn:
        conn.execute(text(f'UPDATE "{schema}".orders SET amount = amount * 1.3'))
    assert len(svc.enqueue_due(engine, start + timedelta(minutes=10))) == 1
    shifted = svc.work_once(engine)
    assert [s["type"] for s in shifted["body"]["signals"]] == ["distribution"]
    assert shifted["body"]["signals"][0]["evidence"]["method"] == "standard_error"

    svc.accept_expected_change(engine, shifted["id"], "Fiyat listesi %30 güncellendi", "reviewer")
    assert len(svc.enqueue_due(engine, start + timedelta(minutes=20))) == 1
    after = svc.work_once(engine)
    assert after["body"]["signals"] == [] and after["body"]["baseline"]["ids"] == [shifted["id"]]
    assert source["id"] == after["source_id"]


def test_connector_enforces_read_only_even_with_privileged_source(source_setup):
    engine, config = source_setup
    schema = config["schema"]
    with engine.begin() as conn:
        conn.execute(text(f'''CREATE FUNCTION "{schema}".try_write() RETURNS integer AS $$
            BEGIN INSERT INTO "{schema}".orders VALUES (4, 'bad', 999); RETURN 4; END;
            $$ LANGUAGE plpgsql VOLATILE'''))
        conn.execute(text(f'CREATE VIEW "{schema}".write_view AS SELECT "{schema}".try_write() AS id'))
    from sqlalchemy.exc import DBAPIError
    with pytest.raises(DBAPIError):
        PostgresConnector().scan({**config, "table": "write_view", "allowlist": [schema + ".write_view"]})
    with engine.connect() as conn:
        assert conn.execute(text(f'SELECT count(*) FROM "{schema}".orders')).scalar() == 3
