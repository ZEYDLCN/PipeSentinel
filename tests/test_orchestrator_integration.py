"""Uçtan uca orchestrator testi — canlı PostgreSQL gerektirir.

Çalıştırmak için:

    export DATABASE_URL=postgresql+psycopg://sentinel:sentinel@localhost:5432/pipeline_sentinel
    pytest -m integration

DATABASE_URL erişilemezse test otomatik olarak skip edilir (CI'da varsayılan
`pytest` çalışmasını bozmaz).
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from pipeline_sentinel import db as db_module
from pipeline_sentinel import pipeline as pipeline_module
from pipeline_sentinel.lineage import downstream_impact, upstream_sources
from pipeline_sentinel.orchestrator import execute_pipeline_run

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def engine():
    eng = db_module.get_engine()
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError:
        pytest.skip("DATABASE_URL'e erişilemiyor; integration testi atlanıyor")

    db_module.reset_schema(eng)
    db_module.run_migrations(eng)
    return eng


def test_healthy_baseline_then_healthy_run_has_low_severity(engine):
    for i in range(6):
        execute_pipeline_run(engine, fault_id=None, n_customers=30, n_orders=300, seed=100 + i)

    outcome = execute_pipeline_run(engine, fault_id=None, n_customers=30, n_orders=300, seed=999)
    for dataset, report in outcome.reports.items():
        assert report.severity in {"low", "medium"}, f"{dataset}: {report.to_dict()}"


def test_fault_f05_is_detected_as_high_or_critical(engine):
    outcome = execute_pipeline_run(engine, fault_id="F05", n_customers=30, n_orders=300, seed=2024)
    report = outcome.reports["orders"]
    assert report.severity in {"high", "critical"}
    assert {s.type for s in report.signals} >= {"range", "distribution"}


def test_signals_are_persisted(engine):
    outcome = execute_pipeline_run(engine, fault_id="F01", n_customers=30, n_orders=300, seed=777)
    run_id = outcome.run_ids["orders"]

    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT type, severity FROM signals WHERE run_id = :run_id"), {"run_id": run_id}
        ).all()
    assert any(t == "schema_missing_column" for t, _ in rows)


def test_lineage_graph_is_synced_after_run(engine):
    execute_pipeline_run(engine, fault_id=None, n_customers=30, n_orders=300, seed=555)

    edges = pipeline_module.fetch_all_edges(engine)
    edge_types = {e.edge_type for e in edges}
    assert edge_types == {"READS_FROM", "WRITES_TO", "DERIVED_FROM", "FEEDS"}

    total_amount_id = pipeline_module.find_column_id(engine, "orders", "total_amount")
    amount_id = pipeline_module.find_column_id(engine, "payments", "amount")
    assert total_amount_id is not None
    assert amount_id is not None

    downstream = {h.node_id for h in downstream_impact(edges, total_amount_id)}
    assert amount_id in downstream

    upstream = {h.node_id for h in upstream_sources(edges, amount_id)}
    assert total_amount_id in upstream

    dashboard_hits = [h for h in downstream_impact(edges, total_amount_id) if h.node_type == "external_asset"]
    names = {pipeline_module.resolve_node_name(engine, h.node_id, h.node_type) for h in dashboard_hits}
    assert names == {"finance_dashboard", "revenue_forecast_model"}


def test_f01_does_not_break_lineage_sync(engine):
    """F01 orders.customer_id kolonunu düşürür; sync bu run'da o kenarı
    atlamalı ama diğer kenarları/graf'ı bozmamalı (§9 uygulama notu)."""
    execute_pipeline_run(engine, fault_id="F01", n_customers=30, n_orders=300, seed=333)
    edges = pipeline_module.fetch_all_edges(engine)
    assert len(edges) > 0


def test_f02_fault_run_does_not_corrupt_canonical_column_catalog(engine):
    """Regresyon: register_dataset eskiden fault run'da bile ON CONFLICT ile
    `columns.data_type`'ı EZİYORDU — F02 (string->integer) sonrası katalog
    kalıcı olarak 'integer' gösteriyordu. track_column_types=False artık
    bunu engelliyor (bkz. orchestrator.py + pipeline.py::register_dataset)."""
    execute_pipeline_run(engine, fault_id=None, n_customers=30, n_orders=300, seed=444)
    execute_pipeline_run(engine, fault_id="F02", n_customers=30, n_orders=300, seed=445)

    dataset_id = pipeline_module.find_dataset_id(engine, "orders")
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT data_type FROM columns WHERE dataset_id = :id AND name = 'status'"),
            {"id": dataset_id},
        ).first()
    assert row[0] == "string"

    # bir sonraki sağlıklı run da katalogu bozmamalı
    execute_pipeline_run(engine, fault_id=None, n_customers=30, n_orders=300, seed=446)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT data_type FROM columns WHERE dataset_id = :id AND name = 'status'"),
            {"id": dataset_id},
        ).first()
    assert row[0] == "string"
