"""Faz 4 Root Cause Agent — uçtan uca integration testi (canlı PostgreSQL).

    export DATABASE_URL=postgresql+psycopg://sentinel:sentinel@localhost:5432/pipeline_sentinel
    pytest -m integration
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from pipeline_sentinel import db as db_module
from pipeline_sentinel import pipeline as pipeline_module
from pipeline_sentinel.analyze import analyze_run
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

    for i in range(8):
        execute_pipeline_run(eng, fault_id=None, n_customers=30, n_orders=300, seed=1000 + i)

    return eng


def test_f05_analyze_produces_scale_error_hypothesis_with_lineage_impact(engine):
    outcome = execute_pipeline_run(engine, fault_id="F05", n_customers=30, n_orders=300, seed=42)
    run_id = outcome.run_ids["orders"]

    report, meta = analyze_run(engine, run_id)

    assert report.severity in {"high", "critical"}
    assert report.root_causes, "F05 must produce at least one root cause hypothesis"
    assert "ölçek" in report.root_causes[0].hypothesis or "TL" in report.root_causes[0].hypothesis
    assert set(report.affected_assets) >= {"payments.amount", "finance_dashboard", "revenue_forecast_model"}
    assert report.llm_used is False  # no ANTHROPIC_API_KEY in this environment
    assert meta["incident_id"]
    assert meta["agent_run_id"]


def test_f01_analyze_resolves_dropped_column_name_correctly(engine):
    """Regresyon testi: dropped kolonun adı `signals`'ta kaybolmamalı
    (bkz. get_all_column_ids fix) — hipotez metninde kolon adı geçmeli."""
    outcome = execute_pipeline_run(engine, fault_id="F01", n_customers=30, n_orders=300, seed=43)
    run_id = outcome.run_ids["orders"]

    report, _ = analyze_run(engine, run_id)

    assert report.root_causes
    assert "customer_id" in report.root_causes[0].hypothesis
    assert report.root_causes[0].evidence_ids


def test_analyze_by_dataset_name_uses_latest_run(engine):
    execute_pipeline_run(engine, fault_id="F06", n_customers=30, n_orders=300, seed=44)
    latest = pipeline_module.get_latest_run(engine, "orders")
    report, _ = analyze_run(engine, latest["run_id"])
    assert "SLA" in report.root_causes[0].hypothesis


def test_healthy_run_analyze_has_no_root_causes(engine):
    outcome = execute_pipeline_run(engine, fault_id=None, n_customers=30, n_orders=300, seed=45)
    run_id = outcome.run_ids["orders"]
    report, _ = analyze_run(engine, run_id)
    assert report.severity == "low"
    assert report.root_causes == []


def test_incident_persisted_and_idempotent_on_rerun(engine):
    outcome = execute_pipeline_run(engine, fault_id="F03", n_customers=30, n_orders=300, seed=46)
    run_id = outcome.run_ids["orders"]

    report1, meta1 = analyze_run(engine, run_id)
    report2, meta2 = analyze_run(engine, run_id)

    # run_id UNIQUE constraint -> re-analyzing the same run updates, not duplicates
    assert meta1["incident_id"] == meta2["incident_id"]

    with engine.begin() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM incidents WHERE run_id = :run_id"), {"run_id": run_id}
        ).scalar()
    assert count == 1
