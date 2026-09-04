"""Faz 7 (approval workflow) — uçtan uca integration testi (canlı PostgreSQL).

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
        execute_pipeline_run(eng, fault_id=None, n_customers=30, n_orders=300, seed=5000 + i)

    return eng


def _make_incident(engine, fault_id: str, seed: int) -> str:
    outcome = execute_pipeline_run(engine, fault_id=fault_id, n_customers=30, n_orders=300, seed=seed)
    _, meta = analyze_run(engine, outcome.run_ids["orders"])
    return meta["incident_id"]


def test_actions_are_synced_after_analyze(engine):
    incident_id = _make_incident(engine, "F05", seed=6001)
    actions = pipeline_module.list_actions_for_incident(engine, incident_id)
    assert len(actions) == 2
    assert {a["type"] for a in actions} == {"sql_patch", "dbt_test"}
    assert all(a["status"] == "pending" for a in actions)
    assert all(a["incident_id"] == incident_id for a in actions)


def test_approve_action_updates_status_and_writes_audit_trail(engine):
    incident_id = _make_incident(engine, "F03", seed=6002)
    actions = pipeline_module.list_actions_for_incident(engine, incident_id)
    action = actions[0]

    result = pipeline_module.decide_action(engine, action["id"], "approve", actor="carol", note="ok")
    assert result["status"] == "approved"

    approvals = pipeline_module.list_approvals_for_action(engine, action["id"])
    assert len(approvals) == 1
    assert approvals[0]["decision"] == "approve"
    assert approvals[0]["actor"] == "carol"
    assert approvals[0]["note"] == "ok"


def test_reject_action(engine):
    incident_id = _make_incident(engine, "F01", seed=6003)
    actions = pipeline_module.list_actions_for_incident(engine, incident_id)
    action = actions[0]

    result = pipeline_module.decide_action(engine, action["id"], "reject", actor="dave")
    assert result["status"] == "rejected"


def test_decide_unknown_action_returns_none(engine):
    result = pipeline_module.decide_action(
        engine, "00000000-0000-0000-0000-000000000000", "approve", actor="x"
    )
    assert result is None


def test_decide_invalid_decision_raises(engine):
    incident_id = _make_incident(engine, "F06", seed=6004)
    action = pipeline_module.list_actions_for_incident(engine, incident_id)[0]
    with pytest.raises(ValueError):
        pipeline_module.decide_action(engine, action["id"], "maybe", actor="x")


def test_reanalysis_preserves_decided_action_status(engine):
    """Faz 7'nin kritik özelliği: aynı hipotez aynı aksiyonu üretmeye devam
    ettiği sürece bir kez verilen approve/reject kararı, incident yeniden
    analiz edildiğinde kaybolmamalı (idempotent sync, §14.3)."""
    outcome = execute_pipeline_run(engine, fault_id="F05", n_customers=30, n_orders=300, seed=6005)
    run_id = outcome.run_ids["orders"]
    _, meta = analyze_run(engine, run_id)
    incident_id = meta["incident_id"]

    actions = pipeline_module.list_actions_for_incident(engine, incident_id)
    sql_action = next(a for a in actions if a["type"] == "sql_patch")
    pipeline_module.decide_action(engine, sql_action["id"], "approve", actor="erin")

    # re-analyze the exact same run
    analyze_run(engine, run_id)

    actions_after = pipeline_module.list_actions_for_incident(engine, incident_id)
    sql_action_after = next(a for a in actions_after if a["type"] == "sql_patch")
    assert sql_action_after["id"] == sql_action["id"]
    assert sql_action_after["status"] == "approved"
    assert len(actions_after) == 2  # dbt_test still pending, not duplicated
