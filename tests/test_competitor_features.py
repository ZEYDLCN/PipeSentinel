"""Olay yaşam döngüsü, susturma, metrik geçmişi, öğrenilmiş tazelik ve PR özeti."""
import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.pool import StaticPool

from pipeline_sentinel import reliability as svc, reliability_store as store
from pipeline_sentinel.connectors import ScanResult, validate_source


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
            "contract": {"expected_schema": {"id": "integer", "amount": "float", "created_at": "datetime"},
                         "rules": [{"type": "uniqueness", "column": "id"}]}}


def make_frame(mean=100.0, n=300, seed=0, newest=None):
    rng = np.random.default_rng(seed)
    newest = newest or datetime(2026, 10, 7, 8, 0, tzinfo=timezone.utc)
    return pd.DataFrame({"id": range(n), "amount": rng.normal(mean, 20, n),
                         "created_at": pd.date_range(end=newest, periods=n, freq="min", tz="UTC")})


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


def incident(engine, config):
    source = svc.register_source(engine, "orders", config, "alice")
    svc.accept_baseline(engine, analyze(engine, source, make_frame(100), "good")["id"], "alice")
    return source, analyze(engine, source, make_frame(140, seed=1), "shifted")


# --- PR özeti --------------------------------------------------------------

def test_data_diff_reports_columns_and_samples_and_stays_json_safe():
    from pipeline_sentinel.repair import data_diff
    before = pd.DataFrame({"id": [1, 2, 3, 4], "amount": [10.0, 20.0, 30.0, None],
                           "when": pd.to_datetime(["2026-01-01"] * 4, utc=True), "tag": ["a", "b", "c", "d"]})
    after = before.copy()
    after.loc[1, "amount"] = 2000.0
    after.loc[3, "amount"] = 5.0
    after.loc[0, "tag"] = "z"
    after = pd.concat([after, pd.DataFrame({"id": [9], "amount": [1.0], "when": [after["when"][0]], "tag": ["n"]})], ignore_index=True)
    diff = data_diff(before, after.iloc[[0, 1, 3, 4]].reset_index(drop=True), ["id"], 1.0)
    assert diff["changed_by_column"] == {"amount": 2, "tag": 1}
    assert diff["added_rows"] == 1 and diff["removed_rows"] == 1
    assert diff["samples"]["added_keys"] == [9] and diff["samples"]["removed_keys"] == [3]
    by_key = {s["key"]: s["changes"] for s in diff["samples"]["changed"]}
    assert by_key[2]["amount"] == [20.0, 2000.0] and by_key[4]["amount"] == [None, 5.0]
    json.dumps(diff, allow_nan=False)
    # çok kolonlu anahtar
    multi = data_diff(before.assign(part=1), after.iloc[[0, 1, 3, 4]].reset_index(drop=True).assign(part=1), ["id", "part"], 1.0)
    assert multi["samples"]["changed"][0]["key"] == [1, 1]


def test_markdown_summary_is_readable_and_neutralises_cell_content():
    from pipeline_sentinel.ci_report import render_markdown
    report = {"passed": False,
              "diff": {"added_rows": 1, "removed_rows": 0, "changed_rows": 1, "changed_cells": 1, "change_ratio": 0.5,
                       "schema_changed": False, "changed_by_column": {"note": 1},
                       "numeric_totals": {"amount": {"before": 100.0, "after": 150.0}},
                       "samples": {"changed": [{"key": 7, "changes": {"note": ["a|b", "<script>x</script>\n`rm`"]}}],
                                   "added_keys": [9], "removed_keys": []}},
              "violations": [{"rule_type": "range", "column": "amount", "message": "x | y"}]}
    text = render_markdown(report)
    assert text.startswith("## Veri kalite kapısı: ❌ Başarısız")
    assert "| Değişim oranı | %50.00 |" in text and "+50" in text and "**Eklenen anahtarlar (ilk 5):** 9" in text
    assert "<script>" not in text and "a\\|b" in text and "x \\| y" in text and "`" not in text
    assert "✅ Geçti" in render_markdown({**report, "passed": True})


def test_ci_check_writes_markdown(tmp_path):
    from typer.testing import CliRunner
    from pipeline_sentinel.cli import app
    before, after, contract = tmp_path / "b.csv", tmp_path / "a.csv", tmp_path / "c.yml"
    before.write_text("id,amount\n1,100\n2,200\n", encoding="utf-8")
    after.write_text("id,amount\n1,100\n2,20000\n", encoding="utf-8")
    contract.write_text("expected_schema: {id: integer, amount: float}\nrules: [{type: range, column: amount, min: 0, max: 1000}]", encoding="utf-8")
    summary = tmp_path / "out" / "summary.md"
    result = CliRunner().invoke(app, ["reliability", "ci-check", str(before), str(after), "--contract", str(contract), "--keys", "id",
                                      "--output", str(tmp_path / "r.json"), "--markdown", str(summary)])
    assert result.exit_code == 1, result.output
    text = summary.read_text(encoding="utf-8")
    assert "❌ Başarısız" in text and "amount: 200 → 20000" in text and "range" in text


# --- olay yaşam döngüsü ----------------------------------------------------

def test_triage_lifecycle_is_audited_and_requires_an_incident(engine, config):
    source, item = incident(engine, config)
    assert svc.observation_detail(engine, item["id"])["triage"] is None
    svc.set_triage(engine, item["id"], "acknowledged", "ayse", "Kampanya mı bakıyorum", "bob")
    svc.set_triage(engine, item["id"], "resolved", "ayse", "Fiyat güncellemesiydi", "bob")
    detail = svc.observation_detail(engine, item["id"])
    assert detail["triage"]["status"] == "resolved" and detail["triage"]["assignee"] == "ayse"
    with engine.connect() as conn:
        assert len(store.rows(conn, store.triage)) == 1, "yinelenen kayıt oluşmamalı"
        events = [e for e in store.rows(conn, store.events) if e["kind"] == "triage_changed"]
        assert [e["body"]["status"] for e in reversed(events)] == ["acknowledged", "resolved"]
        assert svc.triage_map(conn, [item["id"]])[item["id"]]["actor"] == "bob"
    for bad in ("closed", ""):
        with pytest.raises(ValueError):
            svc.set_triage(engine, item["id"], bad, "", "", "bob")
    clean = analyze(engine, source, make_frame(100, seed=5), "clean")
    with pytest.raises(ValueError, match="Sinyalsiz"):
        svc.set_triage(engine, clean["id"], "acknowledged", "", "", "bob")


# --- susturma --------------------------------------------------------------

def test_mute_suppresses_notification_but_keeps_evidence(engine, config):
    source = svc.register_source(engine, "orders", config, "alice")
    svc.accept_baseline(engine, analyze(engine, source, make_frame(100), "good")["id"], "alice")
    first = analyze(engine, source, make_frame(140, seed=1), "one")
    assert first["body"]["muted"] is False
    with engine.connect() as conn:
        assert len(store.rows(conn, store.notifications)) == 1
    mute = svc.add_mute(engine, source["id"], "distribution", "amount", 24, "Kampanya haftası, bilerek yüksek", "bob")
    second = analyze(engine, source, make_frame(140, seed=2), "two")
    assert second["body"]["signals"], "kanıt kaydedilmeye devam eder"
    assert second["body"]["muted"] is True and second["body"]["mute_ids"] == [mute["id"]]
    with engine.connect() as conn:
        assert len(store.rows(conn, store.notifications)) == 1, "susturulan olay için yeni bildirim yok"
    assert svc.observation_detail(engine, second["id"])["mutes"][0]["reason"].startswith("Kampanya")

    svc.remove_mute(engine, mute["id"], "bob")
    assert svc.active_mutes(engine) == []
    third = analyze(engine, source, make_frame(140, seed=3), "three")
    assert third["body"]["muted"] is False
    with engine.connect() as conn:
        assert len(store.rows(conn, store.notifications)) == 2


def test_mute_must_cover_every_signal_and_expires(engine, config):
    source = svc.register_source(engine, "orders", config, "alice")
    svc.accept_baseline(engine, analyze(engine, source, make_frame(100), "good")["id"], "alice")
    svc.add_mute(engine, source["id"], "distribution", "amount", 1, "Yalnızca dağılım susturuldu", "bob")
    broken = make_frame(140, seed=1).assign(id=1)  # dağılım + uniqueness
    item = analyze(engine, source, broken, "two-signals")
    assert {s["type"] for s in item["body"]["signals"]} >= {"distribution", "uniqueness"}
    assert item["body"]["muted"] is False, "susturulmamış sinyal varsa bildirim gider"
    with engine.begin() as conn:
        conn.execute(update(store.mutes).values(until="2000-01-01T00:00:00+00:00"))
    assert svc.active_mutes(engine) == []


@pytest.mark.parametrize("kwargs", [dict(hours=0), dict(hours=721), dict(hours=True), dict(reason="kısa"),
                                    dict(signal_type="")])
def test_mute_validation(engine, config, kwargs):
    source = svc.register_source(engine, "orders", config, "alice")
    params = dict(signal_type="volume", column=None, hours=24, reason="Geçerli bir gerekçe", **{})
    params.update(kwargs)
    with pytest.raises(ValueError):
        svc.add_mute(engine, source["id"], params["signal_type"], params["column"], params["hours"], params["reason"], "bob")


def test_triage_mute_and_history_api_roles(engine, config, monkeypatch):
    from fastapi.testclient import TestClient
    from apps.api.deps import get_engine
    from apps.api.main import app
    app.dependency_overrides[get_engine] = lambda: engine
    reader, operator = "r" * 24, "o" * 24
    monkeypatch.setenv("SENTINEL_MODE", "pilot")
    monkeypatch.setenv("SENTINEL_API_TOKENS", json.dumps({reader: {"actor": "r", "role": "reader"},
                                                           operator: {"actor": "alice", "role": "operator"}}))
    try:
        source, item = incident(engine, config)
        root = "/api/v1/reliability"
        triage = {"status": "acknowledged", "assignee": "ayse", "note": "bakıyorum"}
        mute = {"source_id": source["id"], "signal_type": "distribution", "column": "amount", "hours": 24, "reason": "Kampanya haftası"}
        with TestClient(app) as client:
            client.headers["Authorization"] = "Bearer " + reader
            assert client.post(f"{root}/observations/{item['id']}/triage", json=triage).status_code == 403
            assert client.post(f"{root}/mutes", json=mute).status_code == 403
            assert client.get(f"{root}/sources/{source['id']}/history").json()["points"]
            client.headers["Authorization"] = "Bearer " + operator
            assert client.post(f"{root}/observations/{item['id']}/triage", json={**triage, "status": "bogus"}).status_code == 422
            assert client.post(f"{root}/observations/{item['id']}/triage", json=triage).json()["status"] == "acknowledged"
            created = client.post(f"{root}/mutes", json=mute).json()
            assert client.post(f"{root}/mutes", json={**mute, "hours": 0}).status_code == 422
            assert client.post(f"{root}/mutes", json={**mute, "source_id": "missing"}).status_code == 404
            assert [m["id"] for m in client.get(f"{root}/mutes").json()] == [created["id"]]
            listed = client.get(f"{root}/observations").json()
            assert listed[0]["triage"] == {"status": "acknowledged", "assignee": "ayse"} and listed[0]["muted"] is False
            assert listed[1]["triage"] is None
            client.headers["Authorization"] = "Bearer " + reader
            assert client.delete(f"{root}/mutes/{created['id']}").status_code == 403
            client.headers["Authorization"] = "Bearer " + operator
            assert client.delete(f"{root}/mutes/{created['id']}").json()["active"] is False
            assert client.get(f"{root}/mutes").json() == []
            assert client.get(f"{root}/sources/{source['id']}/history?limit=0").status_code == 422
            assert client.get(f"{root}/sources/missing/history").status_code == 404
    finally:
        app.dependency_overrides.clear()


# --- metrik geçmişi --------------------------------------------------------

def test_metric_history_is_chronological_and_scoped_to_current_config(engine, config):
    source = svc.register_source(engine, "orders", config, "alice")
    for i, mean in enumerate((100, 101, 102)):
        analyze(engine, source, make_frame(mean, seed=i), f"run{i}")
    analyze(engine, source, make_frame(300, seed=9), "shifted")
    history = svc.metric_history(engine, source["id"])
    assert [p["rows"] for p in history["points"]] == [300] * 4
    assert history["points"][-1]["signals"] == 0 and history["points"][0]["at"] < history["points"][-1]["at"]
    assert "amount" in history["columns"] and "id" in history["columns"]
    means = [p["columns"]["amount"]["mean"] for p in history["points"]]
    assert means[-1] > 250 > means[0]
    svc.register_source(engine, "orders", {**config, "max_rows": 500}, "alice")  # analiz yapılandırması değişti
    analyze(engine, source, make_frame(100, seed=4), "new-config")
    assert len(svc.metric_history(engine, source["id"])["points"]) == 1
    assert svc.metric_history(engine, source["id"], 2)["points"][-1]["rows"] == 300
    with pytest.raises(ValueError):
        svc.metric_history(engine, source["id"], 0)


# --- öğrenilmiş tazelik ----------------------------------------------------

def test_learned_freshness_validation(config):
    assert validate_source({**config, "learned_freshness": ["created_at"]})["learned_freshness"] == ["created_at"]
    for bad in (["amount"], ["missing"], "created_at", ["created_at"] * 11):
        with pytest.raises(ValueError):
            validate_source({**config, "learned_freshness": bad})


def test_learned_freshness_flags_a_stalled_table_without_any_sla(engine, config):
    source = svc.register_source(engine, "orders", {**config, "learned_freshness": ["created_at"]}, "alice")
    now = datetime.now(timezone.utc)
    for i in range(6):  # tablo her çalışmada 5 dk önceye kadar güncel
        item = analyze(engine, source, make_frame(100, seed=i, newest=datetime.now(timezone.utc) - timedelta(minutes=5)), f"ok{i}")
        assert not item["body"]["signals"]
        svc.accept_baseline(engine, item["id"], "alice")
    stalled = analyze(engine, source, make_frame(100, seed=7, newest=now - timedelta(hours=6)), "stalled")
    stale = [s for s in stalled["body"]["signals"] if s["type"] == "staleness"]
    assert len(stale) == 1 and stale[0]["column"] == "created_at"
    evidence = stale[0]["evidence"]
    assert evidence["method"] == "learned" and evidence["lag_minutes"] > 300 > evidence["typical_lag_minutes"]
    assert "stale_data" in json.dumps(stalled["body"]["rca"]["recommended_actions"]) or "geç" in stalled["body"]["rca"]["summary"]
    assert stalled["body"]["rca"]["root_causes"][0]["evidence_ids"] == [stale[0]["id"]]
    json.dumps(stalled["body"], default=str, allow_nan=False)


def test_learned_freshness_needs_history_and_tolerates_jitter(engine, config):
    source = svc.register_source(engine, "orders", {**config, "learned_freshness": ["created_at"]}, "alice")
    early = analyze(engine, source, make_frame(100, newest=datetime.now(timezone.utc) - timedelta(days=3)), "cold")
    assert not early["body"]["signals"], "geçmiş yokken tazelik öğrenilemez"
    for i, minutes in enumerate((4, 9, 5, 12, 6, 8)):
        item = analyze(engine, source, make_frame(100, seed=i, newest=datetime.now(timezone.utc) - timedelta(minutes=minutes)), f"j{i}")
        svc.accept_baseline(engine, item["id"], "alice")
    normal = analyze(engine, source, make_frame(100, seed=8, newest=datetime.now(timezone.utc) - timedelta(minutes=14)), "normal")
    assert not [s for s in normal["body"]["signals"] if s["type"] == "staleness"]
