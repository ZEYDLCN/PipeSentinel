"""Geçmişten öğrenen anomali, Isolation Forest (çok değişkenli) ve DuckDB connector."""
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from pipeline_sentinel import baseline, reliability as svc, reliability_store as store
from pipeline_sentinel.anomaly import history_anomalies, multivariate_drift
from pipeline_sentinel.connectors import ScanResult, get_connector, validate_source


@pytest.fixture
def engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    store.metadata.create_all(engine)
    yield engine
    engine.dispose()


def history_item(rows=1000, mean=100.0, null_ratio=0.0, created="2026-10-01T09:00:00+00:00"):
    return {"id": created, "created_at": created, "quality": "accepted",
            "body": {"coverage": {"total_rows": rows},
                     "profile": {"amount": {"mean": mean, "null_ratio": null_ratio, "stddev": 10.0},
                                 "order_id": {"mean": 500.0, "null_ratio": 0.0}}}}


def stable_history(n=10):
    return [history_item(rows=1000 + (i % 3) * 4 - 4, mean=100 + (i % 3) * 0.5, created=f"2026-09-{10 + i:02d}T09:00:00+00:00")
            for i in range(n)]


# --- geçmişten öğrenen eşikler ---------------------------------------------

def test_stable_metrics_learn_a_tight_range():
    history = stable_history()
    profile = {"amount": {"mean": 100.0, "null_ratio": 0.0}, "order_id": {"mean": 9e9, "null_ratio": 0.0}}
    # %13 hacim düşüşü: sabit %30 eşiği kaçırır, öğrenilen aralık yakalar
    found = history_anomalies(profile, {"total_rows": 870}, history, set())
    volume = [s for s in found if s.type == "volume"]
    assert len(volume) == 1 and volume[0].evidence["method"] == "history" and volume[0].evidence["z_score"] < -6
    assert not [s for s in found if s.column == "order_id"], "kimlik kolonları ortalamaya göre denetlenmez"
    assert history_anomalies(profile, {"total_rows": 1000}, history, set()) == []


def test_noise_within_the_learned_range_is_not_an_anomaly():
    rng = np.random.default_rng(3)
    history = [history_item(rows=int(rng.normal(1000, 5)), mean=float(rng.normal(100, 0.5)),
                            created=f"2026-09-{1 + i:02d}T09:00:00+00:00") for i in range(25)]
    for _ in range(30):
        profile = {"amount": {"mean": float(rng.normal(100, 0.5)), "null_ratio": 0.0}}
        assert history_anomalies(profile, {"total_rows": int(rng.normal(1000, 5))}, history, set()) == []


def test_mean_null_ratio_and_dedup():
    history = stable_history()
    shifted = history_anomalies({"amount": {"mean": 115.0, "null_ratio": 0.0}}, {"total_rows": 1000}, history, set())
    assert [(s.type, s.column) for s in shifted] == [("distribution", "amount")]
    assert history_anomalies({"amount": {"mean": 115.0, "null_ratio": 0.0}}, {"total_rows": 1000}, history,
                             {("distribution", "amount")}) == []
    nulls = history_anomalies({"amount": {"mean": 100.0, "null_ratio": 0.03}}, {"total_rows": 1000}, history, set())
    assert [(s.type, s.column) for s in nulls] == [("completeness", "amount")]
    assert history_anomalies({"amount": {"mean": 100.0, "null_ratio": 0.01}}, {"total_rows": 1000}, history, set()) == []
    assert history_anomalies({"amount": {"mean": 100.0, "null_ratio": 0.0}}, {"total_rows": 1200}, history[:7], set()) == [], \
        "yetersiz geçmişte (8'den az) sinyal yok"


def test_seasonal_cohort_uses_only_matching_weekday_and_hour():
    today = datetime(2026, 10, 12, 9, tzinfo=timezone.utc)
    assert today.weekday() == 0  # pazartesi 09:00 kohortu
    monday = lambda week: (today - timedelta(weeks=week + 1)).isoformat()
    same = [history_item(rows=5000 + week, created=monday(week)) for week in range(5)]
    other = [history_item(rows=100, created=(today - timedelta(days=i, hours=-6)).isoformat())
             for i in range(1, 30) if i % 7 != 0]
    now = today.isoformat()
    assert baseline.cohort(now) == baseline.cohort(monday(0))
    assert all(baseline.cohort(h["created_at"]) != baseline.cohort(now) for h in other)
    history = sorted(same + other, key=lambda h: h["created_at"], reverse=True)
    profile = {"amount": {"mean": 100.0, "null_ratio": 0.0}}
    assert history_anomalies(profile, {"total_rows": 5002}, history, set(), baseline.cohort, now) == []
    found = history_anomalies(profile, {"total_rows": 3000}, history, set(), baseline.cohort, now)
    assert [s.type for s in found] == ["volume"]
    assert history_anomalies(profile, {"total_rows": 3000}, history[:0] + other + same[:3], set(), baseline.cohort, now) == []


# --- pipeline içinde -------------------------------------------------------

def base_config(**extra):
    return {"connection_env": "SENTINEL_SOURCE_TEST", "schema": "public", "table": "orders",
            "allowlist": ["public.orders"], "order_by": ["id"], "auto_baseline": True,
            "contract": {"expected_schema": {"id": "integer", "amount": "float", "fee": "float"}, "rules": []}, **extra}


class FakeConnector:
    def __init__(self, frame):
        self.frame = frame

    def scan(self, config):
        return ScanResult(self.frame.copy(), {"complete": True, "total_rows": len(self.frame), "scanned_rows": len(self.frame),
                                              "method": "test", "duration_seconds": 0})


def frame_of(n=600, seed=0, broken=False):
    rng = np.random.default_rng(seed)
    amount = rng.normal(100, 20, n)
    fee = amount * 0.1 + rng.normal(0, 0.2, n)
    if broken:
        fee = rng.permutation(fee)  # kolon ortalamaları/yayılımı aynı, ilişki bozuk
    return pd.DataFrame({"id": range(n), "amount": amount, "fee": fee})


def analyze(engine, source, frame, key):
    svc.enqueue(engine, source["id"], key, "operator")
    result = svc.work_once(engine, FakeConnector(frame))
    assert "body" in result, result
    return result


def test_history_signal_flows_through_the_worker_and_rca(engine):
    source = svc.register_source(engine, "orders", base_config(), "alice")
    svc.accept_baseline(engine, analyze(engine, source, frame_of(1000, 0), "first")["id"], "alice")
    for i, n in enumerate((1004, 996, 1000, 1002, 998, 1001, 999, 1003)):
        item = analyze(engine, source, frame_of(n, i + 1), f"run{i}")
        assert not item["body"]["signals"] and item["quality"] == "accepted", "auto_baseline geçmişi biriktirir"
    drop = analyze(engine, source, frame_of(870, 99), "drop")
    types = {(s["type"], s["column"]): s for s in drop["body"]["signals"]}
    assert types[("volume", None)]["evidence"]["method"] == "history"
    assert drop["body"]["rca"]["root_causes"], "RCA sinyali açıklar"
    assert svc.replay(engine, drop["id"])["matches"]


def test_adaptive_detection_can_be_switched_off(engine):
    source = svc.register_source(engine, "orders", base_config(adaptive_detection=False), "alice")
    svc.accept_baseline(engine, analyze(engine, source, frame_of(1000, 0), "first")["id"], "alice")
    for i, n in enumerate((1004, 996, 1000, 1002, 998, 1001, 999, 1003)):
        analyze(engine, source, frame_of(n, i + 1), f"run{i}")
    assert analyze(engine, source, frame_of(870, 99), "drop")["body"]["signals"] == []


def test_multivariate_config_validation():
    with pytest.raises(ValueError, match="retain_snapshot"):
        validate_source(base_config(multivariate=True))
    assert validate_source(base_config(multivariate=True, retain_snapshot=True))["multivariate"] is True
    for key in ("multivariate", "adaptive_detection"):
        with pytest.raises(ValueError):
            validate_source(base_config(**{key: "yes"}))
    with pytest.raises(ValueError):
        validate_source(base_config(type="mysql"))


def test_dbt_ingestion_works_without_the_sqlglot_extra(monkeypatch):
    from pipeline_sentinel import column_lineage
    from pipeline_sentinel.ingestion import dbt_graph
    monkeypatch.setattr(column_lineage, "available", lambda: False)
    manifest = {"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"},
                "nodes": {"model.p.a": {"name": "a", "resource_type": "model", "compiled_code": "select 1 as x",
                                        "depends_on": {"nodes": []}}}}
    graph = dbt_graph(manifest)
    assert graph["column_edges"] == [] and "sqlglot" in graph["coverage"] and list(graph["nodes"]) == ["model.p.a"]


# --- çok değişkenli (Mahalanobis + isteğe bağlı Isolation Forest) ----------

def correlated(n=1500, seed=0, shuffle_y=False):
    rng = np.random.default_rng(seed)
    x = rng.normal(0, 1, n)
    y = 2 * x + rng.normal(0, 0.1, n)
    if shuffle_y:
        y = rng.permutation(y)
    return pd.DataFrame({"order_id": range(n), "x": x, "y": y, "z": rng.normal(5, 1, n)})


def test_broken_relationship_is_caught_although_every_marginal_is_unchanged():
    reference = correlated(seed=1)
    assert multivariate_drift(reference, correlated(seed=2)) is None, "aynı süreçten yeni örnek alarm vermemeli"
    broken = correlated(seed=3, shuffle_y=True)
    assert abs(broken["y"].mean() - reference["y"].mean()) < 0.2 and abs(broken["y"].std() - reference["y"].std()) < 0.2
    signal = multivariate_drift(reference, broken)
    assert signal is not None and signal.type == "multivariate_drift" and signal.source == "baseline"
    evidence = signal.evidence
    assert evidence["method"] == "mahalanobis" and evidence["outlier_rate"] > 0.5 and evidence["p_value"] < 1e-6
    assert "order_id" not in evidence["columns"] and set(evidence["columns"]) == {"x", "y", "z"}
    assert multivariate_drift(reference, broken).evidence == evidence, "deterministik"
    json.dumps(evidence, allow_nan=False)


def test_isolation_forest_adds_global_outlier_detection():
    pytest.importorskip("sklearn")
    rng = np.random.default_rng(5)
    reference = pd.DataFrame({"a": rng.normal(0, 1, 2000), "b": rng.normal(0, 1, 2000)})
    current = pd.DataFrame({"a": np.r_[rng.normal(0, 1, 850), rng.normal(9, 0.3, 150)],
                            "b": np.r_[rng.normal(0, 1, 850), rng.normal(9, 0.3, 150)]})  # %15 yeni küme
    signal = multivariate_drift(reference, current)
    assert signal is not None
    detectors = signal.evidence["detectors"]
    assert detectors["isolation_forest"]["rate"] >= 0.1 and detectors["mahalanobis"]["rate"] >= 0.1
    assert multivariate_drift(reference, pd.DataFrame({"a": rng.normal(0, 1, 1000), "b": rng.normal(0, 1, 1000)})) is None


def test_multivariate_guards():
    reference = correlated(seed=1)
    assert multivariate_drift(reference.head(50), correlated(seed=3, shuffle_y=True)) is None
    assert multivariate_drift(reference[["order_id", "x"]], correlated(seed=3, shuffle_y=True)) is None
    constant = reference.assign(z=1.0, y=2.0)
    assert multivariate_drift(constant, correlated(seed=3, shuffle_y=True)) is None  # yalnızca x kaldı


def test_without_scikit_learn_the_relationship_detector_still_works(monkeypatch):
    monkeypatch.setitem(sys.modules, "sklearn.ensemble", None)
    signal = multivariate_drift(correlated(seed=1), correlated(seed=3, shuffle_y=True))
    assert signal is not None and signal.evidence["detectors"]["isolation_forest"] is None
    assert signal.evidence["method"] == "mahalanobis"


def test_multivariate_signal_end_to_end_and_expected_change(engine):
    config = base_config(multivariate=True, retain_snapshot=True)
    source = svc.register_source(engine, "orders", config, "alice")
    svc.accept_baseline(engine, analyze(engine, source, frame_of(600, 0), "first")["id"], "alice")
    clean = analyze(engine, source, frame_of(600, 1), "clean")
    assert not [s for s in clean["body"]["signals"] if s["type"] == "multivariate_drift"]
    broken = analyze(engine, source, frame_of(600, 2, broken=True), "broken")
    joint = [s for s in broken["body"]["signals"] if s["type"] == "multivariate_drift"]
    assert len(joint) == 1 and joint[0]["evidence"]["flagged_rows"] > 100
    assert "ortak dağılım" in broken["body"]["rca"]["root_causes"][0]["hypothesis"] or \
        any("birlikte dağılımı" in h["hypothesis"] for h in broken["body"]["rca"]["root_causes"])
    result = svc.accept_expected_change(engine, broken["id"], "Ücret hesaplaması bilerek değiştirildi", "bob")
    assert result["quality"] == "accepted"


# --- DuckDB connector ------------------------------------------------------

@pytest.fixture
def duckdb():
    return pytest.importorskip("duckdb")


@pytest.fixture
def duck(tmp_path, monkeypatch, duckdb):
    path = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE orders AS SELECT i AS id, 'k' || (i % 3) AS batch, 100.0 + i AS amount, "
                "TIMESTAMP '2026-10-01 00:00:00' + i * INTERVAL 1 HOUR AS created_at FROM range(1, 101) t(i)")
    con.close()
    monkeypatch.setenv("SENTINEL_SOURCE_DUCK", str(path))
    config = {"type": "duckdb", "connection_env": "SENTINEL_SOURCE_DUCK", "schema": "main", "table": "orders",
              "allowlist": ["main.orders"], "order_by": ["id"], "max_rows": 1000,
              "contract": {"expected_schema": {"id": "integer", "batch": "string", "amount": "float", "created_at": "datetime"},
                           "rules": [{"type": "uniqueness", "column": "id"}]}}
    return path, config


def test_duckdb_scan_modes_and_coverage(duck):
    path, config = duck
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    connector = get_connector(config)
    full = connector.scan(config)
    assert full.coverage["complete"] and full.coverage["total_rows"] == 100 and list(full.frame["id"][:3]) == [1, 2, 3]
    assert "datetime" in str(full.frame["created_at"].dtype)
    partial = connector.scan({**config, "max_rows": 10})
    assert not partial.coverage["complete"] and len(partial.frame) == 10 and partial.coverage["total_rows"] == 100
    ids = connector.scan({**config, "partition": {"column": "id", "start": 10, "end": 20}})
    assert ids.coverage["scope"] == "partition" and ids.coverage["total_rows"] == 10 and ids.frame["id"].tolist() == list(range(10, 20))
    days = connector.scan({**config, "partition": {"column": "created_at", "start": "2026-10-02", "end": "2026-10-03"}})
    assert days.coverage["total_rows"] == 24
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before, "kaynak dosya değişmemeli"


def test_duckdb_opens_read_only_and_validates(duck, monkeypatch, duckdb):
    _, config = duck
    calls = []
    real = duckdb.connect

    def spy(*args, **kwargs):
        calls.append(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", spy)
    get_connector(config).scan(config)
    assert calls and all(call.get("read_only") is True for call in calls)
    monkeypatch.setenv("SENTINEL_SOURCE_DUCK", "https://example.invalid/x.duckdb")
    with pytest.raises(ValueError, match="dosyasının yolunu"):
        get_connector(config).scan(config)
    monkeypatch.delenv("SENTINEL_SOURCE_DUCK")
    with pytest.raises(ValueError, match="ortam değişkeni"):
        get_connector(config).scan(config)
    with pytest.raises(ValueError):
        get_connector({"type": "oracle"})
    with pytest.raises(ValueError, match="allowlist"):
        validate_source({**config, "table": "secret"})


def test_duckdb_query_timeout(tmp_path, monkeypatch, duckdb):
    path = tmp_path / "slow.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE VIEW slow AS SELECT i FROM range(10) t(i) WHERE (SELECT sum(x) FROM range(20000000000) r(x)) > 0")
    con.close()
    monkeypatch.setenv("SENTINEL_SOURCE_DUCK", str(path))
    config = {"type": "duckdb", "connection_env": "SENTINEL_SOURCE_DUCK", "schema": "main", "table": "slow",
              "allowlist": ["main.slow"], "timeout_seconds": 1, "contract": {"expected_schema": {"i": "integer"}, "rules": []}}
    started = datetime.now()
    with pytest.raises(ValueError, match="süre sınırını"):
        get_connector(config).scan(config)
    assert (datetime.now() - started).total_seconds() < 30


def test_duckdb_source_runs_through_the_worker(duck, engine, duckdb):
    _, config = duck
    source = svc.register_source(engine, "warehouse", config, "alice")
    svc.enqueue(engine, source["id"], "first", "alice")
    first = svc.work_once(engine)  # varsayılan connector: type=duckdb
    assert first["quality"] == "candidate" and first["body"]["coverage"]["total_rows"] == 100
    svc.accept_baseline(engine, first["id"], "alice")
    con = duckdb.connect(str(duck[0]))
    con.execute("UPDATE orders SET amount = amount * 100 WHERE id <= 5")
    con.close()
    svc.enqueue(engine, source["id"], "second", "alice")
    second = svc.work_once(engine)
    assert second["body"]["baseline"]["ids"] == [first["id"]]
    assert any(s["column"] == "amount" for s in second["body"]["signals"])
