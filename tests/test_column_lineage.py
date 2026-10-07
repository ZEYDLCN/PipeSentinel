"""Kolon seviyesinde lineage: dbt derlenmiş SQL (sqlglot) ve OpenLineage columnLineage facet."""
import json

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from pipeline_sentinel import reliability as svc, reliability_store as store
from pipeline_sentinel.connectors import ScanResult
from pipeline_sentinel.ingestion import dbt_graph, openlineage_graph, reachable_columns

sqlglot = pytest.importorskip("sqlglot")

SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"


def source(name, columns):
    return {"name": name, "resource_type": "source", "relation_name": f'"db"."raw"."{name}"',
            "columns": {c: {} for c in columns}, "depends_on": {"nodes": []}}


def model(name, sql, deps, columns=()):
    return {"name": name, "resource_type": "model", "relation_name": f'"db"."analytics"."{name}"', "raw_code": sql,
            "compiled_code": sql, "depends_on": {"nodes": deps}, "columns": {c: {} for c in columns}}


def manifest():
    return {
        "metadata": {"dbt_schema_version": SCHEMA, "adapter_type": "postgres"},
        "sources": {"source.p.raw.orders": source("orders", ["order_id", "total_amount", "currency"]),
                    "source.p.raw.fx": source("fx", ["currency", "rate"])},
        "nodes": {
            "model.p.stg_orders": model("stg_orders", 'select order_id, total_amount * 0.01 as amount, currency from "db"."raw"."orders"',
                                        ["source.p.raw.orders"]),
            "model.p.fx": model("fx", 'select currency, rate from "db"."raw"."fx"', ["source.p.raw.fx"]),
            "model.p.orders": model(
                "orders",
                'select o.order_id, o.amount * f.rate as amount_try, o.currency '
                'from "db"."analytics"."stg_orders" as o join "db"."analytics"."fx" as f on f.currency = o.currency',
                ["model.p.stg_orders", "model.p.fx"], ["order_id", "amount_try", "currency"]),
            "model.p.daily_revenue": model("daily_revenue", 'select * from "db"."analytics"."orders"', ["model.p.orders"]),
            "model.p.with_external": model("with_external", 'select x.id from "db"."ext"."thing" as x', []),
        },
        "exposures": {"exposure.p.finance": {"name": "Finance", "depends_on": {"nodes": ["model.p.daily_revenue"]}}},
    }


def edge_set(graph):
    return {tuple(e) for e in graph["column_edges"]}


def test_dbt_compiled_sql_yields_column_edges_with_aliases_joins_and_star():
    graph = dbt_graph(manifest())
    edges = edge_set(graph)
    assert ("source.p.raw.orders", "total_amount", "model.p.stg_orders", "amount") in edges
    assert ("source.p.raw.orders", "order_id", "model.p.stg_orders", "order_id") in edges
    assert ("model.p.stg_orders", "amount", "model.p.orders", "amount_try") in edges
    assert ("model.p.fx", "rate", "model.p.orders", "amount_try") in edges, "join üzerinden iki kaynaktan türetilen kolon"
    assert ("model.p.orders", "amount_try", "model.p.daily_revenue", "amount_try") in edges, "select * şemayla genişler"
    assert not any(e[2] == "model.p.with_external" for e in edges), "manifest dışı tablo için kenar uydurulmaz"
    stats = graph["column_stats"]
    assert stats["unresolved_tables"] >= 1 and stats["edges"] == len(edges) and stats["dialect"] == "postgres"
    assert "best-effort" in graph["coverage"]
    json.dumps(graph, allow_nan=False)


def test_unqualified_columns_from_a_table_missing_in_the_schema_are_still_traced():
    """Kısmi şemada (tablo belgelenmemiş) nitelendirilmemiş kolonlar şemasız ikinci denemeyle çözülür."""
    partial = manifest()
    partial["nodes"]["model.p.mart"] = model(
        "mart", 'select order_id, amount from "db"."analytics"."stg_orders"', ["model.p.stg_orders"])
    graph = dbt_graph(partial)
    edges = edge_set(graph)
    assert ("model.p.stg_orders", "amount", "model.p.mart", "amount") in edges
    assert ("model.p.stg_orders", "order_id", "model.p.mart", "order_id") in edges
    assert graph["column_stats"]["unresolved_columns"] == 0


def test_without_compiled_sql_there_is_no_column_lineage():
    plain = manifest()
    for node in plain["nodes"].values():
        node.pop("compiled_code")
    graph = dbt_graph(plain)
    assert graph["column_edges"] == [] and graph["column_stats"] is None and "no column lineage" in graph["coverage"]


def test_unparseable_sql_is_skipped_not_fatal():
    broken = manifest()
    broken["nodes"]["model.p.stg_orders"]["compiled_code"] = "select from from ((("
    graph = dbt_graph(broken)
    assert graph["column_stats"]["skipped_columns"] >= 1
    assert ("model.p.fx", "rate", "model.p.orders", "amount_try") in edge_set(graph)


def test_reachable_columns_follows_both_directions_and_is_bounded():
    edges = dbt_graph(manifest())["column_edges"]
    down = reachable_columns(edges, "source.p.raw.orders", "total_amount")
    assert down == [("model.p.stg_orders", "amount"), ("model.p.orders", "amount_try"), ("model.p.daily_revenue", "amount_try")]
    up = reachable_columns(edges, "model.p.daily_revenue", "AMOUNT_TRY", reverse=True)
    assert ("source.p.raw.orders", "total_amount") in up and ("source.p.raw.fx", "rate") in up
    assert reachable_columns(edges, "model.p.orders", "missing") == []
    assert len(reachable_columns(edges, "model.p.orders", "amount_try", reverse=True, limit=1)) == 1
    cyc = [["a", "x", "b", "x"], ["b", "x", "a", "x"]]
    assert reachable_columns(cyc, "a", "x") == [("b", "x")], "döngü sonsuza gitmez"


def test_openlineage_column_lineage_facet():
    event = {"eventType": "COMPLETE", "eventTime": "2026-10-07T10:00:00Z", "run": {"runId": "r"},
             "job": {"namespace": "airflow", "name": "load"},
             "inputs": [{"namespace": "pg", "name": "raw.orders"}],
             "outputs": [{"namespace": "pg", "name": "mart.orders",
                          "facets": {"columnLineage": {"fields": {"Total": {"inputFields": [
                              {"namespace": "pg", "name": "raw.orders", "field": "Amount"}]}}}}}]}
    graph = openlineage_graph(event)
    assert graph["column_edges"] == [["pg::raw.orders", "amount", "pg::mart.orders", "total"]]
    assert "columnLineage facet" in graph["coverage"]
    assert openlineage_graph({**event, "outputs": [{"namespace": "pg", "name": "mart.orders"}]})["column_edges"] == []
    bad = {**event, "outputs": [{"namespace": "pg", "name": "m", "facets": {"columnLineage": {"fields": {
        "c": {"inputFields": [{"namespace": "pg", "name": "x"}]}}}}}]}
    with pytest.raises(ValueError, match="field"):
        openlineage_graph(bad)


# --- analiz içinde ---------------------------------------------------------

@pytest.fixture
def engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    store.metadata.create_all(engine)
    yield engine
    engine.dispose()


class Fake:
    def __init__(self, frame):
        self.frame = frame

    def scan(self, config):
        return ScanResult(self.frame.copy(), {"complete": True, "total_rows": len(self.frame), "scanned_rows": len(self.frame),
                                              "method": "t", "duration_seconds": 0})


def test_signal_columns_are_mapped_to_upstream_and_downstream_columns(engine):
    svc.ingest(engine, "p", manifest(), "alice")
    config = {"connection_env": "SENTINEL_SOURCE_TEST", "schema": "analytics", "table": "orders", "lineage_asset": "model.p.orders",
              "allowlist": ["analytics.orders"], "order_by": ["order_id"],
              "contract": {"expected_schema": {"order_id": "integer", "amount_try": "float", "currency": "string"},
                           "rules": [{"type": "range", "column": "amount_try", "min": 0, "max": 1000}]}}
    src = svc.register_source(engine, "orders", config, "alice")
    rng = np.random.default_rng(0)
    frame = pd.DataFrame({"order_id": range(100), "amount_try": rng.uniform(10, 900, 100), "currency": "TRY"})

    def run(data, key):
        svc.enqueue(engine, src["id"], key, "alice")
        return svc.work_once(engine, Fake(data))

    assert run(frame, "good")["body"]["column_impact"] == []
    bad = run(frame.assign(amount_try=frame["amount_try"] * 100), "bad")
    [impact] = bad["body"]["column_impact"]
    assert impact["column"] == "amount_try"
    assert {"asset": "model.p.stg_orders", "column": "amount"} in impact["upstream"]
    assert {"asset": "source.p.raw.orders", "column": "total_amount"} in impact["upstream"]
    assert {"asset": "model.p.daily_revenue", "column": "amount_try"} in impact["downstream"]
    assert svc.replay(engine, bad["id"])["matches"]


def test_ingest_reports_column_edge_counts_and_openlineage_merge_keeps_history(engine):
    result = svc.ingest(engine, "p", manifest(), "alice")
    assert result["column_edges"] > 5 and result["column_stats"]["models"] >= 4
    event = {"eventType": "COMPLETE", "eventTime": "2026-10-07T10:00:00Z", "run": {"runId": "r1"}, "job": {"namespace": "a", "name": "j"},
             "inputs": [{"namespace": "pg", "name": "x"}],
             "outputs": [{"namespace": "pg", "name": "y", "facets": {"columnLineage": {"fields": {"c": {"inputFields": [
                 {"namespace": "pg", "name": "x", "field": "a"}]}}}}}]}
    assert svc.ingest(engine, "ol", event, "alice", kind="openlineage")["column_edges"] == 1
    second = {**event, "run": {"runId": "r2"}, "outputs": [{"namespace": "pg", "name": "z", "facets": {"columnLineage": {"fields": {
        "d": {"inputFields": [{"namespace": "pg", "name": "y", "field": "c"}]}}}}}]}
    assert svc.ingest(engine, "ol", second, "alice", kind="openlineage")["column_edges"] == 2
    with engine.connect() as conn:
        edges = [tuple(e) for g in store.rows(conn, store.graphs, store.graphs.c.namespace == "ol") for e in g["body"]["column_edges"]]
    assert reachable_columns(edges, "pg::x", "a") == [("pg::y", "c"), ("pg::z", "d")]
