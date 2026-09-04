from pipeline_sentinel.lineage import Edge, downstream_impact, upstream_sources

# Basitleştirilmiş commerce graph'ı (§9):
#
#   customers --WRITES_TO--> [job: orders_etl] --WRITES_TO--> orders
#   customers --READS_FROM--> orders_etl  (orders_etl customers'ı okur)
#   orders --READS_FROM--> payments_etl
#   orders_etl --WRITES_TO--> orders
#   payments_etl --WRITES_TO--> payments
#   customers.customer_id --DERIVED_FROM--> orders.customer_id
#   orders.order_id --DERIVED_FROM--> payments.order_id
#   orders.total_amount --DERIVED_FROM--> payments.amount
#   orders.total_amount --FEEDS--> finance_dashboard


def _sample_edges() -> list[Edge]:
    return [
        Edge("customers_ds", "dataset", "orders_job", "job", "READS_FROM"),
        Edge("orders_job", "job", "orders_ds", "dataset", "WRITES_TO"),
        Edge("orders_ds", "dataset", "payments_job", "job", "READS_FROM"),
        Edge("payments_job", "job", "payments_ds", "dataset", "WRITES_TO"),
        Edge("customers.customer_id", "column", "orders.customer_id", "column", "DERIVED_FROM"),
        Edge("orders.order_id", "column", "payments.order_id", "column", "DERIVED_FROM"),
        Edge("orders.total_amount", "column", "payments.amount", "column", "DERIVED_FROM"),
        Edge("orders.total_amount", "column", "finance_dashboard", "external_asset", "FEEDS"),
    ]


def test_downstream_impact_reaches_dashboard():
    edges = _sample_edges()
    hits = downstream_impact(edges, "orders.total_amount")
    ids = {h.node_id for h in hits}
    assert "payments.amount" in ids
    assert "finance_dashboard" in ids


def test_downstream_impact_hop_distance_is_correct():
    edges = _sample_edges()
    hits = {h.node_id: h for h in downstream_impact(edges, "orders.total_amount")}
    assert hits["payments.amount"].hop == 1
    assert hits["finance_dashboard"].hop == 1


def test_downstream_impact_dataset_level_reaches_job():
    edges = _sample_edges()
    hits = {h.node_id: h for h in downstream_impact(edges, "customers_ds")}
    assert hits["orders_job"].node_type == "job"
    assert hits["orders_ds"].hop == 2


def test_upstream_sources_ordered_nearest_first():
    edges = _sample_edges()
    hits = upstream_sources(edges, "payments.amount")
    ids = [h.node_id for h in hits]
    assert ids[0] == "orders.total_amount"


def test_upstream_sources_from_job_reaches_dataset():
    edges = _sample_edges()
    hits = {h.node_id: h for h in upstream_sources(edges, "payments_job")}
    assert hits["orders_ds"].hop == 1
    assert hits["orders_job"].hop == 2


def test_no_edges_returns_empty():
    assert downstream_impact([], "anything") == []
    assert upstream_sources([], "anything") == []


def test_max_hops_limits_traversal():
    edges = _sample_edges()
    hits = downstream_impact(edges, "customers_ds", max_hops=1)
    ids = {h.node_id for h in hits}
    assert ids == {"orders_job"}


def test_cycle_does_not_infinite_loop():
    edges = [
        Edge("a", "dataset", "b", "dataset", "FEEDS"),
        Edge("b", "dataset", "a", "dataset", "FEEDS"),
    ]
    hits = downstream_impact(edges, "a", max_hops=10)
    assert {h.node_id for h in hits} == {"b"}
