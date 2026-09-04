from pipeline_sentinel.synthetic import generate_commerce_batch


def test_generate_commerce_batch_shapes():
    batch = generate_commerce_batch(n_customers=30, n_orders=100, seed=1)
    assert len(batch.customers) == 30
    assert len(batch.orders) == 100
    # payments has a small missing_rate, so it should be close to but <= orders
    assert 0 < len(batch.payments) <= 100


def test_generate_commerce_batch_is_deterministic():
    a = generate_commerce_batch(n_customers=10, n_orders=20, seed=42)
    b = generate_commerce_batch(n_customers=10, n_orders=20, seed=42)
    assert a.orders["total_amount"].tolist() == b.orders["total_amount"].tolist()
    assert a.customers["email"].tolist() == b.customers["email"].tolist()


def test_orders_reference_known_customers():
    batch = generate_commerce_batch(n_customers=15, n_orders=50, seed=5)
    customer_ids = set(batch.customers["customer_id"])
    assert set(batch.orders["customer_id"]).issubset(customer_ids)


def test_payments_amount_matches_order_amount():
    batch = generate_commerce_batch(n_customers=10, n_orders=30, seed=9)
    orders_by_id = batch.orders.set_index("order_id")["total_amount"]
    for _, payment in batch.payments.iterrows():
        assert payment["amount"] == orders_by_id.loc[payment["order_id"]]
