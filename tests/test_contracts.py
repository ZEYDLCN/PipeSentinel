from datetime import datetime, timedelta, timezone

import pandas as pd

from pipeline_sentinel.contracts import DEFAULT_CONTRACTS, check_contract, check_rules, check_schema


def _healthy_orders_df():
    now = datetime.now(timezone.utc)
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [10, 11, 12],
            "status": ["placed", "paid", "delivered"],
            "total_amount": [100.0, 250.5, 90.0],
            "currency": ["TRY", "TRY", "TRY"],
            "created_at": pd.to_datetime([now - timedelta(minutes=m) for m in (10, 5, 1)]),
        }
    )


def test_healthy_orders_has_no_violations():
    df = _healthy_orders_df()
    violations = check_contract(df, DEFAULT_CONTRACTS["orders"])
    assert violations == []


def test_missing_column_detected():
    df = _healthy_orders_df().drop(columns=["customer_id"])
    violations = check_schema(df, DEFAULT_CONTRACTS["orders"]["expected_schema"])
    types = {v.rule_type for v in violations}
    assert "schema_missing_column" in types


def test_range_violation_detected():
    df = _healthy_orders_df()
    df.loc[0, "total_amount"] = 999_999.0
    violations = check_rules(df, DEFAULT_CONTRACTS["orders"]["rules"])
    assert any(v.rule_type == "range" for v in violations)


def test_uniqueness_violation_detected():
    df = pd.concat([_healthy_orders_df(), _healthy_orders_df().iloc[[0]]], ignore_index=True)
    violations = check_rules(df, DEFAULT_CONTRACTS["orders"]["rules"])
    assert any(v.rule_type == "uniqueness" for v in violations)


def test_allowed_values_violation_detected():
    df = _healthy_orders_df()
    df.loc[0, "status"] = "teleported"
    violations = check_rules(df, DEFAULT_CONTRACTS["orders"]["rules"])
    assert any(v.rule_type == "semantic_drift" for v in violations)
