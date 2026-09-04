import pytest

from pipeline_sentinel.faults import FAULT_CATALOG, apply_fault
from pipeline_sentinel.synthetic import generate_commerce_batch


@pytest.fixture
def frames():
    batch = generate_commerce_batch(n_customers=20, n_orders=100, seed=3)
    return {"customers": batch.customers, "orders": batch.orders, "payments": batch.payments}


def test_all_catalog_entries_are_applicable(frames):
    for fault_id in FAULT_CATALOG:
        mutated, result = apply_fault(frames, fault_id)
        assert result.fault_id == fault_id
        assert result.dataset in mutated


def test_f01_drops_column(frames):
    mutated, result = apply_fault(frames, "F01")
    assert "customer_id" not in mutated["orders"].columns
    # unrelated datasets untouched
    assert mutated["customers"].equals(frames["customers"])
    assert mutated["payments"].equals(frames["payments"])


def test_f03_injects_expected_null_ratio(frames):
    mutated, result = apply_fault(frames, "F03", target_null_ratio=0.4)
    null_ratio = mutated["orders"]["total_amount"].isna().mean()
    assert 0.3 < null_ratio < 0.5


def test_f04_doubles_row_count(frames):
    mutated, result = apply_fault(frames, "F04")
    assert len(mutated["orders"]) == 2 * len(frames["orders"])


def test_f05_scales_amount(frames):
    mutated, result = apply_fault(frames, "F05", factor=100.0)
    ratio = (mutated["orders"]["total_amount"] / frames["orders"]["total_amount"]).round(4)
    assert (ratio == 100.0).all()


def test_unknown_fault_raises(frames):
    with pytest.raises(ValueError):
        apply_fault(frames, "F99")
