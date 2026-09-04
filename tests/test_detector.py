from pipeline_sentinel.contracts import DEFAULT_CONTRACTS
from pipeline_sentinel.detector import (
    AnomalySignal,
    compare_column_profiles,
    compare_row_counts,
    compute_incident_risk,
    run_detection,
    severity_from_score,
)
from pipeline_sentinel.faults import apply_fault
from pipeline_sentinel.profiler import profile_dataset
from pipeline_sentinel.synthetic import generate_commerce_batch


def test_severity_thresholds():
    assert severity_from_score(0.95) == "critical"
    assert severity_from_score(0.7) == "high"
    assert severity_from_score(0.5) == "medium"
    assert severity_from_score(0.1) == "low"


def test_identifier_columns_do_not_trigger_distribution_signal():
    baseline = {"mean": 100.0, "stddev": 5.0}
    current = {"mean": 100_000.0, "stddev": 5.0}  # huge shift, but it's an id column
    signals = compare_column_profiles(baseline, current, column="order_id")
    assert signals == []


def test_non_identifier_distribution_shift_detected():
    baseline = {"mean": 480.0, "stddev": 130.0}
    current = {"mean": 48000.0, "stddev": 130.0}
    signals = compare_column_profiles(baseline, current, column="total_amount")
    assert any(s.type == "distribution" for s in signals)


def test_cardinality_drift_fires_on_numeric_column_with_real_collapse():
    """Regresyon: eskiden `top_values` (yalnızca string/kategorik kolonlarda
    dolar) kontrolü, numeric kolonları hep 'low cardinality' sayıp bu
    kontrolü tamamen atlatıyordu — gerçek bir kardinalite çöküşü (500
    distinct -> 10 distinct) numeric bir kolonda da artık yakalanmalı."""
    baseline = {"row_count": 1000, "distinct_ratio": 0.5}
    current = {"row_count": 1000, "distinct_ratio": 0.01}
    signals = compare_column_profiles(baseline, current, column="discount_tier")
    assert any(s.type == "cardinality_drift" for s in signals)


def test_cardinality_drift_guard_still_blocks_genuinely_low_cardinality_numeric_column():
    """Gerçekten az sayıda değeri olan (ör. 0/1/2 durum kodu) numeric bir
    kolonda oran gürültüsü sahte alarm üretmemeli."""
    baseline = {"row_count": 1000, "distinct_ratio": 0.002}  # ~2 distinct
    current = {"row_count": 1000, "distinct_ratio": 0.001}  # ~1 distinct
    signals = compare_column_profiles(baseline, current, column="priority_level")
    assert not any(s.type == "cardinality_drift" for s in signals)


def test_volume_change_below_threshold_is_ignored():
    assert compare_row_counts(1000, 1050, threshold=0.3) is None


def test_volume_change_above_threshold_detected():
    signal = compare_row_counts(1000, 2000, threshold=0.3)
    assert signal is not None
    assert signal.type == "volume"


def test_compute_incident_risk_empty():
    assert compute_incident_risk([]) == 0.0


def test_compute_incident_risk_contract_violation_dominates():
    signals = [AnomalySignal(type="schema_missing_column", column="x", severity="critical", score=0.95, source="contract")]
    risk = compute_incident_risk(signals)
    assert risk > 0.5


def test_run_detection_healthy_batch_has_no_high_severity_signal():
    batch = generate_commerce_batch(n_customers=60, n_orders=600, seed=11)
    baseline_profile = profile_dataset(batch.orders)
    # A second, independent healthy batch acts as "current"
    batch2 = generate_commerce_batch(n_customers=60, n_orders=600, seed=12)
    current_profile = profile_dataset(batch2.orders)

    report = run_detection(
        dataset="orders",
        df=batch2.orders,
        contract=DEFAULT_CONTRACTS["orders"],
        current_profile=current_profile,
        baseline_profile=baseline_profile,
        baseline_row_count=len(batch.orders),
    )
    assert report.severity in {"low", "medium"}


def test_run_detection_f05_flags_high_or_critical():
    batch = generate_commerce_batch(n_customers=60, n_orders=600, seed=13)
    frames = {"customers": batch.customers, "orders": batch.orders, "payments": batch.payments}
    mutated, _ = apply_fault(frames, "F05")

    baseline_profile = profile_dataset(batch.orders)
    current_profile = profile_dataset(mutated["orders"])

    report = run_detection(
        dataset="orders",
        df=mutated["orders"],
        contract=DEFAULT_CONTRACTS["orders"],
        current_profile=current_profile,
        baseline_profile=baseline_profile,
        baseline_row_count=len(batch.orders),
    )
    assert report.severity in {"high", "critical"}
    assert {s.type for s in report.signals} >= {"range", "distribution"}
