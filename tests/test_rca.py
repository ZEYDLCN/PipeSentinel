from pipeline_sentinel.rca import MAX_HYPOTHESES, MIN_HYPOTHESIS_CONFIDENCE, generate_report


def _sig(id_, type_, column, severity, score, source="contract"):
    return {"id": id_, "type": type_, "column": column, "severity": severity, "score": score, "evidence": {}, "source": source}


def test_no_signals_yields_healthy_report():
    report = generate_report("orders", "run1", [])
    assert report.severity == "low"
    assert report.root_causes == []
    assert "sağlıklı" in report.summary


def test_schema_missing_column_produces_high_confidence_hypothesis():
    signals = [_sig("s1", "schema_missing_column", "customer_id", "critical", 0.95)]
    report = generate_report("orders", "run1", signals)
    assert len(report.root_causes) == 1
    h = report.root_causes[0]
    assert "customer_id" in h.hypothesis
    assert h.evidence_ids == ["s1"]
    assert h.confidence >= 0.9


def test_schema_diff_corroboration_boosts_confidence():
    signals = [_sig("s1", "schema_missing_column", "customer_id", "critical", 0.9)]
    without_diff = generate_report("orders", "run1", signals)
    with_diff = generate_report(
        "orders", "run1", signals, schema_diff={"dropped_columns": ["customer_id"], "added_columns": []}
    )
    assert with_diff.root_causes[0].confidence > without_diff.root_causes[0].confidence


def test_type_mismatch_merges_semantic_drift_evidence():
    signals = [
        _sig("s1", "schema_type_mismatch", "status", "critical", 0.9),
        _sig("s2", "semantic_drift", "status", "medium", 0.5),
    ]
    report = generate_report("orders", "run1", signals)
    h = report.root_causes[0]
    assert h.rule_id == "schema_type_change"
    assert set(h.evidence_ids) == {"s1", "s2"}


def test_range_and_distribution_combine_into_scale_error():
    signals = [
        _sig("s1", "range", "total_amount", "critical", 1.0, source="contract"),
        _sig("s2", "distribution", "total_amount", "critical", 1.0, source="baseline"),
    ]
    report = generate_report("orders", "run1", signals)
    h = report.root_causes[0]
    assert "birim/ölçek" in h.hypothesis
    assert set(h.evidence_ids) == {"s1", "s2"}
    assert h.confidence >= 0.99 - 1e-9 or h.confidence == 1.0


def test_distribution_alone_is_discounted_and_flagged_ambiguous():
    signals = [_sig("s1", "distribution", "total_amount", "high", 0.8, source="baseline")]
    report = generate_report("orders", "run1", signals)
    h = report.root_causes[0]
    assert h.confidence == 0.8 * 0.8
    assert "§8.3" in h.hypothesis


def test_completeness_hypothesis():
    signals = [_sig("s1", "completeness", "total_amount", "critical", 0.95)]
    report = generate_report("orders", "run1", signals)
    assert "eksik" in report.root_causes[0].hypothesis.lower() or "null" in report.summary.lower()


def test_freshness_hypothesis():
    signals = [_sig("s1", "freshness", "created_at", "high", 0.71)]
    report = generate_report("orders", "run1", signals)
    assert "SLA" in report.root_causes[0].hypothesis
    assert report.severity == "high"


def test_duplicate_load_takes_priority_over_per_column_rules():
    signals = [
        _sig("s1", "volume", None, "critical", 1.0, source="baseline"),
        _sig("s2", "uniqueness", "order_id", "critical", 0.9, source="contract"),
        _sig("s3", "cardinality_drift", "status", "medium", 0.6, source="baseline"),
    ]
    report = generate_report("orders", "run1", signals)
    top = report.root_causes[0]
    assert "duplicate" in top.hypothesis.lower() or "birden fazla" in top.hypothesis
    assert set(top.evidence_ids) == {"s1", "s2"}
    # cardinality_drift on a different column should not appear in the top hypothesis
    assert "s3" not in top.evidence_ids


def test_volume_alone_is_discounted():
    signals = [_sig("s1", "volume", None, "high", 0.7, source="baseline")]
    report = generate_report("orders", "run1", signals)
    assert report.root_causes[0].confidence == 0.7 * 0.7


def test_weak_or_overflow_hypotheses_go_to_uncertainties_not_silently_dropped():
    # 4 independent columns each with a distribution-only (discounted) signal:
    # 0.5*0.8 = 0.4 exactly at threshold -> all "strong" but only top 3 kept,
    # the 4th must appear in uncertainties, never vanish.
    signals = [_sig(f"s{i}", "distribution", f"col_{i}", "medium", 0.9, source="baseline") for i in range(4)]
    report = generate_report("orders", "run1", signals)
    assert len(report.root_causes) == MAX_HYPOTHESES
    assert len(report.uncertainties) == 1


def test_low_confidence_signal_alone_goes_to_uncertainties():
    signals = [_sig("s1", "cardinality_drift", "status", "medium", 0.5, source="baseline")]
    # 0.5 * 0.5 = 0.25 < MIN_HYPOTHESIS_CONFIDENCE
    report = generate_report("orders", "run1", signals)
    assert report.root_causes == []
    assert any("kardinalite" in u for u in report.uncertainties)


def test_every_root_cause_has_real_evidence_ids():
    signals = [
        _sig("real-1", "range", "total_amount", "critical", 1.0),
        _sig("real-2", "distribution", "total_amount", "critical", 1.0, source="baseline"),
        _sig("real-3", "freshness", "created_at", "high", 0.7),
    ]
    report = generate_report("orders", "run1", signals)
    known_ids = {s["id"] for s in signals}
    for h in report.root_causes:
        assert h.evidence_ids, "every hypothesis must cite at least one evidence id"
        assert set(h.evidence_ids) <= known_ids


def test_affected_assets_pass_through_unchanged():
    signals = [_sig("s1", "range", "total_amount", "critical", 1.0)]
    report = generate_report(
        "orders", "run1", signals, affected_assets=["payments.amount", "finance_dashboard"]
    )
    assert report.affected_assets == ["payments.amount", "finance_dashboard"]


def test_severity_reflects_worst_signal():
    signals = [
        _sig("s1", "cardinality_drift", "status", "low", 0.3),
        _sig("s2", "completeness", "total_amount", "high", 0.6),
    ]
    report = generate_report("orders", "run1", signals)
    assert report.severity == "high"
