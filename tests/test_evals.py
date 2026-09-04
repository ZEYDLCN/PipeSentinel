"""evals/incidents içindeki F01-F06 altın vakalarını pytest üzerinden çalıştırır.

Her vaka hem tespit motorunun (Faz 1-2) beklenen sinyal tiplerini hem de
Root Cause Agent'ın (Faz 4) Top-1 hipotezinin doğru `rule_id`/güven eşiğine
ulaştığını doğrular (§16.2, §16.3)."""

import pytest

from evals.graders.grader import compute_metrics, evaluate_fault, load_incident_cases

CASES = load_incident_cases()


@pytest.mark.parametrize("case", CASES, ids=[c["fault_id"] for c in CASES])
def test_fault_case_matches_expected_signals_and_root_cause(case):
    result = evaluate_fault(
        fault_id=case["fault_id"],
        expected_signal_types=case["expected_signal_types"],
        expected_rule_id=case.get("expected_rule_id"),
        min_confidence=case.get("min_confidence", 0.0),
    )
    assert result.passed, (
        f"{case['fault_id']} eval başarısız: {result.failures} "
        f"(observed_signals={sorted(result.observed_types)}, top_rule={result.top_rule_id!r}, "
        f"confidence={result.top_confidence:.2f})"
    )


def test_eval_set_achieves_perfect_top1_accuracy_on_golden_set():
    """Bu golden set üzerinde regresyon: Top-1 doğruluk %100 olmalı —
    düşerse (yeni bir kural, ağırlık değişikliği vb.) CI kırmızı olur."""
    results = [
        evaluate_fault(
            fault_id=case["fault_id"],
            expected_signal_types=case["expected_signal_types"],
            expected_rule_id=case.get("expected_rule_id"),
            min_confidence=case.get("min_confidence", 0.0),
        )
        for case in CASES
    ]
    metrics = compute_metrics(results)
    assert metrics["root_cause_top1_accuracy"] == 1.0
    assert metrics["detection_recall"] == 1.0
    assert metrics["evidence_precision"] == 1.0
