"""F01-F06 için deterministik regression grader'ı (§16, §20 "Evaluation").

DB gerektirmez: sentetik bir "kontrol" (fault'suz) batch'i baseline kabul
eder, aynı seed ile üretilmiş bir batch'e fault uygular, tespit motorunu
(Faz 1-2) VE Root Cause Agent'ı (Faz 4, `rca.py`) çalıştırır, ve
`evals/incidents/*.json` içindeki beklentilerle karşılaştırır:

- `expected_signal_types`: tespit motorunun üretmesi gereken sinyal tipleri
- `expected_rule_id`: RCA'nın Top-1 hipotezinin `rule_id`'si (§16.2 "Root
  cause Top-1 accuracy")
- `min_confidence`: Top-1 hipotezin en az bu güvene ulaşması gerekir

Ayrıca her hipotezin `evidence_ids`'inin gerçek (bu run'da üretilen)
sinyal id'lerinin bir alt kümesi olduğu doğrulanır (§16.2 "Evidence
precision" / §11.3 kaynak zorunluluğu — burada bir policy check olarak).

CI'da (`pytest tests/test_evals.py`) ve elle (`python -m
evals.graders.grader`) çalıştırılabilir.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pipeline_sentinel.contracts import DEFAULT_CONTRACTS
from pipeline_sentinel.detector import run_detection
from pipeline_sentinel.faults import apply_fault
from pipeline_sentinel.profiler import profile_dataset
from pipeline_sentinel.rca import generate_report
from pipeline_sentinel.synthetic import generate_commerce_batch

INCIDENTS_DIR = Path(__file__).resolve().parents[1] / "incidents"


@dataclass
class EvalResult:
    fault_id: str
    passed: bool
    expected_types: set[str]
    observed_types: set[str]
    missing_types: set[str]
    risk_score: float
    severity: str
    expected_rule_id: str | None
    top_rule_id: str | None
    top_confidence: float
    min_confidence: float
    rule_match: bool
    confidence_ok: bool
    evidence_valid: bool
    failures: list[str] = field(default_factory=list)


def load_incident_cases() -> list[dict[str, Any]]:
    return [json.loads(p.read_text()) for p in sorted(INCIDENTS_DIR.glob("*.json"))]


def evaluate_fault(
    fault_id: str,
    expected_signal_types: list[str],
    expected_rule_id: str | None = None,
    min_confidence: float = 0.0,
    n_customers: int = 60,
    n_orders: int = 600,
    seed: int = 7,
) -> EvalResult:
    control = generate_commerce_batch(n_customers=n_customers, n_orders=n_orders, seed=seed)
    frames = {
        "customers": control.customers,
        "orders": control.orders,
        "payments": control.payments,
    }

    mutated_frames, fault_result = apply_fault(frames, fault_id)
    dataset = fault_result.dataset

    baseline_df = frames[dataset]
    current_df = mutated_frames[dataset]

    baseline_profile = profile_dataset(baseline_df)
    current_profile = profile_dataset(current_df)

    detection = run_detection(
        dataset=dataset,
        df=current_df,
        contract=DEFAULT_CONTRACTS[dataset],
        current_profile=current_profile,
        baseline_profile=baseline_profile,
        baseline_row_count=len(baseline_df),
    )

    observed_types = {s.type for s in detection.signals}
    expected_types = set(expected_signal_types)
    missing = expected_types - observed_types

    # rca.py DB'siz saf sözlükler bekler; eval bağlamında sentetik id atanır
    # (gerçek çalışmada bu id'ler `signals` tablosundan gelir — bkz.
    # pipeline.py::get_signals_for_run).
    signal_dicts = [
        {
            "id": f"eval-{i}",
            "type": s.type,
            "column": s.column,
            "severity": s.severity,
            "score": s.score,
            "evidence": s.evidence,
            "source": s.source,
        }
        for i, s in enumerate(detection.signals)
    ]
    known_ids = {s["id"] for s in signal_dicts}

    rca_report = generate_report(dataset=dataset, run_id="eval-run", signals=signal_dicts)

    top = rca_report.root_causes[0] if rca_report.root_causes else None
    top_rule_id = top.rule_id if top else None
    top_confidence = top.confidence if top else 0.0

    rule_match = expected_rule_id is None or top_rule_id == expected_rule_id
    confidence_ok = top_confidence >= min_confidence
    evidence_valid = all(set(h.evidence_ids) <= known_ids and h.evidence_ids for h in rca_report.root_causes)

    failures = []
    if missing:
        failures.append(f"eksik sinyal tipleri: {sorted(missing)}")
    if not rule_match:
        failures.append(f"beklenen rule_id={expected_rule_id!r}, gözlenen={top_rule_id!r}")
    if not confidence_ok:
        failures.append(f"güven {top_confidence:.2f} < min {min_confidence:.2f}")
    if not evidence_valid:
        failures.append("bir hipotez boş veya bilinmeyen evidence_ids içeriyor (§11.3 ihlali)")

    return EvalResult(
        fault_id=fault_id,
        passed=not failures,
        expected_types=expected_types,
        observed_types=observed_types,
        missing_types=missing,
        risk_score=detection.risk_score,
        severity=detection.severity,
        expected_rule_id=expected_rule_id,
        top_rule_id=top_rule_id,
        top_confidence=top_confidence,
        min_confidence=min_confidence,
        rule_match=rule_match,
        confidence_ok=confidence_ok,
        evidence_valid=evidence_valid,
        failures=failures,
    )


def run_all() -> list[EvalResult]:
    results = []
    for case in load_incident_cases():
        results.append(
            evaluate_fault(
                fault_id=case["fault_id"],
                expected_signal_types=case["expected_signal_types"],
                expected_rule_id=case.get("expected_rule_id"),
                min_confidence=case.get("min_confidence", 0.0),
            )
        )
    return results


def compute_metrics(results: list[EvalResult]) -> dict[str, float]:
    """§16.2 Ölçüm metriklerinin bu eval seti üzerindeki basitleştirilmiş
    karşılıkları (gerçek precision/recall için daha büyük bir altın veri
    seti gerekir; bu sadece 6 fault senaryosu üzerinden bir özet verir)."""
    n = len(results)
    if n == 0:
        return {}
    detection_recall = sum(1 for r in results if not r.missing_types) / n
    top1_accuracy = sum(1 for r in results if r.rule_match) / n
    with_rule = [r for r in results if r.expected_rule_id is not None and r.rule_match]
    mean_top1_confidence = sum(r.top_confidence for r in with_rule) / len(with_rule) if with_rule else 0.0
    evidence_precision = sum(1 for r in results if r.evidence_valid) / n
    return {
        "detection_recall": detection_recall,
        "root_cause_top1_accuracy": top1_accuracy,
        "mean_top1_confidence_when_correct": mean_top1_confidence,
        "evidence_precision": evidence_precision,
    }


def main() -> int:
    results = run_all()
    exit_code = 0
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {r.fault_id}  risk={r.risk_score:.2f} severity={r.severity}")
        print(f"         sinyal: expected={sorted(r.expected_types)} observed={sorted(r.observed_types)}")
        print(
            f"         RCA: top_rule={r.top_rule_id!r} (beklenen={r.expected_rule_id!r})  "
            f"confidence={r.top_confidence:.2f} (min={r.min_confidence:.2f})"
        )
        if not r.passed:
            for f in r.failures:
                print(f"         ✗ {f}")
            exit_code = 1

    metrics = compute_metrics(results)
    print(f"\n{sum(r.passed for r in results)}/{len(results)} fault senaryosu geçti.")
    print("Metrikler (§16.2):")
    for k, v in metrics.items():
        print(f"  {k}: {v:.2%}" if "confidence" not in k else f"  {k}: {v:.2f}")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
