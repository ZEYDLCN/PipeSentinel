"""Anomali tespit motoru (§7): deterministik kontroller + istatistiksel
baseline karşılaştırması, tek bir `AnomalySignal` listesinde birleşir.

Bu modül LLM çağırmaz. Root Cause Agent (Faz 4) bu sinyalleri girdi olarak
kullanacak; anomaliyi burada, mümkün olduğunca deterministik biçimde
yakalamak tasarım ilkesidir (bkz. doküman başlığı "Tasarım ilkesi").
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .contracts import Violation, check_contract

# ---------------------------------------------------------------------------
# Skor / severity yardımcıları
# ---------------------------------------------------------------------------

SEVERITY_THRESHOLDS = (
    (0.85, "critical"),
    (0.65, "high"),
    (0.40, "medium"),
    (0.0, "low"),
)


def severity_from_score(score: float) -> str:
    for threshold, label in SEVERITY_THRESHOLDS:
        if score >= threshold:
            return label
    return "low"


@dataclass
class AnomalySignal:
    """Tekil anomali sinyali — `signals` tablosuna karşılık gelir (§6.1)."""

    type: str
    column: str | None
    severity: str
    score: float
    evidence: dict[str, Any] = field(default_factory=dict)
    source: str = "detector"  # "contract" | "baseline"

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "column": self.column,
            "severity": self.severity,
            "score": round(self.score, 4),
            "evidence": self.evidence,
            "source": self.source,
        }


# ---------------------------------------------------------------------------
# Deterministik ihlallerden sinyal üretimi (§7.1)
# ---------------------------------------------------------------------------

_VIOLATION_SCORE: dict[str, float] = {
    "schema_missing_column": 0.95,
    "schema_unexpected_column": 0.5,
    "schema_type_mismatch": 0.9,
    "uniqueness": 0.9,
    "referential_integrity": 0.85,
    "semantic_drift": 0.5,
}


def _score_for_violation(v: Violation) -> float:
    if v.rule_type in _VIOLATION_SCORE:
        return _VIOLATION_SCORE[v.rule_type]

    if v.rule_type == "completeness":
        null_ratio = v.evidence.get("null_ratio", 0.0)
        max_allowed = v.evidence.get("max_allowed", 0.0)
        excess = max(0.0, null_ratio - max_allowed)
        return min(1.0, 0.5 + excess * 2.0)

    if v.rule_type == "range":
        ratio = v.evidence.get("violation_ratio", 0.0)
        return min(1.0, 0.4 + ratio * 3.0)

    if v.rule_type == "freshness":
        lag = v.evidence.get("lag_minutes", 0.0)
        sla = v.evidence.get("max_lag_minutes", 1.0) or 1.0
        return min(1.0, 0.4 + (lag / sla - 1.0) * 0.3)

    return 0.6


def signals_from_violations(violations: list[Violation]) -> list[AnomalySignal]:
    signals = []
    for v in violations:
        score = _score_for_violation(v)
        signals.append(
            AnomalySignal(
                type=v.rule_type,
                column=v.column,
                severity=severity_from_score(score),
                score=score,
                evidence={"message": v.message, **v.evidence},
                source="contract",
            )
        )
    return signals


# ---------------------------------------------------------------------------
# Baseline karşılaştırması (§7.2 istatistiksel yöntemler — basitleştirilmiş
# robust z-score + oransal değişim; Faz 2'de EWMA/seasonal/change-point/
# PSI/Isolation Forest eklenir)
# ---------------------------------------------------------------------------


def _is_identifier_column(column: str) -> bool:
    """Sürekli artan surrogate id/pk kolonları (`order_id`, `customer_id`, ...)
    istatistiksel olarak anlamsızdır: her run'da ortalama doğal biçimde
    kayar. Bu kolonlarda yalnızca completeness (null oranı) karşılaştırılır;
    mean/stddev ve distinct_ratio karşılaştırması atlanır."""
    return column.lower().endswith("_id") or column.lower() == "id"


def compare_column_profiles(
    baseline: dict[str, Any],
    current: dict[str, Any],
    column: str,
    z_threshold: float = 3.0,
    null_delta_threshold: float = 0.05,
) -> list[AnomalySignal]:
    signals: list[AnomalySignal] = []
    is_identifier = _is_identifier_column(column)

    b_null = baseline.get("null_ratio", 0.0)
    c_null = current.get("null_ratio", 0.0)
    if c_null - b_null > null_delta_threshold:
        score = min(1.0, 0.5 + (c_null - b_null))
        signals.append(
            AnomalySignal(
                type="completeness",
                column=column,
                severity=severity_from_score(score),
                score=score,
                evidence={
                    "message": f"{column} null oranı baseline {b_null:.4f} -> {c_null:.4f}",
                    "baseline_null_ratio": b_null,
                    "current_null_ratio": c_null,
                },
                source="baseline",
            )
        )

    if not is_identifier and "mean" in baseline and "mean" in current and baseline.get("stddev", 0) > 1e-9:
        z = (current["mean"] - baseline["mean"]) / baseline["stddev"]
        if abs(z) >= z_threshold:
            score = min(1.0, (abs(z) - z_threshold) / z_threshold + 0.5)
            signals.append(
                AnomalySignal(
                    type="distribution",
                    column=column,
                    severity=severity_from_score(score),
                    score=score,
                    evidence={
                        "message": f"{column} ortalaması baseline'dan {z:.2f} std sapıyor",
                        "z_score": round(z, 3),
                        "baseline_mean": baseline["mean"],
                        "current_mean": current["mean"],
                        "baseline_stddev": baseline["stddev"],
                    },
                    source="baseline",
                )
            )

    # distinct_ratio karşılaştırması yalnızca gerçekten birden fazla değeri
    # olan kolonlarda anlamlıdır — ikili (boolean tarzı) kolonlarda oran,
    # tamamen örneklem büyüklüğünün gürültüsüdür ve sahte alarm üretir; bu
    # yüzden en az 4 farklı değer görülmüş olmalı. Bu, `distinct_ratio *
    # row_count`'tan tahmini distinct sayısına bakarak hesaplanır — yalnızca
    # `top_values`'a (profiler.py'de sadece string/kategorik kolonlarda
    # dolan bir alan) bakmak numeric/datetime kolonlarda bu kontrolü hep
    # atlatırdı.
    def _approx_distinct_count(profile: dict[str, Any]) -> float:
        return profile.get("distinct_ratio", 0.0) * profile.get("row_count", 0)

    is_low_cardinality_categorical = (
        _approx_distinct_count(baseline) <= 3 or _approx_distinct_count(current) <= 3
    )
    b_distinct = baseline.get("distinct_ratio")
    c_distinct = current.get("distinct_ratio")
    if (
        not is_identifier
        and not is_low_cardinality_categorical
        and b_distinct is not None
        and c_distinct is not None
        and b_distinct > 1e-9
    ):
        rel_change = (c_distinct - b_distinct) / b_distinct
        if abs(rel_change) >= 0.5:
            score = min(1.0, 0.4 + abs(rel_change) * 0.4)
            signals.append(
                AnomalySignal(
                    type="cardinality_drift",
                    column=column,
                    severity=severity_from_score(score),
                    score=score,
                    evidence={
                        "message": (
                            f"{column} distinct_ratio baseline {b_distinct:.4f} -> "
                            f"{c_distinct:.4f} (%{rel_change * 100:.1f})"
                        ),
                        "baseline_distinct_ratio": b_distinct,
                        "current_distinct_ratio": c_distinct,
                    },
                    source="baseline",
                )
            )

    return signals


def compare_row_counts(
    baseline_row_count: int, current_row_count: int, threshold: float = 0.3
) -> AnomalySignal | None:
    if baseline_row_count <= 0:
        return None
    rel_change = (current_row_count - baseline_row_count) / baseline_row_count
    if abs(rel_change) < threshold:
        return None
    score = min(1.0, abs(rel_change))
    return AnomalySignal(
        type="volume",
        column=None,
        severity=severity_from_score(score),
        score=score,
        evidence={
            "message": (
                f"Satır sayısı baseline {baseline_row_count} -> {current_row_count} "
                f"(%{rel_change * 100:.1f})"
            ),
            "baseline_row_count": baseline_row_count,
            "current_row_count": current_row_count,
            "relative_change": round(rel_change, 4),
        },
        source="baseline",
    )


# ---------------------------------------------------------------------------
# Birleşik risk skoru (§7.3) — Faz 1'de yalnızca contract + magnitude
# terimleri doludur; persistence/downstream_impact/model_score Faz 2-4'te
# (baseline geçmişi, lineage, Root Cause Agent) eklenir.
# ---------------------------------------------------------------------------


def compute_incident_risk(signals: list[AnomalySignal]) -> float:
    """Bir run'a ait sinyalleri tek bir 0-1 risk skoruna indirger.

    risk = 0.35*contract + 0.25*magnitude + 0.20*persistence
         + 0.10*downstream_impact + 0.10*model_score  (§7.3)

    Faz 1'de persistence/downstream_impact/model_score verisi yok; bu
    terimler 0 kabul edilir ve ağırlıkları contract+magnitude'e
    yeniden normalize edilir (0.35/0.60, 0.25/0.60).
    """
    if not signals:
        return 0.0

    has_contract_violation = any(s.source == "contract" for s in signals)
    magnitude = max(s.score for s in signals)

    contract_term = 1.0 if has_contract_violation else 0.0
    risk = (0.35 / 0.60) * contract_term + (0.25 / 0.60) * magnitude
    return round(min(1.0, risk), 4)


# ---------------------------------------------------------------------------
# Tek çağrı ile tam tespit akışı
# ---------------------------------------------------------------------------


@dataclass
class DetectionReport:
    dataset: str
    signals: list[AnomalySignal]
    risk_score: float
    severity: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "risk_score": self.risk_score,
            "severity": self.severity,
            "signal_count": len(self.signals),
            "signals": [s.to_dict() for s in self.signals],
        }


def run_detection(
    dataset: str,
    df,
    contract: dict[str, Any],
    current_profile: dict[str, Any],
    baseline_profile: dict[str, Any] | None = None,
    baseline_row_count: int | None = None,
) -> DetectionReport:
    violations = check_contract(df, contract)
    signals = signals_from_violations(violations)

    if baseline_profile:
        for column, current_col_profile in current_profile.items():
            baseline_col_profile = baseline_profile.get(column)
            if not baseline_col_profile:
                continue
            signals += compare_column_profiles(baseline_col_profile, current_col_profile, column)

        if baseline_row_count is not None:
            row_signal = compare_row_counts(baseline_row_count, len(df))
            if row_signal:
                signals.append(row_signal)

    risk = compute_incident_risk(signals)
    return DetectionReport(
        dataset=dataset,
        signals=signals,
        risk_score=risk,
        severity=severity_from_score(risk) if signals else "low",
    )
