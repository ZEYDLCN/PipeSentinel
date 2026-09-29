"""Data contract tanımları ve deterministik kontroller (§6.1 contracts, §7.1).

Her contract; beklenen şema (`expected_schema`) ve bir kural listesi
(`rules`) taşır. `check_contract` bu kuralları saf pandas üzerinde
uygular ve ihlalleri `Violation` olarak döner — LLM'e gitmeden önce
"kesin ve açıklanabilir" olan katman budur (§7.1).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from .synthetic import ORDER_STATUSES, PAYMENT_METHODS

# Rule şeması, temel data_type isimleriyle string tip eşlemesi yapmaz;
# pandas dtype.kind üzerinden karşılaştırılır (bkz. _dtype_matches).
_TYPE_KIND = {
    "integer": "iu",
    "float": "iuf",
    "string": "O",
    "boolean": "b",
    "datetime": "M",
}


@dataclass
class Violation:
    """Bir contract kuralının ihlali — deterministik, kanıtlanabilir."""

    rule_type: str
    column: str | None
    message: str
    evidence: dict[str, Any]
    severity: str = "high"  # schema/uniqueness ihlalleri varsayılan olarak yüksek


def _dtype_matches(series: pd.Series, expected: str) -> bool:
    kinds = _TYPE_KIND.get(expected, "")
    if expected == "string":
        return pd.api.types.is_string_dtype(series.dtype) or series.dtype.kind == "O"
    if expected == "datetime":
        return "datetime" in str(series.dtype) or series.dtype.kind == "M"
    return series.dtype.kind in kinds


def check_schema(df: pd.DataFrame, expected_schema: dict[str, str]) -> list[Violation]:
    violations: list[Violation] = []

    missing = [c for c in expected_schema if c not in df.columns]
    for col in missing:
        violations.append(
            Violation(
                rule_type="schema_missing_column",
                column=col,
                message=f"Beklenen kolon eksik: {col}",
                evidence={"expected_columns": list(expected_schema)},
            )
        )

    extra = [c for c in df.columns if c not in expected_schema]
    for col in extra:
        violations.append(
            Violation(
                rule_type="schema_unexpected_column",
                column=col,
                message=f"Beklenmeyen kolon: {col}",
                evidence={"observed_columns": list(df.columns)},
                severity="medium",
            )
        )

    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            continue
        if not _dtype_matches(df[col], expected_type):
            violations.append(
                Violation(
                    rule_type="schema_type_mismatch",
                    column=col,
                    message=(
                        f"{col} beklenen tip '{expected_type}' değil, "
                        f"gözlenen dtype '{df[col].dtype}'"
                    ),
                    evidence={"expected_type": expected_type, "observed_dtype": str(df[col].dtype)},
                )
            )
    return violations


def check_rules(df: pd.DataFrame, rules: list[dict[str, Any]]) -> list[Violation]:
    violations: list[Violation] = []
    row_count = len(df)

    for rule in rules:
        rtype = rule["type"]
        col = rule.get("column")

        if rtype == "not_null":
            if col not in df.columns:
                continue
            null_ratio = 0.0 if row_count == 0 else df[col].isna().mean()
            max_allowed = rule.get("max_null_ratio", 0.0)
            if null_ratio > max_allowed:
                violations.append(
                    Violation(
                        rule_type="completeness",
                        column=col,
                        message=(
                            f"{col} null oranı {null_ratio:.4f}, izin verilen "
                            f"üst sınır {max_allowed:.4f}"
                        ),
                        evidence={"null_ratio": round(float(null_ratio), 6), "max_allowed": max_allowed},
                    )
                )

        elif rtype == "range":
            if col not in df.columns:
                continue
            series = df[col].dropna()
            lo, hi = rule.get("min"), rule.get("max")
            out_of_range = series[(series < lo) | (series > hi)] if series.dtype.kind in "iuf" else series.iloc[0:0]
            if len(out_of_range) > 0:
                violations.append(
                    Violation(
                        rule_type="range",
                        column=col,
                        message=(
                            f"{col} için {len(out_of_range)} satır [{lo}, {hi}] "
                            f"aralığının dışında (ör. {out_of_range.iloc[0]})"
                        ),
                        evidence={
                            "min": lo,
                            "max": hi,
                            "violation_count": int(len(out_of_range)),
                            "violation_ratio": round(float(len(out_of_range) / row_count), 6) if row_count else 0.0,
                            "sample_value": float(out_of_range.iloc[0]),
                        },
                    )
                )

        elif rtype == "uniqueness":
            if col not in df.columns:
                continue
            non_null = df[col].dropna()
            dup_count = int(len(non_null) - non_null.nunique())
            if dup_count > 0:
                violations.append(
                    Violation(
                        rule_type="uniqueness",
                        column=col,
                        message=f"{col} kolonunda {dup_count} tekrarlanan değer var",
                        evidence={"duplicate_count": dup_count},
                    )
                )

        elif rtype == "freshness":
            if col not in df.columns or df[col].dropna().empty:
                continue
            max_lag = rule.get("max_lag_minutes", 30)
            ts = pd.to_datetime(df[col], utc=True, errors="coerce").dropna()
            if ts.empty:
                continue
            now = rule.get("as_of") or datetime.now(timezone.utc)
            lag_minutes = (now - ts.max()).total_seconds() / 60.0
            if lag_minutes > max_lag:
                violations.append(
                    Violation(
                        rule_type="freshness",
                        column=col,
                        message=(
                            f"{col} en güncel değeri {lag_minutes:.1f} dakika önce; "
                            f"SLA {max_lag} dakika"
                        ),
                        evidence={"lag_minutes": round(lag_minutes, 2), "max_lag_minutes": max_lag},
                    )
                )

        elif rtype == "allowed_values":
            if col not in df.columns:
                continue
            allowed = set(rule.get("values", []))
            observed = set(df[col].dropna().unique().tolist())
            unexpected = observed - allowed
            if unexpected:
                violations.append(
                    Violation(
                        rule_type="semantic_drift",
                        column=col,
                        message=f"{col} için beklenmeyen kategori değerleri: {sorted(unexpected)}",
                        evidence={"unexpected_values": sorted(unexpected), "allowed_values": sorted(allowed)},
                        severity="medium",
                    )
                )

        elif rtype == "referential_integrity":
            ref_values = set(rule.get("reference_values", []))
            if col not in df.columns:
                continue
            observed = df[col].dropna()
            orphans = observed[~observed.isin(ref_values)]
            if len(orphans) > 0:
                violations.append(
                    Violation(
                        rule_type="referential_integrity",
                        column=col,
                        message=f"{col} için {len(orphans)} tanımsız referans bulundu",
                        evidence={
                            "orphan_count": int(len(orphans)),
                            "sample": orphans.iloc[:5].tolist(),
                        },
                    )
                )

    return violations


def check_contract(df: pd.DataFrame, contract: dict[str, Any]) -> list[Violation]:
    violations = check_schema(df, contract.get("expected_schema", {}))
    violations += check_rules(df, contract.get("rules", []))
    return violations


# ---------------------------------------------------------------------------
# examples/commerce-pipeline için varsayılan contract'lar
# ---------------------------------------------------------------------------

CUSTOMERS_CONTRACT: dict[str, Any] = {
    "expected_schema": {
        "customer_id": "integer",
        "email": "string",
        "country": "string",
        "signup_at": "datetime",
        "is_active": "boolean",
    },
    "rules": [
        {"type": "not_null", "column": "customer_id", "max_null_ratio": 0.0},
        {"type": "not_null", "column": "email", "max_null_ratio": 0.0},
        {"type": "uniqueness", "column": "customer_id"},
    ],
}

ORDERS_CONTRACT: dict[str, Any] = {
    "expected_schema": {
        "order_id": "integer",
        "customer_id": "integer",
        "status": "string",
        "total_amount": "float",
        "currency": "string",
        "created_at": "datetime",
    },
    "rules": [
        {"type": "not_null", "column": "order_id", "max_null_ratio": 0.0},
        {"type": "not_null", "column": "total_amount", "max_null_ratio": 0.01},
        # Üst sınır §7.1'deki genel şablondan (0..1_000_000) değil, bu demo'nun
        # gerçek dağılımından kalibre edilmiştir (mean≈482, p99≈800 TRY);
        # eşikler dataset bazında ayrı kalibre edilmelidir (§7.3).
        {"type": "range", "column": "total_amount", "min": 0, "max": 50_000},
        {"type": "uniqueness", "column": "order_id"},
        {"type": "freshness", "column": "created_at", "max_lag_minutes": 90},
        {"type": "allowed_values", "column": "status", "values": ORDER_STATUSES},
    ],
}

PAYMENTS_CONTRACT: dict[str, Any] = {
    "expected_schema": {
        "payment_id": "integer",
        "order_id": "integer",
        "amount": "float",
        "method": "string",
        "paid_at": "datetime",
    },
    "rules": [
        {"type": "not_null", "column": "payment_id", "max_null_ratio": 0.0},
        {"type": "uniqueness", "column": "payment_id"},
        {"type": "range", "column": "amount", "min": 0, "max": 50_000},
        {"type": "allowed_values", "column": "method", "values": PAYMENT_METHODS},
    ],
}

DEFAULT_CONTRACTS: dict[str, dict[str, Any]] = {
    "customers": CUSTOMERS_CONTRACT,
    "orders": ORDERS_CONTRACT,
    "payments": PAYMENTS_CONTRACT,
}
