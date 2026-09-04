"""Kolon/veri kümesi profilleme (§6.2, §7.1 Deterministik kontroller altyapısı).

Girdi bir pandas DataFrame'dir; çıktı, ``profiles.metrics`` alanına
doğrudan yazılabilecek JSON-uyumlu bir sözlüktür. Veritabanı bağımlılığı
yoktur — CSV, Postgres ya da bellek içi DataFrame ile aynı şekilde çalışır.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

NUMERIC_KINDS = "iuf"  # int, unsigned int, float


def _sample_hash(series: pd.Series, sample_size: int = 200) -> str:
    sample = series.dropna().astype(str).sort_values().head(sample_size)
    digest = hashlib.sha256("|".join(sample.tolist()).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def profile_column(series: pd.Series, row_count: int | None = None) -> dict[str, Any]:
    """Tek bir kolon için ColumnProfile üretir (§6.2 formatı)."""
    row_count = row_count if row_count is not None else len(series)
    non_null = series.dropna()
    null_ratio = 0.0 if row_count == 0 else round(1 - len(non_null) / row_count, 6)
    distinct_ratio = 0.0 if row_count == 0 else round(non_null.nunique() / row_count, 6)

    profile: dict[str, Any] = {
        "row_count": int(row_count),
        "null_ratio": null_ratio,
        "distinct_ratio": distinct_ratio,
        "sample_hash": _sample_hash(series),
    }

    if non_null.empty:
        return profile

    if series.dtype.kind in NUMERIC_KINDS:
        numeric = non_null.astype(float)
        profile.update(
            {
                "mean": round(float(numeric.mean()), 6),
                "stddev": round(float(numeric.std(ddof=0)), 6),
                "min": round(float(numeric.min()), 6),
                "max": round(float(numeric.max()), 6),
                "p50": round(float(numeric.quantile(0.50)), 6),
                "p95": round(float(numeric.quantile(0.95)), 6),
                "p99": round(float(numeric.quantile(0.99)), 6),
            }
        )
    elif pd.api.types.is_datetime64_any_dtype(series):
        ts = pd.to_datetime(non_null, utc=True, errors="coerce").dropna()
        if not ts.empty:
            profile.update(
                {
                    "min": ts.min().isoformat(),
                    "max": ts.max().isoformat(),
                }
            )
    else:
        top = non_null.astype(str).value_counts().head(5)
        profile["top_values"] = [
            {"value": v, "count": int(c)} for v, c in top.items()
        ]

    return profile


def profile_dataset(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """Her kolon için ColumnProfile üretir; anahtar kolon adıdır."""
    row_count = len(df)
    return {col: profile_column(df[col], row_count=row_count) for col in df.columns}


@dataclass
class DatasetProfile:
    """profile_dataset çıktısını sarmalayan, run metadata'sı taşıyan yardımcı."""

    dataset: str
    run_id: str
    columns: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, dataset: str, run_id: str) -> "DatasetProfile":
        return cls(dataset=dataset, run_id=run_id, columns=profile_dataset(df))

    def to_dict(self) -> dict[str, Any]:
        return {"dataset": self.dataset, "run_id": self.run_id, "columns": self.columns}
