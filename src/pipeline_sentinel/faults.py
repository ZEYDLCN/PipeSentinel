"""Kontrollü hata enjeksiyon kataloğu F01-F06 (§16.1).

Her fonksiyon saf bir DataFrame dönüşümüdür: girdi olarak
``examples/commerce-pipeline`` batch'inin bir tablosunu alır, mutasyona
uğratılmış DataFrame'i ve kanıt/metadata sözlüğünü döner. Bu modül hiçbir
veritabanı işlemi yapmaz — CLI (`sentinel inject-fault`) ve eval seti
(`evals/incidents`) bu fonksiyonları çağırır.

F07 (yeni normal kategori / drift) ve F08 (prompt injection) bilinçli
olarak burada değil: F07 insan onayı gerektiren bir ürün akışıdır (§8.3),
F08 ise Root Cause Agent güvenlik testidir (Faz 4, §14).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd


@dataclass
class FaultResult:
    fault_id: str
    dataset: str
    description: str
    metadata: dict[str, Any]


def f01_drop_column(df: pd.DataFrame, column: str = "customer_id") -> tuple[pd.DataFrame, dict[str, Any]]:
    """F01 — Kolon silme → schema contract alarmı beklenir."""
    mutated = df.drop(columns=[column])
    return mutated, {"column": column, "description": f"Kolon silindi: {column}"}


def f02_type_change(df: pd.DataFrame, column: str = "status") -> tuple[pd.DataFrame, dict[str, Any]]:
    """F02 — string → integer tip değişimi → breaking change alarmı beklenir."""
    mutated = df.copy()
    categories = sorted(mutated[column].dropna().unique().tolist())
    code_map = {value: i for i, value in enumerate(categories)}
    mutated[column] = mutated[column].map(code_map).astype("Int64")
    return mutated, {
        "column": column,
        "description": f"{column} string kategoriden integer koda çevrildi",
        "code_map": code_map,
    }


def f03_null_spike(
    df: pd.DataFrame,
    column: str = "total_amount",
    target_null_ratio: float = 0.40,
    seed: int = 42,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """F03 — Null oranını ~%2'den ~%40'a çıkarma → completeness alarmı beklenir."""
    rng = np.random.default_rng(seed)
    mutated = df.copy()
    n = len(mutated)
    k = int(n * target_null_ratio)
    idx = rng.choice(mutated.index, size=k, replace=False)
    mutated.loc[idx, column] = None
    return mutated, {
        "column": column,
        "target_null_ratio": target_null_ratio,
        "injected_null_count": int(k),
        "description": f"{column} için {k} satır null'a çevrildi",
    }


def f04_duplicate_rows(
    df: pd.DataFrame, fraction: float = 1.0, seed: int = 42
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """F04 — Kayıtları iki kez yükleme → volume + uniqueness alarmı beklenir."""
    dup = df.sample(frac=fraction, random_state=seed) if fraction < 1.0 else df.copy()
    mutated = pd.concat([df, dup], ignore_index=True)
    return mutated, {
        "duplicated_rows": int(len(dup)),
        "description": f"{len(dup)} satır tekrar yüklendi (duplicate load)",
    }


def f05_scale_currency(
    df: pd.DataFrame, column: str = "total_amount", factor: float = 100.0
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """F05 — TL → kuruş ölçek hatası → distribution + range alarmı beklenir."""
    mutated = df.copy()
    mutated[column] = mutated[column] * factor
    return mutated, {
        "column": column,
        "factor": factor,
        "description": f"{column} {factor}x ölçeklendi (TL → kuruş)",
    }


def f06_freshness_delay(
    df: pd.DataFrame, column: str = "created_at", hours: float = 3.0
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """F06 — Üç saat veri gecikmesi → freshness/SLA alarmı beklenir."""
    mutated = df.copy()
    mutated[column] = pd.to_datetime(mutated[column], utc=True) - pd.Timedelta(hours=hours)
    return mutated, {
        "column": column,
        "delay_hours": hours,
        "description": f"{column} {hours} saat geriye kaydırıldı",
    }


# fault_id -> (hedef dataset, dönüşüm fonksiyonu)
FaultFn = Callable[..., tuple[pd.DataFrame, dict[str, Any]]]

FAULT_CATALOG: dict[str, dict[str, Any]] = {
    "F01": {"dataset": "orders", "fn": f01_drop_column, "label": "Kolon silme"},
    "F02": {"dataset": "orders", "fn": f02_type_change, "label": "String → integer tip değişimi"},
    "F03": {"dataset": "orders", "fn": f03_null_spike, "label": "Null oranı %2 → %40"},
    "F04": {"dataset": "orders", "fn": f04_duplicate_rows, "label": "Kayıtları iki kez yükleme"},
    "F05": {"dataset": "orders", "fn": f05_scale_currency, "label": "TL → kuruş ölçek hatası"},
    "F06": {"dataset": "orders", "fn": f06_freshness_delay, "label": "Üç saat veri gecikmesi"},
}


def apply_fault(
    frames: dict[str, pd.DataFrame], fault_id: str, **overrides: Any
) -> tuple[dict[str, pd.DataFrame], FaultResult]:
    """Bir hata senaryosunu `frames` (dataset adı -> DataFrame) üzerine uygular.

    Yalnızca hedef dataset mutasyona uğrar; diğer tablolar değişmeden döner.
    """
    if fault_id not in FAULT_CATALOG:
        raise ValueError(f"Bilinmeyen fault_id: {fault_id}. Geçerli: {sorted(FAULT_CATALOG)}")

    entry = FAULT_CATALOG[fault_id]
    dataset = entry["dataset"]
    if dataset not in frames:
        raise ValueError(f"{fault_id} için hedef dataset '{dataset}' frames içinde yok")

    mutated_df, metadata = entry["fn"](frames[dataset], **overrides)
    result = FaultResult(
        fault_id=fault_id,
        dataset=dataset,
        description=f"{fault_id} — {entry['label']}",
        metadata=metadata,
    )

    new_frames = dict(frames)
    new_frames[dataset] = mutated_df
    return new_frames, result
