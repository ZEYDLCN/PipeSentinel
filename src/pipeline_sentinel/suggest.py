"""Veriden taslak data contract çıkarımı (onboarding).

Bir DataFrame'in şemasından ve dağılımından, ``contract_io.validate_contract``
şemasına uyan bir başlangıç sözleşmesi üretir. Çıktı bir *taslaktır*:
kenar payları gözlenen örneğe göre seçilir, SLA gibi iş bilgisi gerektiren
kurallar (freshness) tahmin edilmez, yalnızca not olarak önerilir.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

import pandas as pd
import yaml

from .contract_io import validate_contract

MAX_CATEGORIES = 12
MIN_ROWS = 20  # bundan az gözlemden uniqueness/range/kategori çıkarımı yapılmaz
MIN_ROWS_FOR_CATEGORIES = 50
SPARSE_NULL_RATIO = 0.2  # bunun üzerinde boş olan kolona not_null önerilmez
_KEY_NAME = re.compile(r"(^id$|_id$|_key$|^uuid$|_uuid$|^pk$)", re.IGNORECASE)
_TIME_NAME = re.compile(r"(_at$|_time$|_date$|^timestamp$|^date$)", re.IGNORECASE)


@dataclass
class Suggestion:
    contract: dict
    notes: list[str] = field(default_factory=list)

    def to_yaml(self) -> str:
        header = ["# Taslak sözleşme — otomatik çıkarıldı, kaydetmeden önce gözden geçirin.",
                  "# Eşikler gözlenen örneğe göre seçildi; iş kurallarınızı yansıtmayabilir."]
        header += ["# NOT: " + note for note in self.notes]
        body = yaml.safe_dump(self.contract, sort_keys=False, allow_unicode=True)
        return "\n".join(header) + "\n" + body


def _type_of(series: pd.Series) -> str | None:
    kind = series.dtype.kind
    if kind == "b":
        return "boolean"
    if kind in "iu":
        return "integer"
    if kind == "f":
        return "float"
    if kind == "M":
        return "datetime"
    if kind == "O" or pd.api.types.is_string_dtype(series.dtype):
        return "string"
    return None


def _nice(value: float, up: bool) -> float | int:
    """Değeri iki anlamlı basamağa yukarı/aşağı yuvarlar (1234 → 1300)."""
    if value == 0 or not math.isfinite(value):
        return 0
    step = 10 ** (math.floor(math.log10(abs(value))) - 1)
    rounded = (math.ceil if up else math.floor)(value / step) * step
    return int(rounded) if float(rounded).is_integer() else round(rounded, 6)


def _range_bounds(series: pd.Series) -> tuple[float | int, float | int]:
    low, high = float(series.min()), float(series.max())
    margin = max((high - low) * 0.5, abs(high) * 0.1, 1.0 if high == low else 0.0)
    upper = _nice(high + margin, up=True)
    lower = 0 if low >= 0 else _nice(low - margin, up=False)
    return lower, upper


def frame_from_csv(source, max_rows: int = 100_000) -> pd.DataFrame:
    """CSV'yi okur; adı zaman kolonuna benzeyen ve tüm değerleri çözümlenen kolonları datetime yapar."""
    frame = pd.read_csv(source, nrows=max_rows + 1)
    if len(frame) > max_rows:
        raise ValueError(f"CSV en fazla {max_rows} satır olabilir")
    for name in frame.columns:
        if frame[name].dtype.kind == "O" and _TIME_NAME.search(str(name)):
            parsed = pd.to_datetime(frame[name], utc=True, errors="coerce")
            if parsed.notna().sum() == frame[name].notna().sum():
                frame[name] = parsed
    return frame


def suggest_contract(frame: pd.DataFrame) -> Suggestion:
    """``frame``'den taslak sözleşme çıkarır; sonuç her zaman geçerli bir sözleşmedir."""
    if frame.columns.has_duplicates:
        raise ValueError("Yinelenen kolon adları var; sözleşme çıkarılamaz")
    schema: dict[str, str] = {}
    rules: list[dict] = []
    notes: list[str] = []
    rows = len(frame)
    if rows == 0:
        raise ValueError("Boş veri kümesinden sözleşme çıkarılamaz")
    if rows < MIN_ROWS:
        notes.append(f"Yalnızca {rows} satır incelendi; yalnızca şema ve not_null önerildi.")

    for name in frame.columns:
        series = frame[name]
        kind = _type_of(series)
        if kind is None or not isinstance(name, str) or not name:
            notes.append(f"'{name}' kolonu desteklenmeyen tip ({series.dtype}) nedeniyle atlandı.")
            continue
        schema[name] = kind
        non_null = series.dropna()
        null_ratio = float(series.isna().mean())
        key_like = bool(_KEY_NAME.search(name))

        if null_ratio <= SPARSE_NULL_RATIO:
            limit = 0.0 if key_like and null_ratio == 0 else round(min(1.0, max(0.01, null_ratio * 2)), 4)
            rules.append({"type": "not_null", "column": name, "max_null_ratio": limit})
        else:
            notes.append(f"'{name}' kolonunun %{null_ratio * 100:.0f}'i boş; not_null önerilmedi.")

        if rows < MIN_ROWS or non_null.empty:
            continue
        unique = non_null.nunique() == len(non_null) and null_ratio == 0
        if key_like and unique and kind in {"integer", "string"}:
            rules.append({"type": "uniqueness", "column": name})
        elif unique and kind in {"integer", "string"} and not key_like:
            notes.append(f"'{name}' örnekte tekil; anahtar ise uniqueness kuralı ekleyin.")

        if kind in {"integer", "float"} and not key_like:
            lower, upper = _range_bounds(non_null.astype(float))
            rules.append({"type": "range", "column": name, "min": lower, "max": upper})
        elif kind == "string" and rows >= MIN_ROWS_FOR_CATEGORIES:
            values = non_null.unique().tolist()
            if (2 <= len(values) <= MAX_CATEGORIES and all(isinstance(v, str) for v in values)
                    and len(values) / len(non_null) <= 0.2):
                rules.append({"type": "allowed_values", "column": name, "values": sorted(values)})
        elif kind == "datetime" and _TIME_NAME.search(name):
            latest = pd.to_datetime(non_null, utc=True, errors="coerce").max()
            notes.append(f"'{name}' için SLA'nız varsa freshness kuralı ekleyin "
                         f"(örnekteki en güncel değer: {latest.isoformat() if pd.notna(latest) else '?'}).")

    if not schema:
        raise ValueError("Desteklenen hiçbir kolon bulunamadı")
    contract = validate_contract({"version": 1, "expected_schema": schema, "rules": rules})
    return Suggestion(contract=contract, notes=notes)
