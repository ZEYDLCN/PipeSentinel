"""Bounded repair templates on saved data copies; no SQL or source credentials."""
from __future__ import annotations

import io
import json
import multiprocessing
from datetime import datetime

import pandas as pd

from .contract_io import at_time, digest
from .contracts import check_contract
from .profiler import profile_dataset


def snapshot(df):
    return {"split": json.loads(df.to_json(orient="split", date_format="iso", double_precision=15)),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}}


def restore(data):
    frame = pd.read_json(io.StringIO(json.dumps(data["split"])), orient="split", dtype=False)
    for column, dtype in data["dtypes"].items():
        if "datetime" in dtype:
            frame[column] = pd.to_datetime(frame[column], utc="UTC" in dtype)
        else:
            frame[column] = frame[column].astype(dtype)
    return frame


def data_diff(before, after, keys: list[str], max_change_ratio=0.0):
    if not keys or any(k not in before or k not in after for k in keys):
        raise ValueError("Her iki tabloda bulunan anahtar kolonlar gerekli")
    if before[keys].isna().any().any() or after[keys].isna().any().any() or before.duplicated(keys).any() or after.duplicated(keys).any():
        raise ValueError("Karşılaştırma anahtarları tekil ve dolu olmalı")
    if not 0 <= max_change_ratio <= 1:
        raise ValueError("max_change_ratio 0–1 arasında olmalı")
    left, right = before.set_index(keys), after.set_index(keys)
    common = left.index.intersection(right.index)
    columns = left.columns.intersection(right.columns)
    l, r = left.loc[common, columns], right.loc[common, columns]
    equal = l.eq(r) | (l.isna() & r.isna())
    changed = int((~equal.all(axis=1)).sum())
    added, removed = len(right.index.difference(left.index)), len(left.index.difference(right.index))
    ratio = (changed + added + removed) / max(len(left.index.union(right.index)), 1)
    schema_changed = list(before.columns) != list(after.columns) or any(str(before[c].dtype) != str(after[c].dtype) for c in columns.union(pd.Index(keys)))
    return {"added_rows": added, "removed_rows": removed, "changed_rows": changed,
            "changed_cells": int((~equal).sum().sum()), "change_ratio": ratio,
            "schema_changed": schema_changed, "passed": ratio <= max_change_ratio and not schema_changed,
            "numeric_totals": {c: {"before": float(before[c].sum()), "after": float(after[c].sum())}
                               for c in columns if pd.api.types.is_numeric_dtype(before[c]) and pd.api.types.is_numeric_dtype(after[c])}}


def evaluate(data: dict, contract: dict, action: dict, observed_at: str) -> dict:
    before = restore(data)
    if len(before) > 100000 or len(before.columns) > 500:
        raise ValueError("Sandbox veri sınırı aşıldı")
    if set(action) - {"type", "column", "factor", "where", "keys", "max_changed_rows"}:
        raise ValueError("Bilinmeyen aksiyon alanı")
    maximum = action.get("max_changed_rows", 1000)
    if type(maximum) is not int or not 1 <= maximum <= 100000:
        raise ValueError("max_changed_rows 1–100000 olmalı")
    after = before.copy(deep=True)
    if action.get("type") == "scale":
        col, factor, where = action.get("column"), action.get("factor"), action.get("where")
        if col not in before or not pd.api.types.is_numeric_dtype(before[col]):
            raise ValueError("Sayısal hedef kolon gerekli")
        if factor not in (.01, .1, 10, 100):
            raise ValueError("Yalnızca 0.01, 0.1, 10, 100 ölçekleri desteklenir")
        if not isinstance(where, dict) or not where or any(c not in before for c in where):
            raise ValueError("Etkilenen batch'i seçen where eşitlik filtresi gerekli")
        mask = pd.Series(True, index=before.index)
        for c, value in where.items():
            if type(value) not in (str, int, float, bool):
                raise ValueError("Filtre değerleri skaler olmalı")
            mask &= before[c].eq(value)
        affected = int(mask.sum())
        if affected > maximum:
            raise ValueError("Değiştirilecek satır sınırı aşıldı")
        after[col] = after[col].astype(float)
        after.loc[mask, col] = before.loc[mask, col] * factor
        untouched = before.loc[~mask].equals(after.loc[~mask].astype(before.dtypes.to_dict()))
    elif action.get("type") == "deduplicate":
        keys = action.get("keys")
        if not keys or any(k not in before for k in keys):
            raise ValueError("Tekilleştirme anahtarları gerekli")
        # Ambiguous duplicates are not silently discarded.
        exact = before.drop_duplicates()
        if exact.duplicated(keys).any():
            raise ValueError("Aynı anahtarda farklı kayıtlar var; otomatik tekilleştirme belirsiz")
        after = exact.copy()
        affected = len(before) - len(after)
        untouched = True
    else:
        raise ValueError("Desteklenen şablonlar: scale, deduplicate")
    if not 0 < affected <= maximum:
        raise ValueError("Aksiyon etkisiz veya satır sınırını aşıyor")
    frozen = at_time(contract, datetime.fromisoformat(observed_at))
    old, new = check_contract(before, frozen), check_contract(after, frozen)
    return {"passed": bool(old) and not new and untouched,
            "changed_rows": affected, "before_rows": len(before), "after_rows": len(after),
            "before_violations": [{"type": v.rule_type, "column": v.column} for v in old],
            "after_violations": [{"type": v.rule_type, "column": v.column} for v in new],
            "unchanged_control_passed": untouched, "before_profile": profile_dataset(before),
            "after_profile": profile_dataset(after), "snapshot_hash": digest(data),
            "contract_hash": digest(contract), "action_hash": digest(action),
            "coverage": "saved snapshot and its contract only; downstream not executed",
            "production_executed": False}


def _child(sender, args):
    try:
        sender.send((True, evaluate(*args)))
    except Exception as exc:
        sender.send((False, str(exc) if isinstance(exc, ValueError) else "Sandbox doğrulaması başarısız"))
    finally:
        sender.close()


def sandbox(data, contract, action, observed_at, timeout=15):
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    process = ctx.Process(target=_child, args=(sender, (data, contract, action, observed_at)))
    process.start()
    sender.close()
    try:
        if not receiver.poll(timeout):
            raise ValueError("Sandbox zaman aşımı")
        ok, result = receiver.recv()
        if not ok:
            raise ValueError(result)
        return result
    finally:
        if process.is_alive():
            process.terminate()
        process.join(3)
        receiver.close()
