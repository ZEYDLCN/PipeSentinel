"""Accepted history, seasonal cohorts, and segment-aware comparisons."""
from datetime import datetime
from statistics import median

import pandas as pd

from .detector import AnomalySignal, compare_column_profiles, compare_row_counts
from .profiler import profile_dataset


def cohort(timestamp: str) -> tuple[int, int]:
    dt = datetime.fromisoformat(timestamp)
    return dt.weekday(), dt.hour


def choose(history: list[dict], timestamp: str, seasonal=False, minimum=3) -> dict:
    eligible = [h for h in history if h.get("quality") == "accepted"]
    if seasonal:
        eligible = [h for h in eligible if cohort(h["created_at"]) == cohort(timestamp)]
    if not eligible:
        return {"state": "cold_start", "ids": [], "profile": None, "row_count": None, "segments": {}}
    if seasonal and len(eligible) < minimum:
        return {"state": "insufficient_history", "ids": [h["id"] for h in eligible], "profile": None, "row_count": None, "segments": {}}
    chosen = eligible[:12] if seasonal else eligible[:1]
    profiles = [h["body"]["profile"] for h in chosen]
    result = {col: dict(metrics) for col, metrics in profiles[0].items()}
    for col, metrics in result.items():
        for key, value in list(metrics.items()):
            if type(value) in (int, float):
                values = [p[col][key] for p in profiles if col in p and type(p[col].get(key)) in (int, float)]
                metrics[key] = median(values)
    return {"state": "ready", "ids": [h["id"] for h in chosen], "profile": result,
            "row_count": int(median(h["body"]["coverage"]["total_rows"] for h in chosen)),
            "segments": chosen[0]["body"].get("segments", {})}


def segment_profiles(df: pd.DataFrame, columns: list[str]) -> dict:
    if not columns or any(c not in df for c in columns):
        return {}
    import json
    groups = df.groupby(columns, dropna=False, observed=True)
    if groups.ngroups > 100:
        raise ValueError("100'den fazla segment: daha dar segment/partition seçin")
    result = {}
    for key, frame in groups:
        values = key if isinstance(key, tuple) else (key,)
        label = json.dumps([None if pd.isna(v) else str(v) for v in values], ensure_ascii=False)
        result[label] = {"row_count": len(frame), "profile": profile_dataset(frame.drop(columns=columns))}
    return result


def compare_segments(previous: dict, current: dict) -> list[AnomalySignal]:
    signals = []
    for label, old in previous.items():
        if label not in current:
            signals.append(AnomalySignal("segment_missing", None, "high", .8, {"segment": label}, "segment"))
            continue
        new = current[label]
        found = []
        volume = compare_row_counts(old["row_count"], new["row_count"])
        if volume:
            found.append(volume)
        for column, metrics in new["profile"].items():
            if column in old["profile"]:
                found.extend(compare_column_profiles(old["profile"][column], metrics, column))
        for signal in found:
            signal.source = "segment"
            signal.evidence["segment"] = label
        signals.extend(found)
    return signals
