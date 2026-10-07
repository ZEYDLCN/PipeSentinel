"""Geçmişten öğrenen anomali tespiti.

* ``history_anomalies``: kabul edilmiş analizlerin metrik geçmişinden (medyan + MAD) kaynağa özgü
  "olağan aralığı" öğrenir. Tek bir referans analizle kıyaslayan sabit eşiklerin (örn. %30 hacim)
  göremediği, kararlı metriklerdeki küçük ama sürekli sapmaları yakalar.
* ``multivariate_drift``: kolonların *birlikte* dağılımındaki değişimi arar; tek tek kolonlarda
  ortalama/aralık aynı kalırken aralarındaki ilişki bozulduğunda devreye girer. Mahalanobis
  mesafesi (bağımlılıksız) ilişki bozulmalarını, Isolation Forest (isteğe bağlı,
  ``pip install "pipeline-sentinel[ml]"``) küresel/çok kipli aykırılıkları yakalar.

İkisi de deterministiktir (sabit ``random_state``); sinyaller kanıt alanlarında yöntemi ve sayıları taşır.
"""
from __future__ import annotations

from statistics import median

import numpy as np
import pandas as pd

from .detector import AnomalySignal, severity_from_score

MIN_HISTORY = 8
MAX_HISTORY = 30
Z_THRESHOLD = 6.0


def _robust(values: list[float]) -> tuple[float, float]:
    center = median(values)
    return center, 1.4826 * median(abs(v - center) for v in values)


def _score(z: float) -> float:
    # 6 robust sigma zaten çok nadir bir sapmadır; RCA'nın hipotez eşiğini (volume için ×0,7) geçecek kadar yüksek başlar.
    return min(1.0, 0.58 + min(abs(z), 30.0) / 75.0)


def _is_identifier(name: str) -> bool:
    return name.lower().endswith("_id") or name.lower() == "id"


def history_anomalies(profile: dict, coverage: dict, history: list[dict], existing: set[tuple[str, str | None]],
                      cohort_of=None, timestamp: str | None = None) -> list[AnomalySignal]:
    """`history`: yeniden eskiye kabul edilmiş analizler. `existing`: zaten üretilmiş (tür, kolon) çiftleri."""
    items = history
    minimum = MIN_HISTORY
    if cohort_of is not None and timestamp is not None:  # mevsimsel kaynak: yalnızca aynı hafta günü/saat
        items, minimum = [h for h in history if cohort_of(h["created_at"]) == cohort_of(timestamp)], 5
    items = items[:MAX_HISTORY]
    if len(items) < minimum:
        return []
    signals: list[AnomalySignal] = []

    def series(getter):
        values = []
        for item in items:
            try:
                value = getter(item["body"])
            except (KeyError, TypeError):
                continue
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                values.append(float(value))
        return values

    def check(kind: str, column: str | None, current, values: list[float], practical, label: str, one_sided_up=False):
        if (kind, column) in existing or len(values) < minimum or not isinstance(current, (int, float)):
            return
        center, sigma = _robust(values)
        sigma = max(sigma, 0.02 * abs(center), 1e-9)
        z = (float(current) - center) / sigma
        if abs(z) < Z_THRESHOLD or (one_sided_up and z < 0) or not practical(float(current), center):
            return
        score = _score(z)
        signals.append(AnomalySignal(
            kind, column, severity_from_score(score), score,
            {"message": f"{label}: {current:.6g}; geçmiş {len(values)} analizin olağan değeri {center:.6g} "
                        f"(robust z={z:.1f})",
             "method": "history", "z_score": round(max(-1e6, min(1e6, z)), 3), "typical": round(center, 6),
             "history_runs": len(values), "current": float(current)},
            "baseline"))

    relative = lambda current, center: abs(current - center) / max(abs(center), 1e-9) >= 0.05
    check("volume", None, coverage.get("total_rows"), series(lambda b: b["coverage"]["total_rows"]), relative, "Satır sayısı")
    for column, metrics in profile.items():
        if "mean" in metrics and not _is_identifier(column):
            check("distribution", column, metrics["mean"], series(lambda b, c=column: b["profile"][c]["mean"]), relative,
                  f"{column} ortalaması")
        if "null_ratio" in metrics:
            check("completeness", column, metrics["null_ratio"], series(lambda b, c=column: b["profile"][c]["null_ratio"]),
                  lambda current, center: current - center >= 0.02, f"{column} boş oranı", one_sided_up=True)
    return signals


def _mahalanobis_flags(ref: pd.DataFrame, cur: pd.DataFrame, contamination: float) -> np.ndarray:
    """Kolonlar arası *doğrusal ilişkinin* bozulmasını yakalar (örn. ücret ≈ 0,1 × tutar artık geçerli değil).
    Eşik, referansın kendi satırlarının mesafe dağılımından (1 − contamination) niceliği olarak seçilir."""
    mean = ref.mean().to_numpy()
    scale = (ref.quantile(0.75) - ref.quantile(0.25)).replace(0, np.nan).fillna(ref.std()).to_numpy()
    scale = np.where(scale > 0, scale, 1.0)
    a, b = (ref.to_numpy() - mean) / scale, (cur.to_numpy() - mean) / scale
    covariance = np.cov(a, rowvar=False)
    covariance = covariance + 1e-3 * np.trace(covariance) / covariance.shape[0] * np.eye(covariance.shape[0])
    inverse = np.linalg.pinv(covariance)
    distance = lambda x: np.einsum("ij,jk,ik->i", x, inverse, x)
    return distance(b) > np.quantile(distance(a), 1 - contamination)


def _isolation_forest_flags(ref: pd.DataFrame, cur: pd.DataFrame, contamination: float) -> np.ndarray | None:
    """Küresel/çok kipli aykırılıklar (yeni bir küme, uç kombinasyonlar). scikit-learn yoksa None."""
    try:
        from sklearn.ensemble import IsolationForest
    except ImportError:
        return None
    model = IsolationForest(n_estimators=100, contamination=contamination, random_state=0).fit(ref)
    return model.score_samples(cur) < float(np.quantile(model.score_samples(ref), contamination))


def multivariate_drift(reference: pd.DataFrame, current: pd.DataFrame, *, contamination: float = 0.01,
                       min_rows: int = 200, sample: int = 20000, max_columns: int = 10) -> AnomalySignal | None:
    """Referans verinin ortak dağılımına göre güncel satırların alışılmadık olma oranını ölçer.

    İki dedektör çalışır: Mahalanobis (ilişki bozulması; bağımlılıksız) ve Isolation Forest (küresel
    aykırılıklar; yalnızca scikit-learn kuruluysa). Oran, referansın kendi `contamination` oranının
    anlamlı biçimde (≥ 3×, p < 1e-6) üstündeyse sinyal üretilir; kanıt her iki dedektörün sonucunu taşır.
    """
    from scipy.stats import binomtest

    columns = [c for c in reference.columns if c in current.columns and not _is_identifier(str(c))
               and pd.api.types.is_numeric_dtype(reference[c]) and pd.api.types.is_numeric_dtype(current[c])
               and not pd.api.types.is_bool_dtype(reference[c]) and reference[c].nunique(dropna=True) > 1]
    columns = columns[:max_columns]
    if len(columns) < 2:
        return None
    ref, cur = reference[columns].dropna(), current[columns].dropna()
    if len(ref) < min_rows or len(cur) < min_rows:
        return None
    ref = ref.sample(n=min(len(ref), sample), random_state=0)
    cur = cur.sample(n=min(len(cur), sample), random_state=0)
    detectors: dict[str, dict | None] = {}
    for name, flags in (("mahalanobis", _mahalanobis_flags(ref, cur, contamination)),
                        ("isolation_forest", _isolation_forest_flags(ref, cur, contamination))):
        if flags is None:
            detectors[name] = None
            continue
        flagged = int(flags.sum())
        detectors[name] = {"flagged_rows": flagged, "rate": round(flagged / len(cur), 4),
                           "p_value": float(binomtest(flagged, len(cur), contamination, alternative="greater").pvalue)}
    firing = {name: d for name, d in detectors.items()
              if d and d["rate"] >= max(0.03, 3 * contamination) and d["p_value"] < 1e-6}
    if not firing:
        return None
    method, best = max(firing.items(), key=lambda item: item[1]["rate"])
    iqr = (ref.quantile(0.75) - ref.quantile(0.25)).replace(0, np.nan)
    shifts = ((cur.median() - ref.median()) / iqr).abs().fillna(0).sort_values(ascending=False)
    score = min(1.0, 0.5 + best["rate"])
    return AnomalySignal(
        "multivariate_drift", None, severity_from_score(score), score,
        {"message": f"Kolonların ortak dağılımı değişti: satırların %{best['rate'] * 100:.1f}'i referansta alışılmadık "
                    f"(beklenen %{contamination * 100:.1f}; yöntem {method})",
         "method": method, "outlier_rate": best["rate"], "expected_rate": contamination,
         "rows": int(len(cur)), "flagged_rows": best["flagged_rows"], "columns": columns, "p_value": best["p_value"],
         "detectors": detectors, "median_shift_iqr": {c: round(float(v), 3) for c, v in shifts.head(3).items()}},
        "baseline")
