"""Root Cause Agent — kural tabanlı hipotez motoru (§10.3, §12).

DB bilmez: girdisi `pipeline.get_incident_context()`'in ürettiği kanıt
paketidir (signals + schema_diff), çıktısı §12.1'deki structured output
sözleşmesine uyan bir `IncidentReport`'tur.

Tasarım ilkesi (dokümanın başlığı): anomaliyi deterministik yöntemler
yakalar (Faz 1-2); burası da öyledir — hipotezler LLM'siz, saf kural
eşlemesiyle üretilir. `llm.py` yalnızca bu hipotezleri (mevcutsa) doğal
dile çevirir/rafine eder, yeni kanıt icat etmez (§12.2).

Her hipotezin `evidence_ids`'i gerçek `signals` tablosu satırlarına işaret
eder (§11.3 "kaynak zorunluluğu"). Kanıtı MIN_HYPOTHESIS_CONFIDENCE eşiğini
geçemeyen veya ilk üçe (§12.2) giremeyen adaylar `uncertainties`'e düşer —
asla sessizce atılmaz.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

MAX_HYPOTHESES = 3
MIN_HYPOTHESIS_CONFIDENCE = 0.4


@dataclass
class Hypothesis:
    hypothesis: str
    confidence: float
    evidence_ids: list[str]
    counter_evidence: list[str] = field(default_factory=list)
    rule_id: str = ""
    column: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "hypothesis": self.hypothesis,
            "confidence": round(self.confidence, 4),
            "evidence_ids": self.evidence_ids,
            "counter_evidence": self.counter_evidence,
        }


@dataclass
class IncidentReport:
    dataset: str
    run_id: str
    summary: str
    severity: str
    root_causes: list[Hypothesis]
    affected_assets: list[str]
    recommended_actions: list[dict[str, Any]]
    uncertainties: list[str]
    llm_used: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "run_id": self.run_id,
            "summary": self.summary,
            "severity": self.severity,
            "root_causes": [h.to_dict() for h in self.root_causes],
            "affected_assets": self.affected_assets,
            "recommended_actions": self.recommended_actions,
            "uncertainties": self.uncertainties,
            "llm_used": self.llm_used,
        }


# ---------------------------------------------------------------------------
# rule_id -> taslak aksiyon şablonları (§10.1 generate_test/propose_patch,
# §14.3 "İnsan onayı" — yalnızca taslak, hiçbiri otomatik uygulanmaz)
# ---------------------------------------------------------------------------


def _actions_for_rule(rule_id: str, column: str | None) -> list[dict[str, Any]]:
    col = column or "ilgili kolon"
    templates: dict[str, list[dict[str, Any]]] = {
        "schema_drop": [
            {"type": "dbt_test", "description": f"{col} için kolon varlığı/schema testi ekle", "requires_approval": False},
        ],
        "schema_type_change": [
            {"type": "dbt_test", "description": f"{col} için veri tipi kontrolü testi ekle", "requires_approval": False},
            {"type": "sql_patch", "description": f"Upstream kaynak şemasını incele; {col} tip dönüşümünü değerlendir", "requires_approval": True},
        ],
        "scale_error": [
            {"type": "sql_patch", "description": f"{col} için ölçek düzeltme SQL'i (örn. değeri 100'e böl)", "requires_approval": True},
            {"type": "dbt_test", "description": f"{col} için range testi ekle (§7.1)", "requires_approval": False},
        ],
        "completeness_gap": [
            {"type": "dbt_test", "description": f"{col} için not_null testi ekle", "requires_approval": False},
        ],
        "duplicate_load": [
            {"type": "sql_patch", "description": "Duplicate kayıtları temizleyen dedup SQL'i", "requires_approval": True},
            {"type": "dbt_test", "description": "uniqueness testi ekle", "requires_approval": False},
        ],
        "freshness_sla_breach": [
            {"type": "alert", "description": f"{col} için SLA izleme eşiğini gözden geçir", "requires_approval": False},
        ],
        "unexplained_distribution_shift": [
            {"type": "dbt_test", "description": "İş sahibi onayıyla baseline'ı güncelle (§8.2) ya da sapmayı doğrula", "requires_approval": True},
        ],
        "volume_shift": [
            {"type": "dbt_test", "description": "Volume/seasonal baseline eşiğini gözden geçir", "requires_approval": False},
        ],
        "semantic_drift": [
            {"type": "dbt_test", "description": f"{col} için allowed_values kontratını güncelle ya da iş sahibinden onay al", "requires_approval": True},
        ],
        "cardinality_drift": [
            {"type": "dbt_test", "description": f"{col} kardinalite değişimini izlemeye al (bilgilendirici)", "requires_approval": False},
        ],
        "joint_distribution_shift": [
            {"type": "dbt_test", "description": "Birbiriyle ilişkili kolonlar için ilişki/oran testi ekle; son dönüşüm veya join değişikliğini incele", "requires_approval": False},
        ],
        "stale_data": [
            {"type": "alert", "description": f"{col} için yükleme işinin son çalışmasını ve kaynak sistem durumunu kontrol et", "requires_approval": False},
        ],
        "row_count_bound": [
            {"type": "alert", "description": "Yükleme işinin kaynağını ve filtrelerini kontrol et; sözleşmedeki satır sınırını gözden geçir", "requires_approval": False},
        ],
        "column_relation_broken": [
            {"type": "dbt_test", "description": f"{col} için kolonlar arası ilişki testi ekle/doğrula", "requires_approval": False},
        ],
    }
    return templates.get(rule_id, [])


# ---------------------------------------------------------------------------
# Kural motoru
# ---------------------------------------------------------------------------


def _group_by_column(signals: list[dict[str, Any]]) -> dict[str | None, list[dict[str, Any]]]:
    groups: dict[str | None, list[dict[str, Any]]] = {}
    for s in signals:
        groups.setdefault(s["column"], []).append(s)
    return groups


def _find(signals: list[dict[str, Any]], type_: str):
    for s in signals:
        if s["type"] == type_:
            return s
    return None


def _column_hypotheses(
    column: str | None, signals: list[dict[str, Any]], schema_diff: dict[str, list[str]]
) -> list[Hypothesis]:
    hyps: list[Hypothesis] = []
    consumed: set[str] = set()

    schema_missing = _find(signals, "schema_missing_column")
    if schema_missing:
        confidence = schema_missing["score"]
        if column in schema_diff.get("dropped_columns", []):
            confidence = min(0.99, confidence + 0.03)
        hyps.append(
            Hypothesis(
                hypothesis=f"'{column}' kolonu upstream kaynakta kaldırılmış olabilir (breaking schema change).",
                confidence=confidence,
                evidence_ids=[schema_missing["id"]],
                rule_id="schema_drop",
                column=column,
            )
        )
        consumed.add(schema_missing["id"])

    type_mismatch = _find(signals, "schema_type_mismatch")
    if type_mismatch:
        evidence_ids = [type_mismatch["id"]]
        semantic = _find(signals, "semantic_drift")
        text_extra = ""
        if semantic and semantic["id"] not in consumed:
            evidence_ids.append(semantic["id"])
            consumed.add(semantic["id"])
            text_extra = " Beklenmeyen kategori/kod değerleri de gözlemlendi."
        hyps.append(
            Hypothesis(
                hypothesis=(
                    f"'{column}' kolonunun veri tipi upstream'de değişmiş olabilir "
                    f"(breaking type change).{text_extra}"
                ),
                confidence=type_mismatch["score"],
                evidence_ids=evidence_ids,
                rule_id="schema_type_change",
                column=column,
            )
        )
        consumed.add(type_mismatch["id"])

    range_sig = _find(signals, "range")
    dist_sig = _find(signals, "distribution")
    if range_sig and dist_sig:
        confidence = min(0.99, max(range_sig["score"], dist_sig["score"]) + 0.05)
        hyps.append(
            Hypothesis(
                hypothesis=(
                    f"'{column}' için olası birim/ölçek hatası (örn. TL → kuruş dönüşümü) — "
                    f"değerler beklenen aralığın çok üzerinde ve baseline'dan anlamlı sapıyor."
                ),
                confidence=confidence,
                evidence_ids=[range_sig["id"], dist_sig["id"]],
                rule_id="scale_error",
                column=column,
            )
        )
        consumed.update([range_sig["id"], dist_sig["id"]])
    elif dist_sig and dist_sig["id"] not in consumed:
        hyps.append(
            Hypothesis(
                hypothesis=(
                    f"'{column}' ortalaması baseline'dan anlamlı şekilde sapıyor; gerçek iş "
                    f"davranışı değişikliği veya veri üretim hatası olabilir (§8.3 — insan "
                    f"doğrulaması önerilir)."
                ),
                confidence=dist_sig["score"] * 0.8,
                evidence_ids=[dist_sig["id"]],
                rule_id="unexplained_distribution_shift",
                column=column,
            )
        )
        consumed.add(dist_sig["id"])
    elif range_sig and range_sig["id"] not in consumed:
        hyps.append(
            Hypothesis(
                hypothesis=f"'{column}' için beklenen aralık dışı değerler gözlemlendi.",
                confidence=range_sig["score"],
                evidence_ids=[range_sig["id"]],
                rule_id="scale_error",
                column=column,
            )
        )
        consumed.add(range_sig["id"])

    completeness = _find(signals, "completeness")
    if completeness and completeness["id"] not in consumed:
        hyps.append(
            Hypothesis(
                hypothesis=(
                    f"'{column}' için upstream kaynakta veri toplama/dönüşüm adımı eksik "
                    f"değer üretmeye başlamış olabilir (null oranı sıçraması)."
                ),
                confidence=completeness["score"],
                evidence_ids=[completeness["id"]],
                rule_id="completeness_gap",
                column=column,
            )
        )
        consumed.add(completeness["id"])

    freshness = _find(signals, "freshness")
    if freshness:
        hyps.append(
            Hypothesis(
                hypothesis=(
                    f"'{column}' SLA'yı aştı; upstream job gecikmiş, scheduler/queue sorunu "
                    f"ya da kaynak sistemde kesinti olabilir."
                ),
                confidence=freshness["score"],
                evidence_ids=[freshness["id"]],
                rule_id="freshness_sla_breach",
                column=column,
            )
        )
        consumed.add(freshness["id"])

    stale = _find(signals, "staleness")
    if stale:
        evidence = stale["evidence"]
        hyps.append(
            Hypothesis(
                hypothesis=(
                    f"'{column}' verisi öğrenilen olağan düzenden geç geliyor "
                    f"({evidence.get('lag_minutes', '?')} dk; olağan {evidence.get('typical_lag_minutes', '?')} dk): "
                    f"upstream yükleme durmuş, takılmış ya da kaynak sistemde kesinti olabilir."
                ),
                confidence=stale["score"],
                evidence_ids=[stale["id"]],
                rule_id="stale_data",
                column=column,
            )
        )
        consumed.add(stale["id"])

    semantic = _find(signals, "semantic_drift")
    if semantic and semantic["id"] not in consumed:
        hyps.append(
            Hypothesis(
                hypothesis=(
                    f"'{column}' içinde beklenmeyen yeni kategori değerleri görülüyor; "
                    f"upstream iş mantığı değişmiş olabilir (§8.3 — insan doğrulaması önerilir)."
                ),
                confidence=semantic["score"] * 0.75,
                evidence_ids=[semantic["id"]],
                rule_id="semantic_drift",
                column=column,
            )
        )
        consumed.add(semantic["id"])

    relation = _find(signals, "column_compare")
    if relation:
        other = relation["evidence"].get("other", "diğer kolon")
        hyps.append(
            Hypothesis(
                hypothesis=(
                    f"'{column}' ile '{other}' arasındaki sözleşme ilişkisi bozuldu "
                    f"({relation['evidence'].get('violation_count', '?')} satır): kolonlardan biri "
                    f"yanlış doldurulmuş, birimleri/saat dilimleri farklılaşmış ya da sıra karışmış olabilir."
                ),
                confidence=relation["score"],
                evidence_ids=[relation["id"]],
                rule_id="column_relation_broken",
                column=column,
            )
        )
        consumed.add(relation["id"])

    cardinality = _find(signals, "cardinality_drift")
    if cardinality and cardinality["id"] not in consumed:
        hyps.append(
            Hypothesis(
                hypothesis=f"'{column}' için kardinalite (distinct oranı) baseline'dan sapıyor.",
                confidence=cardinality["score"] * 0.5,
                evidence_ids=[cardinality["id"]],
                rule_id="cardinality_drift",
                column=column,
            )
        )
        consumed.add(cardinality["id"])

    return hyps


def _dataset_level_hypotheses(signals_by_column: dict[str | None, list[dict[str, Any]]]) -> list[Hypothesis]:
    """`volume` sinyali dataset seviyesindedir (column=None); `uniqueness`
    genelde bir PK kolonundadır. İkisi birlikteyse duplicate-load hipotezi
    diğer tüm per-column kurallardan önceliklidir."""
    bound = _find(signals_by_column.get(None, []), "row_count")
    bound_hypotheses = [
        Hypothesis(
            hypothesis=(
                "Satır sayısı sözleşmedeki sınırın dışında: kaynak eksik/boş yüklenmiş, "
                "filtre değişmiş ya da hacim beklenmedik biçimde değişmiş olabilir."
            ),
            confidence=bound["score"],
            evidence_ids=[bound["id"]],
            rule_id="row_count_bound",
            column=None,
        )
    ] if bound else []

    joint = _find(signals_by_column.get(None, []), "multivariate_drift")
    if joint:
        bound_hypotheses.append(
            Hypothesis(
                hypothesis=(
                    f"Kolonların birlikte dağılımı baseline'dan saptı (satırların %{joint['evidence'].get('outlier_rate', 0) * 100:.1f}'i "
                    f"alışılmadık); tek tek kolonlar normal görünse bile aralarındaki ilişkiyi değiştiren bir join, "
                    f"dönüşüm ya da kaynak değişikliği olabilir (insan doğrulaması önerilir)."
                ),
                confidence=joint["score"] * 0.8,
                evidence_ids=[joint["id"]],
                rule_id="joint_distribution_shift",
                column=None,
            )
        )

    volume = _find(signals_by_column.get(None, []), "volume")
    if not volume:
        return bound_hypotheses

    uniqueness = None
    for col, sigs in signals_by_column.items():
        if col is None:
            continue
        hit = _find(sigs, "uniqueness")
        if hit:
            uniqueness = hit
            break

    if uniqueness:
        confidence = min(0.99, (volume["score"] + uniqueness["score"]) / 2 + 0.1)
        return bound_hypotheses + [
            Hypothesis(
                hypothesis=(
                    "Kayıtlar muhtemelen birden fazla kez yüklendi (duplicate load / "
                    "idempotency hatası) — satır sayısı arttı ve tekillik ihlali var."
                ),
                confidence=confidence,
                evidence_ids=[volume["id"], uniqueness["id"]],
                rule_id="duplicate_load",
                column=None,
            )
        ]

    return bound_hypotheses + [
        Hypothesis(
            hypothesis=(
                "Kaynak veri hacminde beklenmeyen bir değişiklik var (upstream veri kaybı/"
                "kaynak kesintisi ya da iş hacminde gerçek bir değişim olabilir; §8.3 — "
                "insan doğrulaması önerilir)."
            ),
            confidence=volume["score"] * 0.7,
            evidence_ids=[volume["id"]],
            rule_id="volume_shift",
            column=None,
        )
    ]


def generate_hypotheses(
    signals: list[dict[str, Any]], schema_diff: dict[str, list[str]] | None = None
) -> list[Hypothesis]:
    """§10.3 hipotez puanlama — tamamen kural tabanlı, LLM'siz."""
    schema_diff = schema_diff or {"dropped_columns": [], "added_columns": []}
    by_column = _group_by_column(signals)

    candidates: list[Hypothesis] = _dataset_level_hypotheses(by_column)
    consumed_dataset_level = {eid for h in candidates for eid in h.evidence_ids}

    for column, col_signals in by_column.items():
        if column is None:
            continue
        remaining = [s for s in col_signals if s["id"] not in consumed_dataset_level]
        candidates.extend(_column_hypotheses(column, remaining, schema_diff))

    candidates.sort(key=lambda h: h.confidence, reverse=True)
    return candidates


def generate_report(
    dataset: str,
    run_id: str,
    signals: list[dict[str, Any]],
    schema_diff: dict[str, list[str]] | None = None,
    affected_assets: list[str] | None = None,
) -> IncidentReport:
    """Kanıt paketinden §12.1'e uyan `IncidentReport` üretir."""
    if not signals:
        return IncidentReport(
            dataset=dataset,
            run_id=run_id,
            summary="Sinyal tespit edilmedi; pipeline sağlıklı görünüyor.",
            severity="low",
            root_causes=[],
            affected_assets=[],
            recommended_actions=[],
            uncertainties=[],
        )

    candidates = generate_hypotheses(signals, schema_diff)

    strong = [h for h in candidates if h.confidence >= MIN_HYPOTHESIS_CONFIDENCE][:MAX_HYPOTHESES]
    strong_ids = {id(h) for h in strong}
    weak_or_overflow = [h for h in candidates if id(h) not in strong_ids]

    uncertainties = [
        f"{h.hypothesis} (güven: {h.confidence:.2f} — eşiğin altında veya ilk {MAX_HYPOTHESES} dışında)"
        for h in weak_or_overflow
    ]

    if strong:
        top = strong[0]
        summary = top.hypothesis
        recommended_actions = _actions_for_rule(top.rule_id, top.column)
    else:
        top_signal = max(signals, key=lambda s: s["score"])
        summary = (
            f"'{top_signal['column'] or dataset}' üzerinde '{top_signal['type']}' tipi bir "
            f"anomali tespit edildi ancak güçlü bir kök neden hipotezi kurulamadı."
        )
        recommended_actions = []
        uncertainties.insert(0, "Hiçbir hipotez minimum güven eşiğini geçemedi — kanıt yetersiz.")

    severities = {s["severity"] for s in signals}
    severity = (
        "critical" if "critical" in severities
        else "high" if "high" in severities
        else "medium" if "medium" in severities
        else "low"
    )

    return IncidentReport(
        dataset=dataset,
        run_id=run_id,
        summary=summary,
        severity=severity,
        root_causes=strong,
        affected_assets=affected_assets or [],
        recommended_actions=recommended_actions,
        uncertainties=uncertainties,
    )
