"""Root Cause Agent orkestrasyonu (§10.2 agent döngüsü):

    Plan → Kanıt topla → Hipotez kur → Doğrula → Raporla

Bu modül DB'yi (`pipeline.py`), lineage graph'ı (`lineage.py`), kural
tabanlı hipotez motorunu (`rca.py`) ve opsiyonel LLM sentezini (`llm.py`)
tek bir çağrıda birleştirir. `sentinel analyze` CLI komutu bunu kullanır.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import Engine

from . import lineage as lineage_module
from . import llm as llm_module
from . import pipeline as pipeline_module
from . import rca


def _compute_affected_assets(
    engine: Engine, dataset_id: str, dataset_name: str, signals: list[dict[str, Any]]
) -> list[str]:
    """En yüksek skorlu sinyalin düğümünden başlayarak lineage graph'ında
    downstream etkiyi hesaplar (Faz 3 — §9.3 "Downstream etki")."""
    if not signals:
        return []

    edges = pipeline_module.fetch_all_edges(engine)
    if not edges:
        return []

    top_signal = max(signals, key=lambda s: s["score"])

    node_id: str | None = None
    node_type = "dataset"
    if top_signal["column"]:
        node_id = pipeline_module.find_column_id(engine, dataset_name, top_signal["column"])
        node_type = "column"
    if node_id is None:
        node_id, node_type = dataset_id, "dataset"

    hits = lineage_module.downstream_impact(edges, node_id)
    return [pipeline_module.resolve_node_name(engine, h.node_id, h.node_type) for h in hits]


def analyze_run(
    engine: Engine, run_id: str, use_llm: bool | None = None
) -> tuple[rca.IncidentReport, dict[str, Any]]:
    """Bir run için tam RCA akışını çalıştırır ve `incidents`/`agent_runs`
    olarak kalıcı hale getirir.

    Returns
    -------
    (report, meta) — `meta` incident_id/agent_run_id ve (varsa) LLM
    kullanım bilgisini taşır.
    """
    context = pipeline_module.get_incident_context(engine, run_id)
    dataset_id = context["dataset_id"]
    dataset_name = context["dataset_name"]
    signals = context["signals"]
    schema_diff = context["schema_diff"]

    affected_assets = _compute_affected_assets(engine, dataset_id, dataset_name, signals)

    report = rca.generate_report(
        dataset=dataset_name,
        run_id=run_id,
        signals=signals,
        schema_diff=schema_diff,
        affected_assets=affected_assets,
    )

    should_use_llm = llm_module.is_llm_available() if use_llm is None else (use_llm and llm_module.is_llm_available())
    llm_meta: dict[str, Any] = {}
    if should_use_llm and signals:
        refined, llm_meta = llm_module.refine_report(report, context)
        if refined is not None:
            report = refined

    incident_id = pipeline_module.save_incident(engine, dataset_id, run_id, report)
    pipeline_module.sync_incident_actions(engine, incident_id, report.recommended_actions)
    agent_run_id = pipeline_module.save_agent_run(
        engine,
        incident_id=incident_id,
        evidence_count=len(signals),
        llm_used=report.llm_used,
        model=llm_meta.get("model"),
        metadata=llm_meta,
    )

    return report, {"incident_id": incident_id, "agent_run_id": agent_run_id, "llm_meta": llm_meta}
