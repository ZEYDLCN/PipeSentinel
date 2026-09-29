"""Pipeline Sentinel AI — Faz 5 REST API (§13).

Çalıştırma:

    export DATABASE_URL=postgresql+psycopg://sentinel:sentinel@localhost:5432/pipeline_sentinel
    python -m uvicorn apps.api.main:app --reload --port 8000

API: http://localhost:8000/api/v1/...  (Swagger UI: /docs)
Dashboard (apps/web, statik dosyalar): http://localhost:8000/

Not (§13.1 "Incident oluşturma cevabı"): Dokümanda uzun analizler için
`202 Accepted` + polling öngörülür. Bu demo ölçeğinde (yüzlerce-binlerce
satır) hem pipeline run hem RCA analizi saniyeler içinde bitiyor; bu yüzden
MVP basitliği için endpoint'ler senkron `200 OK` döner. Gerçek ölçekte
(§19.2) bu uçlar bir kuyruğa (job table / Kafka) devredilmeli ve
`202 Accepted` + `GET .../{id}` polling'e geçilmelidir.
"""

from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from sqlalchemy import Engine

from pipeline_sentinel import lineage as lineage_module
from pipeline_sentinel import pipeline as pipeline_module
from pipeline_sentinel.analyze import analyze_run
from pipeline_sentinel.faults import FAULT_CATALOG
from pipeline_sentinel.orchestrator import execute_pipeline_run

from . import schemas
from .deps import get_engine
from .auth import access_policy, identity
from .reliability import router as reliability_router

WEB_DIR = Path(__file__).resolve().parents[1] / "web"

app = FastAPI(
    title="Pipeline Sentinel AI API",
    description="Self-healing data pipeline agent — REST API (§13)",
    version="0.1.0",
)

app.middleware("http")(access_policy)
app.include_router(reliability_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/api/v1/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


@app.get("/api/v1/datasets", response_model=list[schemas.DatasetOut])
def list_datasets(engine: Engine = Depends(get_engine)) -> list[dict[str, Any]]:
    datasets = pipeline_module.list_datasets(engine)
    out = []
    for ds in datasets:
        latest = pipeline_module.get_latest_run(engine, ds["name"])
        severity = pipeline_module.get_run_severity(engine, latest["run_id"]) if latest else None
        out.append(
            {
                **ds,
                "latest_run_id": latest["run_id"] if latest else None,
                "latest_severity": severity,
                "latest_fault_id": latest["fault_id"] if latest else None,
                "latest_run_at": latest["started_at"] if latest else None,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Fault catalog (dashboard'ın "hata enjekte et" formu için — §16.1)
# ---------------------------------------------------------------------------


@app.get("/api/v1/faults", response_model=list[schemas.FaultCatalogEntryOut])
def list_faults() -> list[dict[str, Any]]:
    return [
        {"fault_id": fid, "dataset": entry["dataset"], "label": entry["label"]}
        for fid, entry in FAULT_CATALOG.items()
    ]


# ---------------------------------------------------------------------------
# Pipeline runs (§13 "/profile-runs" — burada tam pipeline run'ı anlamına
# genişletilmiştir, bkz. modül docstring'i)
# ---------------------------------------------------------------------------


@app.post("/api/v1/pipeline-runs", response_model=schemas.PipelineRunResponse)
def trigger_pipeline_run(
    payload: schemas.PipelineRunRequest, engine: Engine = Depends(get_engine)
) -> dict[str, Any]:
    if payload.fault_id and payload.fault_id not in FAULT_CATALOG:
        raise HTTPException(status_code=400, detail=f"Bilinmeyen fault_id: {payload.fault_id}")

    outcome = execute_pipeline_run(
        engine,
        fault_id=payload.fault_id,
        n_customers=payload.n_customers,
        n_orders=payload.n_orders,
        seed=payload.seed,
    )
    return {
        "run_ids": outcome.run_ids,
        "fault_id": outcome.fault_id,
        "reports": {k: v.to_dict() for k, v in outcome.reports.items()},
    }


@app.post("/api/v1/bootstrap")
def trigger_bootstrap(payload: schemas.BootstrapRequest, engine: Engine = Depends(get_engine)) -> dict[str, Any]:
    """Baseline oluşturmak için art arda sağlıklı run'lar çalıştırır (§8.1)."""
    for i in range(payload.runs):
        execute_pipeline_run(
            engine, fault_id=None, n_customers=payload.n_customers, n_orders=payload.n_orders, seed=2_000_000 + i
        )
    return {"runs_completed": payload.runs}


@app.get("/api/v1/pipeline-runs/{run_id}")
def get_pipeline_run(run_id: str, engine: Engine = Depends(get_engine)) -> dict[str, Any]:
    info = pipeline_module.get_run_info(engine, run_id)
    if info is None:
        raise HTTPException(status_code=404, detail="run bulunamadı")
    signals = pipeline_module.get_signals_for_run(engine, run_id)
    return {**info, "signals": signals, "severity": pipeline_module.get_run_severity(engine, run_id)}


@app.post("/api/v1/pipeline-runs/{run_id}/analyze", response_model=schemas.AnalyzeResponse)
def analyze_pipeline_run(
    run_id: str,
    llm: bool | None = Query(None, description="LLM sentezini zorla aç/kapat"),
    engine: Engine = Depends(get_engine),
) -> dict[str, Any]:
    """Bir run için Root Cause Agent'ı ilk kez çalıştırır ve incident'ı
    oluşturur (§10.2). `POST /incidents/{id}/analyze` bunun run_id zaten
    biliniyorsa (bir incident üzerinden) kullanılan kısayoludur — ikisi de
    aynı `analyze_run`'ı çağırır ve idempotenttir (`incidents.run_id`
    UNIQUE — tekrar çağrı incident'ı günceller, çoğaltmaz)."""
    if pipeline_module.get_run_info(engine, run_id) is None:
        raise HTTPException(status_code=404, detail="run bulunamadı")

    report, meta = analyze_run(engine, run_id, use_llm=llm)
    return {
        "incident_id": meta["incident_id"],
        "agent_run_id": meta["agent_run_id"],
        "llm_used": report.llm_used,
        "report": report.to_dict(),
    }


# ---------------------------------------------------------------------------
# Incidents (§13 /incidents)
# ---------------------------------------------------------------------------


@app.get("/api/v1/incidents", response_model=list[schemas.IncidentSummaryOut])
def list_incidents(
    dataset: str | None = Query(None),
    severity: str | None = Query(None),
    limit: int = Query(50, le=200),
    engine: Engine = Depends(get_engine),
) -> list[dict[str, Any]]:
    return pipeline_module.list_incidents(engine, dataset=dataset, severity=severity, limit=limit)


@app.get("/api/v1/incidents/{incident_id}", response_model=schemas.IncidentDetailOut)
def get_incident(incident_id: str, engine: Engine = Depends(get_engine)) -> dict[str, Any]:
    incident = pipeline_module.get_incident(engine, incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="incident bulunamadı")
    incident["signals"] = pipeline_module.get_signals_for_run(engine, incident["run_id"])
    # `recommended_actions`, incidents.recommended_actions'ın statik JSONB
    # kopyası değil, `actions` tablosundaki stabil-id'li ve
    # onaylanabilir/reddedilebilir satırlardır (Faz 7, §14.3).
    incident["recommended_actions"] = pipeline_module.list_actions_for_incident(engine, incident_id)
    return incident


@app.get("/api/v1/incidents/{incident_id}/actions", response_model=list[schemas.ActionOut])
def list_incident_actions(incident_id: str, engine: Engine = Depends(get_engine)) -> list[dict[str, Any]]:
    if pipeline_module.get_incident(engine, incident_id) is None:
        raise HTTPException(status_code=404, detail="incident bulunamadı")
    return pipeline_module.list_actions_for_incident(engine, incident_id)


@app.post("/api/v1/incidents/{incident_id}/analyze", response_model=schemas.AnalyzeResponse)
def reanalyze_incident(
    incident_id: str,
    llm: bool | None = Query(None, description="LLM sentezini zorla aç/kapat"),
    engine: Engine = Depends(get_engine),
) -> dict[str, Any]:
    """§13 POST /incidents/{id}/analyze — RCA agent'ı incident'ın run'ı
    üzerinde yeniden çalıştırır (§10.2 agent döngüsü)."""
    incident = pipeline_module.get_incident(engine, incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="incident bulunamadı")

    report, meta = analyze_run(engine, incident["run_id"], use_llm=llm)
    return {
        "incident_id": meta["incident_id"],
        "agent_run_id": meta["agent_run_id"],
        "llm_used": report.llm_used,
        "report": report.to_dict(),
    }


# ---------------------------------------------------------------------------
# Lineage (§13 GET /lineage/{assetId}, §9)
# ---------------------------------------------------------------------------


def _resolve_asset(engine: Engine, asset_id: str) -> tuple[str, str]:
    """`asset_id` "dataset" ya da "dataset.column" biçiminde olabilir."""
    dataset_name, _, column_name = asset_id.partition(".")
    resolved = pipeline_module.resolve_lineage_node(engine, dataset_name, column_name or None)
    if resolved is None:
        raise HTTPException(status_code=404, detail=f"Varlık bulunamadı: {asset_id}")
    return resolved


@app.get("/api/v1/lineage/{asset_id}", response_model=schemas.LineageResponse)
def get_lineage(
    asset_id: str, max_hops: int = Query(6, ge=1, le=20), engine: Engine = Depends(get_engine)
) -> dict[str, Any]:
    node_id, node_type = _resolve_asset(engine, asset_id)
    edges = pipeline_module.fetch_all_edges(engine)

    downstream = lineage_module.downstream_impact(edges, node_id, max_hops=max_hops)
    upstream = lineage_module.upstream_sources(edges, node_id, max_hops=max_hops)

    def _to_out(hits: list) -> list[dict[str, Any]]:
        return [
            {
                "node_id": h.node_id,
                "node_type": h.node_type,
                "name": pipeline_module.resolve_node_name(engine, h.node_id, h.node_type),
                "hop": h.hop,
                "via_edge_type": h.via_edge_type,
            }
            for h in hits
        ]

    return {
        "asset": asset_id,
        "node_type": node_type,
        "downstream": _to_out(downstream),
        "upstream": _to_out(upstream),
    }


# ---------------------------------------------------------------------------
# Actions / approval workflow (§13 /actions/{id}/approve|reject, §14.3)
# ---------------------------------------------------------------------------


@app.post("/api/v1/actions/{action_id}/approve", response_model=schemas.ActionOut)
def approve_action(
    action_id: str, payload: schemas.DecisionRequest, request: Request, engine: Engine = Depends(get_engine)
) -> dict[str, Any]:
    """Bir aksiyonu onaylar. Hiçbir SQL/dbt çalıştırmaz — yalnızca insan
    kararını kaydeder (§14.3); gerçek execution Faz 7'nin bilinçli olarak
    dışında bırakılan bir sonraki adımıdır (§22 "v1.5 Repair sandbox")."""
    result = pipeline_module.decide_action(engine, action_id, "approve", actor=identity(request)["actor"], note=payload.note)
    if result is None:
        raise HTTPException(status_code=404, detail="aksiyon bulunamadı")
    return result


@app.post("/api/v1/actions/{action_id}/reject", response_model=schemas.ActionOut)
def reject_action(
    action_id: str, payload: schemas.DecisionRequest, request: Request, engine: Engine = Depends(get_engine)
) -> dict[str, Any]:
    result = pipeline_module.decide_action(engine, action_id, "reject", actor=identity(request)["actor"], note=payload.note)
    if result is None:
        raise HTTPException(status_code=404, detail="aksiyon bulunamadı")
    return result


@app.get("/api/v1/actions/{action_id}/approvals", response_model=list[schemas.ApprovalOut])
def get_action_approvals(action_id: str, engine: Engine = Depends(get_engine)) -> list[dict[str, Any]]:
    if pipeline_module.get_action(engine, action_id) is None:
        raise HTTPException(status_code=404, detail="aksiyon bulunamadı")
    return pipeline_module.list_approvals_for_action(engine, action_id)


# ---------------------------------------------------------------------------
# Statik dashboard (apps/web) — kök path'e mount edilir, API route'larından
# SONRA tanımlanmalı ki /api/v1/* öncelik alsın.
# ---------------------------------------------------------------------------

if WEB_DIR.exists():
    from fastapi.responses import FileResponse

    @app.get("/", include_in_schema=False)
    def dashboard():
        return FileResponse(WEB_DIR / "reliability.html")

    @app.get("/demo", include_in_schema=False)
    def demo_dashboard():
        return FileResponse(WEB_DIR / "index.html")

    app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
