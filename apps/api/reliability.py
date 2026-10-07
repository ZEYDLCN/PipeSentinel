from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Engine

from pipeline_sentinel import reliability as service, reliability_store as store
from pipeline_sentinel.contract_io import digest, load_contract
from pipeline_sentinel.suggest import frame_from_csv, suggest_contract
from .auth import identity
from .deps import get_engine

router = APIRouter(prefix="/api/v1/reliability", tags=["Reliability"])


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceRequest(Payload):
    name: str = Field(min_length=1, max_length=160)
    config: dict[str, Any]


class ContractRequest(Payload):
    yaml: str = Field(max_length=200000)


class SuggestRequest(Payload):
    csv: str = Field(min_length=1, max_length=5_000_000)


class IngestRequest(Payload):
    namespace: str = Field(min_length=1, max_length=160)
    artifact: dict[str, Any]
    run_results: dict[str, Any] | None = None


class FeedbackRequest(Payload):
    verdict: str
    note: str = Field(default="", max_length=2000)


class TriageRequest(Payload):
    status: str
    assignee: str = Field(default="", max_length=160)
    note: str = Field(default="", max_length=2000)


class MuteRequest(Payload):
    source_id: str = Field(min_length=1, max_length=36)
    signal_type: str = Field(min_length=1, max_length=60)
    column: str | None = Field(default=None, max_length=160)
    hours: int = Field(ge=1, le=720)
    reason: str = Field(min_length=5, max_length=500)


class ExpectedChangeRequest(Payload):
    note: str = Field(min_length=1, max_length=2000)


class PolicyRequest(Payload):
    asset: str = Field(min_length=1, max_length=300)
    owner: str = Field(min_length=1, max_length=160)
    criticality: int = Field(ge=1, le=5)
    sla_minutes: int = Field(ge=1)


class RepairRequest(Payload):
    action: dict[str, Any]


def invoke(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except LookupError:
        raise HTTPException(404, "Kayıt bulunamadı") from None
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None


@router.get("/session")
def session(request: Request):
    import os
    return {**identity(request), "mode": os.environ.get("SENTINEL_MODE", "development")}


@router.get("/monitoring")
def monitoring(engine: Engine = Depends(get_engine)):
    return service.monitoring_status(engine)


@router.get("/metrics")
def metrics(engine: Engine = Depends(get_engine)):
    return service.metrics(engine)


@router.post("/contracts/validate")
def validate(payload: ContractRequest):
    contract = invoke(load_contract, payload.yaml)
    return {"valid": True, "hash": digest(contract), "contract": contract}


@router.post("/contracts/suggest")
def suggest(payload: SuggestRequest):
    """CSV örneğinden taslak sözleşme; veri kaydedilmez, kaynağa bağlanılmaz."""
    from io import StringIO
    suggestion = invoke(lambda: suggest_contract(frame_from_csv(StringIO(payload.csv))))
    return {"yaml": suggestion.to_yaml(), "contract": suggestion.contract, "notes": suggestion.notes}


@router.get("/sources")
def sources(engine: Engine = Depends(get_engine)):
    with engine.connect() as conn:
        return store.rows(conn, store.sources)


@router.post("/sources")
def save_source(payload: SourceRequest, request: Request, engine: Engine = Depends(get_engine)):
    return invoke(service.register_source, engine, payload.name, payload.config, identity(request)["actor"])


@router.post("/sources/{source_id}/analyze", status_code=202)
def analyze(source_id: str, request: Request, idempotency_key: str = Header(..., max_length=160), engine: Engine = Depends(get_engine)):
    return invoke(service.enqueue, engine, source_id, idempotency_key, identity(request)["actor"])


@router.get("/jobs")
def jobs(engine: Engine = Depends(get_engine)):
    with engine.connect() as conn:
        return [{k: v for k, v in r.items() if k != "config"} for r in store.rows(conn, store.jobs)]


@router.get("/jobs/{job_id}")
def job(job_id: str, engine: Engine = Depends(get_engine)):
    with engine.connect() as conn:
        result = invoke(store.get, conn, store.jobs, job_id)
        result.pop("config", None)
        return result


@router.post("/jobs/{job_id}/retry", status_code=202)
def retry(job_id: str, request: Request, engine: Engine = Depends(get_engine)):
    invoke(service.retry_job, engine, job_id, identity(request)["actor"])
    return {"status": "pending"}


@router.get("/observations")
def observations(source_id: str | None = None, limit: int = Query(50, ge=1, le=200), engine: Engine = Depends(get_engine)):
    with engine.connect() as conn:
        rows = store.rows(conn, store.observations, store.observations.c.source_id == source_id if source_id else None, limit)
        triage = service.triage_map(conn, [r["id"] for r in rows])
        return [{"id": r["id"], "source_id": r["source_id"], "created_at": r["created_at"], "quality": r["quality"],
                 "severity": r["body"]["severity"], "signal_count": len(r["body"]["signals"]),
                 "summary": r["body"]["rca"]["summary"], "baseline_state": r["body"]["baseline"]["state"],
                 "coverage": r["body"]["coverage"], "group_id": r["body"]["group_id"], "impact": r["body"]["impact"],
                 "muted": r["body"].get("muted", False),
                 "triage": ({"status": triage[r["id"]]["status"], "assignee": triage[r["id"]]["assignee"]}
                            if r["id"] in triage else None)} for r in rows]


@router.get("/sources/{source_id}/history")
def history(source_id: str, limit: int = Query(60, ge=1, le=200), engine: Engine = Depends(get_engine)):
    return invoke(service.metric_history, engine, source_id, limit)


@router.post("/observations/{observation_id}/triage")
def triage(observation_id: str, payload: TriageRequest, request: Request, engine: Engine = Depends(get_engine)):
    return invoke(service.set_triage, engine, observation_id, payload.status, payload.assignee, payload.note, identity(request)["actor"])


@router.get("/mutes")
def mutes(engine: Engine = Depends(get_engine)):
    return service.active_mutes(engine)


@router.post("/mutes")
def add_mute(payload: MuteRequest, request: Request, engine: Engine = Depends(get_engine)):
    return invoke(service.add_mute, engine, payload.source_id, payload.signal_type, payload.column, payload.hours,
                  payload.reason, identity(request)["actor"])


@router.delete("/mutes/{mute_id}")
def remove_mute(mute_id: str, request: Request, engine: Engine = Depends(get_engine)):
    return invoke(service.remove_mute, engine, mute_id, identity(request)["actor"])


@router.get("/observations/{observation_id}")
def detail(observation_id: str, engine: Engine = Depends(get_engine)):
    return invoke(service.observation_detail, engine, observation_id)


@router.post("/observations/{observation_id}/baseline")
def baseline(observation_id: str, request: Request, engine: Engine = Depends(get_engine)):
    return invoke(service.accept_baseline, engine, observation_id, identity(request)["actor"])


@router.post("/observations/{observation_id}/expected-change")
def expected_change(observation_id: str, payload: ExpectedChangeRequest, request: Request, engine: Engine = Depends(get_engine)):
    return invoke(service.accept_expected_change, engine, observation_id, payload.note, identity(request)["actor"])


@router.post("/observations/{observation_id}/replay")
def replay(observation_id: str, engine: Engine = Depends(get_engine)):
    return invoke(service.replay, engine, observation_id)


@router.post("/observations/{observation_id}/feedback")
def feedback(observation_id: str, payload: FeedbackRequest, request: Request, engine: Engine = Depends(get_engine)):
    return invoke(service.record_feedback, engine, observation_id, payload.verdict, payload.note, identity(request)["actor"])


@router.post("/observations/{observation_id}/repairs")
def repair(observation_id: str, payload: RepairRequest, request: Request, engine: Engine = Depends(get_engine)):
    return invoke(service.try_repair, engine, observation_id, payload.action, identity(request)["actor"])


@router.post("/repairs/{repair_id}/{decision}")
def decide(repair_id: str, decision: str, request: Request, engine: Engine = Depends(get_engine)):
    return invoke(service.decide_repair, engine, repair_id, decision, identity(request)["actor"])


@router.post("/ingest/{kind}")
def ingest(kind: str, payload: IngestRequest, request: Request, engine: Engine = Depends(get_engine)):
    if kind not in {"dbt", "openlineage"}:
        raise HTTPException(422, "dbt/openlineage gerekli")
    return invoke(service.ingest, engine, payload.namespace, payload.artifact, identity(request)["actor"], payload.run_results, kind)


@router.get("/lineage")
def lineage(engine: Engine = Depends(get_engine)):
    with engine.connect() as conn:
        return store.rows(conn, store.graphs)


@router.get("/policies")
def policies(engine: Engine = Depends(get_engine)):
    with engine.connect() as conn:
        return store.rows(conn, store.policies)


@router.post("/policies")
def policy(payload: PolicyRequest, request: Request, engine: Engine = Depends(get_engine)):
    return invoke(service.set_policy, engine, **payload.model_dump(), actor=identity(request)["actor"])


@router.get("/notifications")
def notifications(engine: Engine = Depends(get_engine)):
    with engine.connect() as conn:
        return store.rows(conn, store.notifications)
