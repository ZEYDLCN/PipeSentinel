"""Pydantic response/request modelleri (§13 API tasarımı)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class DatasetOut(BaseModel):
    id: str
    namespace: str
    name: str
    type: str
    owner: str | None = None
    latest_run_id: str | None = None
    latest_severity: str | None = None
    latest_fault_id: str | None = None
    latest_run_at: datetime | None = None


class SignalOut(BaseModel):
    id: str
    type: str
    column: str | None = None
    severity: str
    score: float
    evidence: dict[str, Any]
    source: str


class HypothesisOut(BaseModel):
    hypothesis: str
    confidence: float
    evidence_ids: list[str]
    counter_evidence: list[str] = []


class RecommendedActionOut(BaseModel):
    type: str
    description: str
    requires_approval: bool


class ActionOut(BaseModel):
    """Faz 7 — kalıcı, onaylanabilir/reddedilebilir aksiyon (§10.1, §14.3).
    `recommended_actions` taslağının stabil-id'li hali; hiçbir işlem
    otomatik uygulanmaz, yalnızca `status` bir insan kararını taşır."""

    id: str
    incident_id: str
    type: str
    description: str
    requires_approval: bool
    status: str
    created_at: datetime


class ApprovalOut(BaseModel):
    id: str
    decision: str
    actor: str
    note: str | None = None
    created_at: datetime


class DecisionRequest(BaseModel):
    actor: str
    note: str | None = None


class IncidentSummaryOut(BaseModel):
    id: str
    dataset: str
    run_id: str
    status: str
    severity: str
    summary: str
    created_at: datetime


class IncidentDetailOut(BaseModel):
    id: str
    dataset: str
    run_id: str
    status: str
    severity: str
    summary: str
    root_causes: list[dict[str, Any]]
    affected_assets: list[str]
    uncertainties: list[str]
    recommended_actions: list[ActionOut]
    created_at: datetime
    signals: list[SignalOut]


class AnalyzeResponse(BaseModel):
    incident_id: str
    agent_run_id: str
    llm_used: bool
    report: dict[str, Any]


class LineageNodeOut(BaseModel):
    node_id: str
    node_type: str
    name: str
    hop: int
    via_edge_type: str


class LineageResponse(BaseModel):
    asset: str
    node_type: str
    downstream: list[LineageNodeOut]
    upstream: list[LineageNodeOut]


class PipelineRunRequest(BaseModel):
    fault_id: str | None = None
    n_customers: int = 50
    n_orders: int = 500
    seed: int | None = None


class PipelineRunResponse(BaseModel):
    run_ids: dict[str, str]
    fault_id: str | None
    reports: dict[str, dict[str, Any]]


class BootstrapRequest(BaseModel):
    runs: int = 14
    n_customers: int = 50
    n_orders: int = 500


class FaultCatalogEntryOut(BaseModel):
    fault_id: str
    dataset: str
    label: str
