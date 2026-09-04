-- Pipeline Sentinel AI — Faz 4: Root Cause Agent (§6.1, §10-§12)
--
-- incidents: bir run'ın sinyallerinden üretilen, kanıtlı kök neden raporu.
-- agent_runs: her `sentinel analyze` çağrısının audit/observability kaydı
-- (§15 — tam OpenTelemetry entegrasyonu değil, hafif bir audit trace).

-- Sinyalin deterministik contract ihlalinden mi yoksa baseline
-- karşılaştırmasından mı geldiği (detector.py::AnomalySignal.source) daha
-- önce persist edilmiyordu; RCA'nın hipotez güvenini doğru puanlaması için
-- gerekli (§10.3 "deterministik doğrulama").
ALTER TABLE signals ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'contract';
ALTER TABLE signals DROP CONSTRAINT IF EXISTS signals_source_check;
ALTER TABLE signals ADD CONSTRAINT signals_source_check CHECK (source IN ('contract', 'baseline'));

CREATE TABLE IF NOT EXISTS incidents (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id  UUID NOT NULL REFERENCES datasets (id) ON DELETE CASCADE,
    run_id      UUID NOT NULL REFERENCES job_runs (id) ON DELETE CASCADE,
    status      TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acknowledged', 'resolved')),
    severity    TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    summary     TEXT NOT NULL,
    root_cause  JSONB NOT NULL DEFAULT '[]'::jsonb,   -- root_causes[] (§12.1)
    impact      JSONB NOT NULL DEFAULT '[]'::jsonb,   -- affected_assets[] (§12.1)
    uncertainties JSONB NOT NULL DEFAULT '[]'::jsonb,
    recommended_actions JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id)  -- MVP: run başına en fazla bir incident (§5.2 correlator henüz yok)
);

CREATE INDEX IF NOT EXISTS idx_incidents_dataset_id ON incidents (dataset_id);

CREATE TABLE IF NOT EXISTS agent_runs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trace_id      UUID NOT NULL DEFAULT gen_random_uuid(),
    incident_id   UUID REFERENCES incidents (id) ON DELETE CASCADE,
    prompt_version TEXT NOT NULL DEFAULT 'v1',
    model         TEXT,                 -- NULL = deterministik mod (LLM çağrılmadı)
    llm_used      BOOLEAN NOT NULL DEFAULT false,
    tool_calls    INTEGER NOT NULL DEFAULT 0,
    evidence_count INTEGER NOT NULL DEFAULT 0,
    input_tokens  INTEGER,
    output_tokens INTEGER,
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agent_runs_incident_id ON agent_runs (incident_id);
