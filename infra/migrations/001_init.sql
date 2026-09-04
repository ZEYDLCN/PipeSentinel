-- Pipeline Sentinel AI — initial schema (Faz 1: Veri laboratuvarı)
-- Kaynak: docs/pipeline-sentinel-design.md §6 Veri modeli
--
-- MVP kapsamı: datasets, columns, jobs, job_runs, profiles, contracts, signals.
-- incidents / agent_runs / approvals Faz 4+ (Root Cause Agent) ile eklenecek.

CREATE EXTENSION IF NOT EXISTS pgcrypto; -- gen_random_uuid()

-- ---------------------------------------------------------------------------
-- datasets: tablo, dosya, topic veya feature kaydı
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS datasets (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace   TEXT NOT NULL,
    name        TEXT NOT NULL,
    type        TEXT NOT NULL DEFAULT 'table',
    owner       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (namespace, name)
);

-- ---------------------------------------------------------------------------
-- columns: kolon metadata'sı
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS columns (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id  UUID NOT NULL REFERENCES datasets (id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    data_type   TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (dataset_id, name)
);

-- ---------------------------------------------------------------------------
-- jobs: pipeline/dönüşüm işi
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jobs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace   TEXT NOT NULL,
    name        TEXT NOT NULL,
    code_ref    TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (namespace, name)
);

-- ---------------------------------------------------------------------------
-- job_runs: çalıştırma geçmişi
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_runs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      UUID NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    status      TEXT NOT NULL DEFAULT 'running'
                CHECK (status IN ('running', 'success', 'failed')),
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at    TIMESTAMPTZ,
    fault_id    TEXT,               -- F01..F08 kontrollü hata enjeksiyon kimliği (varsa)
    metadata    JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_job_runs_job_id_started_at
    ON job_runs (job_id, started_at DESC);

-- ---------------------------------------------------------------------------
-- lineage_edges: dataset/job bağımlılığı (adjacency table; Faz 3'te genişler)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lineage_edges (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id   UUID NOT NULL,
    target_id   UUID NOT NULL,
    edge_type   TEXT NOT NULL
                CHECK (edge_type IN ('READS_FROM', 'WRITES_TO', 'DERIVED_FROM', 'FEEDS')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lineage_edges_source ON lineage_edges (source_id);
CREATE INDEX IF NOT EXISTS idx_lineage_edges_target ON lineage_edges (target_id);

-- ---------------------------------------------------------------------------
-- contracts: beklenen schema ve iş kuralları (§6.1, §7.1)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contracts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id  UUID NOT NULL REFERENCES datasets (id) ON DELETE CASCADE,
    version     INTEGER NOT NULL DEFAULT 1,
    rules       JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (dataset_id, version)
);

-- ---------------------------------------------------------------------------
-- profiles: zaman damgalı kolon kalite profili (§6.2)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS profiles (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    column_id   UUID NOT NULL REFERENCES columns (id) ON DELETE CASCADE,
    run_id      UUID NOT NULL REFERENCES job_runs (id) ON DELETE CASCADE,
    metrics     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (column_id, run_id)
);

CREATE INDEX IF NOT EXISTS idx_profiles_column_created
    ON profiles (column_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- signals: tekil anomali sinyali (§7)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signals (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id      UUID NOT NULL REFERENCES job_runs (id) ON DELETE CASCADE,
    column_id   UUID REFERENCES columns (id) ON DELETE CASCADE,
    dataset_id  UUID NOT NULL REFERENCES datasets (id) ON DELETE CASCADE,
    type        TEXT NOT NULL,      -- schema | null | volume | distribution | uniqueness | freshness | range
    severity    TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    score       DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    evidence    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_signals_run_id ON signals (run_id);
CREATE INDEX IF NOT EXISTS idx_signals_dataset_id ON signals (dataset_id);
