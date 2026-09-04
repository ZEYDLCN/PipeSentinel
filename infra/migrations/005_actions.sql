-- Pipeline Sentinel AI — Faz 7: Repair plan / approval workflow (§10.1, §14.3)
--
-- `rca.py`'nin ürettiği `recommended_actions` (taslak metin) her
-- `analyze_run` çağrısında bu tabloya stabil id'lerle senkronlanır
-- (idempotent — bkz. pipeline.py::sync_incident_actions), böylece
-- API/CLI/dashboard üzerinden tek tek approve/reject edilebilirler.
--
-- ÖNEMLİ: Hiçbir aksiyon burada OTOMATİK UYGULANMAZ. `status` yalnızca
-- bir insan kararını kaydeder (§14.3). Gerçek SQL/dbt çalıştırma, dry-run
-- sandbox ve rollback — §22 yol haritasında "v1.5 Repair sandbox ve
-- otomatik doğrulama" olarak bu MVP'nin kasıtlı olarak dışında bırakıldı.

CREATE TABLE IF NOT EXISTS actions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id         UUID NOT NULL REFERENCES incidents (id) ON DELETE CASCADE,
    type                TEXT NOT NULL,      -- sql_patch | dbt_test | alert
    description         TEXT NOT NULL,
    requires_approval   BOOLEAN NOT NULL DEFAULT true,
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'approved', 'rejected')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (incident_id, type, description)
);

CREATE INDEX IF NOT EXISTS idx_actions_incident_id ON actions (incident_id);

CREATE TABLE IF NOT EXISTS approvals (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action_id   UUID NOT NULL REFERENCES actions (id) ON DELETE CASCADE,
    decision    TEXT NOT NULL CHECK (decision IN ('approve', 'reject')),
    actor       TEXT NOT NULL,
    note        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_approvals_action_id ON approvals (action_id);
