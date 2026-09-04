-- Pipeline Sentinel AI — Faz 3: Lineage graph (§9)
--
-- lineage_edges'e düğüm tipi bilgisi ekler (source_type/target_type) ve
-- Dashboard/ML Feature gibi backing tablosu olmayan terminal düğümler
-- için external_assets tablosunu tanımlar.
--
-- Yön kuralı (bilinçli mühendislik kararı — §9.2'deki edge isimlerinin
-- grameri her zaman bu yönle örtüşmez): her kenar source_id -> target_id
-- olacak şekilde VERİ/ETKİ AKIŞI yönünde saklanır. source = bozulursa
-- etkileyen (upstream), target = etkilenen (downstream). Böylece downstream
-- etki sorgusu source->target, upstream kaynak sorgusu target->source
-- yönünde tek tip bir traversal ile yazılabilir (bkz. lineage.py).
--
--   READS_FROM   : dataset -> job            (job'ın okuduğu dataset upstream'dir)
--   WRITES_TO    : job -> dataset            (job, ürettiği dataset'in upstream'idir)
--   DERIVED_FROM : origin_column -> derived_column
--   FEEDS        : dataset/column -> external_asset (dashboard/ml_feature)

ALTER TABLE lineage_edges ADD COLUMN IF NOT EXISTS source_type TEXT;
ALTER TABLE lineage_edges ADD COLUMN IF NOT EXISTS target_type TEXT;

ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_edge_type_check;
ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_edge_type_check
    CHECK (edge_type IN ('READS_FROM', 'WRITES_TO', 'DERIVED_FROM', 'FEEDS'));

ALTER TABLE lineage_edges DROP CONSTRAINT IF EXISTS lineage_edges_node_type_check;
ALTER TABLE lineage_edges ADD CONSTRAINT lineage_edges_node_type_check
    CHECK (
        source_type IN ('dataset', 'column', 'job', 'external_asset')
        AND target_type IN ('dataset', 'column', 'job', 'external_asset')
    );

-- Aynı kenarın tekrar tekrar eklenmesi (idempotent sync) için UNIQUE index —
-- INSERT ... ON CONFLICT (source_id, target_id, edge_type) DO NOTHING bunu kullanır.
CREATE UNIQUE INDEX IF NOT EXISTS idx_lineage_edges_unique
    ON lineage_edges (source_id, target_id, edge_type);

-- ---------------------------------------------------------------------------
-- external_assets: Dashboard / ML Feature gibi backing tablosu olmayan
-- terminal downstream düğümler (§9.1).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS external_assets (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type        TEXT NOT NULL CHECK (type IN ('dashboard', 'ml_feature')),
    name        TEXT NOT NULL,
    owner       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (type, name)
);
