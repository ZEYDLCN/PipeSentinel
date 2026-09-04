"""Postgres'e bağlanan orkestrasyon katmanı.

`synthetic` / `profiler` / `contracts` / `faults` / `detector` modülleri saf
Python'dur ve veritabanı bilmez; bu modül onları `datasets`, `jobs`,
`job_runs`, `profiles`, `signals` tablolarına bağlar (§6.1 veri modeli).
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import pandas as pd
from sqlalchemy import Engine, text

from .detector import AnomalySignal, DetectionReport
from .lineage import Edge
from .rca import IncidentReport


def _dtype_label(series: pd.Series) -> str:
    kind = series.dtype.kind
    if kind in "iu":
        return "integer"
    if kind == "f":
        return "float"
    if kind == "b":
        return "boolean"
    if kind == "M" or "datetime" in str(series.dtype):
        return "datetime"
    return "string"


def register_dataset(
    engine: Engine,
    namespace: str,
    name: str,
    df: pd.DataFrame,
    type_: str = "table",
    owner: str | None = None,
    track_column_types: bool = True,
) -> tuple[str, dict[str, str]]:
    """Dataset + kolonlarını idempotent biçimde kaydeder.

    `track_column_types=False` iken mevcut bir kolonun `data_type`'ı
    güncellenmez — yalnızca kolon ilk kez görülüyorsa yazılır. Bunu
    hata enjekte edilmiş (fault) bir run için kullanın: aksi halde F02
    gibi bir tip-değişikliği hatası, `columns` tablosundaki kanonik
    tipi kalıcı olarak (bir sonraki sağlıklı run'a kadar) bozar — bu
    kayıt gerçek şema kataloğu olarak okunur (bkz. get_schema_diff,
    get_all_column_ids).

    Returns (dataset_id, {column_name: column_id}).
    """
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO datasets (namespace, name, type, owner)
                VALUES (:ns, :name, :type, :owner)
                ON CONFLICT (namespace, name) DO UPDATE SET owner = EXCLUDED.owner
                RETURNING id
                """
            ),
            {"ns": namespace, "name": name, "type": type_, "owner": owner},
        ).first()
        dataset_id = str(row[0])

        data_type_update_sql = (
            "EXCLUDED.data_type" if track_column_types else "columns.data_type"
        )
        column_ids: dict[str, str] = {}
        for col in df.columns:
            crow = conn.execute(
                text(
                    f"""
                    INSERT INTO columns (dataset_id, name, data_type)
                    VALUES (:dataset_id, :name, :data_type)
                    ON CONFLICT (dataset_id, name)
                        DO UPDATE SET data_type = {data_type_update_sql}
                    RETURNING id
                    """
                ),
                {"dataset_id": dataset_id, "name": col, "data_type": _dtype_label(df[col])},
            ).first()
            column_ids[col] = str(crow[0])

    return dataset_id, column_ids


def register_job(engine: Engine, namespace: str, name: str, code_ref: str | None = None) -> str:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO jobs (namespace, name, code_ref)
                VALUES (:ns, :name, :code_ref)
                ON CONFLICT (namespace, name) DO UPDATE SET code_ref = EXCLUDED.code_ref
                RETURNING id
                """
            ),
            {"ns": namespace, "name": name, "code_ref": code_ref},
        ).first()
        return str(row[0])


def start_run(engine: Engine, job_id: str, fault_id: str | None = None) -> str:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO job_runs (job_id, status, fault_id)
                VALUES (:job_id, 'running', :fault_id)
                RETURNING id
                """
            ),
            {"job_id": job_id, "fault_id": fault_id},
        ).first()
        return str(row[0])


def finish_run(engine: Engine, run_id: str, status: str = "success", metadata: dict[str, Any] | None = None) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE job_runs
                SET status = :status, ended_at = now(),
                    metadata = COALESCE(metadata, '{}'::jsonb) || CAST(:metadata AS JSONB)
                WHERE id = :run_id
                """
            ),
            {"run_id": run_id, "status": status, "metadata": json.dumps(metadata or {})},
        )


def load_dataframe(engine: Engine, schema: str, table: str, df: pd.DataFrame, if_exists: str = "append") -> None:
    df.to_sql(table, engine, schema=schema, if_exists=if_exists, index=False, method="multi", chunksize=1000)


def save_profile(
    engine: Engine,
    run_id: str,
    column_ids: dict[str, str],
    profile: dict[str, dict[str, Any]],
) -> None:
    with engine.begin() as conn:
        for column_name, metrics in profile.items():
            column_id = column_ids.get(column_name)
            if column_id is None:
                continue
            conn.execute(
                text(
                    """
                    INSERT INTO profiles (column_id, run_id, metrics)
                    VALUES (:column_id, :run_id, CAST(:metrics AS JSONB))
                    ON CONFLICT (column_id, run_id) DO UPDATE SET metrics = EXCLUDED.metrics
                    """
                ),
                {"column_id": column_id, "run_id": run_id, "metrics": json.dumps(metrics)},
            )


def save_signals(
    engine: Engine,
    run_id: str,
    dataset_id: str,
    column_ids: dict[str, str],
    signals: list[AnomalySignal],
) -> None:
    with engine.begin() as conn:
        for signal in signals:
            column_id = column_ids.get(signal.column) if signal.column else None
            conn.execute(
                text(
                    """
                    INSERT INTO signals (run_id, column_id, dataset_id, type, severity, score, evidence, source)
                    VALUES (:run_id, :column_id, :dataset_id, :type, :severity, :score, CAST(:evidence AS JSONB), :source)
                    """
                ),
                {
                    "run_id": run_id,
                    "column_id": column_id,
                    "dataset_id": dataset_id,
                    "type": signal.type,
                    "severity": signal.severity,
                    "score": signal.score,
                    "evidence": json.dumps(signal.evidence),
                    "source": signal.source,
                },
            )


def get_baseline_profile(
    engine: Engine,
    job_id: str,
    column_ids: dict[str, str],
    exclude_run_id: str | None = None,
) -> tuple[dict[str, dict[str, Any]], int | None]:
    """Aynı job'a ait, hata enjekte edilmemiş en güncel başarılı run'ın
    profilini döner (baseline). Bulunamazsa boş sözlük döner (§8.1:
    ilk kurulumda gözlem modu — karşılaştırma yapılmaz)."""
    with engine.begin() as conn:
        params: dict[str, Any] = {"job_id": job_id}
        exclude_clause = ""
        if exclude_run_id:
            exclude_clause = "AND id != :exclude_run_id"
            params["exclude_run_id"] = exclude_run_id

        row = conn.execute(
            text(
                f"""
                SELECT id, metadata
                FROM job_runs
                WHERE job_id = :job_id
                  AND status = 'success'
                  AND fault_id IS NULL
                  {exclude_clause}
                ORDER BY started_at DESC
                LIMIT 1
                """
            ),
            params,
        ).first()

        if row is None:
            return {}, None

        baseline_run_id, metadata = row
        baseline_row_count = (metadata or {}).get("row_count")

        if not column_ids:
            return {}, baseline_row_count

        # Kolon başına ayrı sorgu yerine tek sorguda tüm profilleri çek
        # (N+1 önlenir — bir dataset'in kolon sayısı arttıkça her run/analyze
        # döngüsünde orantılı olarak büyüyen sorgu sayısını sabitler).
        id_to_name = {v: k for k, v in column_ids.items()}
        rows = conn.execute(
            text(
                "SELECT column_id, metrics FROM profiles "
                "WHERE run_id = :rid AND column_id = ANY(:cids)"
            ),
            {"rid": str(baseline_run_id), "cids": list(column_ids.values())},
        ).all()

        profile: dict[str, dict[str, Any]] = {
            id_to_name[str(cid)]: metrics for cid, metrics in rows if str(cid) in id_to_name
        }

        return profile, baseline_row_count


def persist_detection_report(
    engine: Engine,
    run_id: str,
    dataset_id: str,
    column_ids: dict[str, str],
    report: DetectionReport,
) -> None:
    save_signals(engine, run_id, dataset_id, column_ids, report.signals)


# ---------------------------------------------------------------------------
# Lineage graph (§9) — external_assets + lineage_edges kalıcılığı.
#
# `lineage.py` saf traversal mantığını taşır; burası yalnızca DB IO'dur:
# düğümleri kaydetmek, kenar eklemek (idempotent) ve tüm grafı BFS için
# belleğe çekmek.
# ---------------------------------------------------------------------------


def register_external_asset(
    engine: Engine, type_: str, name: str, owner: str | None = None
) -> str:
    """Dashboard/ML feature gibi backing tablosu olmayan terminal düğüm (§9.1)."""
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO external_assets (type, name, owner)
                VALUES (:type, :name, :owner)
                ON CONFLICT (type, name) DO UPDATE SET owner = EXCLUDED.owner
                RETURNING id
                """
            ),
            {"type": type_, "name": name, "owner": owner},
        ).first()
        return str(row[0])


def add_lineage_edge(
    engine: Engine,
    source_id: str,
    source_type: str,
    target_id: str,
    target_type: str,
    edge_type: str,
) -> None:
    """Bir lineage kenarı ekler; aynı üçlü (source, target, edge_type) zaten
    varsa no-op'tur (bkz. `idx_lineage_edges_unique`, §9.2 uygulama notu)."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO lineage_edges (source_id, source_type, target_id, target_type, edge_type)
                VALUES (:source_id, :source_type, :target_id, :target_type, :edge_type)
                ON CONFLICT (source_id, target_id, edge_type) DO NOTHING
                """
            ),
            {
                "source_id": source_id,
                "source_type": source_type,
                "target_id": target_id,
                "target_type": target_type,
                "edge_type": edge_type,
            },
        )


def fetch_all_edges(engine: Engine) -> list[Edge]:
    """Tüm lineage graph'ını tek sorguda belleğe çeker (küçük graf; BFS
    `lineage.py` içinde bellek üzerinde çalışır — bkz. o modülün docstring'i)."""
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT source_id, source_type, target_id, target_type, edge_type FROM lineage_edges")
        ).all()
    return [
        Edge(
            source_id=str(r[0]),
            source_type=r[1],
            target_id=str(r[2]),
            target_type=r[3],
            edge_type=r[4],
        )
        for r in rows
    ]


def resolve_node_name(engine: Engine, node_id: str, node_type: str) -> str:
    """Bir düğüm id'sini insan-okunabilir ada çevirir (rapor/CLI çıktısı için)."""
    with engine.begin() as conn:
        if node_type == "dataset":
            row = conn.execute(text("SELECT namespace, name FROM datasets WHERE id = :id"), {"id": node_id}).first()
            return f"{row[0]}.{row[1]}" if row else node_id
        if node_type == "job":
            row = conn.execute(text("SELECT namespace, name FROM jobs WHERE id = :id"), {"id": node_id}).first()
            return f"{row[0]}.{row[1]}" if row else node_id
        if node_type == "column":
            row = conn.execute(
                text(
                    """
                    SELECT d.name, c.name
                    FROM columns c JOIN datasets d ON d.id = c.dataset_id
                    WHERE c.id = :id
                    """
                ),
                {"id": node_id},
            ).first()
            return f"{row[0]}.{row[1]}" if row else node_id
        if node_type == "external_asset":
            row = conn.execute(text("SELECT name FROM external_assets WHERE id = :id"), {"id": node_id}).first()
            return row[0] if row else node_id
        return node_id


def find_column_id(engine: Engine, dataset_name: str, column_name: str) -> str | None:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT c.id
                FROM columns c
                JOIN datasets d ON d.id = c.dataset_id
                WHERE d.name = :dataset_name AND c.name = :column_name
                """
            ),
            {"dataset_name": dataset_name, "column_name": column_name},
        ).first()
        return str(row[0]) if row else None


def find_dataset_id(engine: Engine, dataset_name: str) -> str | None:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id FROM datasets WHERE name = :name"), {"name": dataset_name}).first()
        return str(row[0]) if row else None


def resolve_lineage_node(engine: Engine, dataset: str, column: str | None = None) -> tuple[str, str] | None:
    """Bir dataset/kolon adını lineage graph düğüm id'sine çözer.

    `sentinel lineage` CLI komutları ve `GET /api/v1/lineage/{assetId}`
    aynı çözümleme mantığını paylaşır (bkz. çağıranlardaki not-found
    işleme — CLI `typer.Exit`, API `HTTPException` fırlatır)."""
    if column:
        node_id = find_column_id(engine, dataset, column)
        return (node_id, "column") if node_id else None

    node_id = find_dataset_id(engine, dataset)
    return (node_id, "dataset") if node_id else None


def get_all_column_ids(engine: Engine, dataset_id: str) -> dict[str, str]:
    """Bir dataset için TÜM tarihsel kolon adlarını döner (`columns` hiç
    silinmez — bkz. §9 uygulama notu). `register_dataset(df)`'in döndürdüğü
    `column_ids` yalnızca O RUN'IN df'inde var olan kolonları içerir; bir
    kolon o run'da silinmişse (F01) `signals` tablosuna yazarken bu
    fonksiyonla geçmişten çözümlenmesi gerekir — aksi halde sinyalin
    `column_id`'si NULL kalır ve kolon adı geri okunamaz."""
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT name, id FROM columns WHERE dataset_id = :dataset_id"), {"dataset_id": dataset_id}
        ).all()
    return {name: str(cid) for name, cid in rows}


# ---------------------------------------------------------------------------
# Root Cause Agent — evidence gathering (§10.1 get_incident/get_schema_diff/
# compare_profiles araçlarının DB katmanı; `rca.py` bunu saf hipotez
# üretimine besler).
# ---------------------------------------------------------------------------


def get_latest_run(engine: Engine, dataset_name: str) -> dict[str, Any] | None:
    """Bir dataset için en son run'ı döner (job adı `<dataset>_etl` kuralına
    değil, WRITES_TO lineage kenarına dayanır — Faz 3 ile tutarlı)."""
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT jr.id, jr.started_at, jr.fault_id, jr.job_id
                FROM job_runs jr
                JOIN lineage_edges le ON le.source_id = jr.job_id AND le.edge_type = 'WRITES_TO'
                JOIN datasets d ON d.id = le.target_id AND le.target_type = 'dataset'
                WHERE d.name = :dataset_name
                ORDER BY jr.started_at DESC
                LIMIT 1
                """
            ),
            {"dataset_name": dataset_name},
        ).first()
    if row is None:
        return None
    return {"run_id": str(row[0]), "started_at": row[1], "fault_id": row[2], "job_id": str(row[3])}


def get_run_dataset_id(engine: Engine, run_id: str) -> str | None:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT le.target_id
                FROM job_runs jr
                JOIN lineage_edges le ON le.source_id = jr.job_id AND le.edge_type = 'WRITES_TO'
                WHERE jr.id = :run_id AND le.target_type = 'dataset'
                LIMIT 1
                """
            ),
            {"run_id": run_id},
        ).first()
    return str(row[0]) if row else None


def get_signals_for_run(engine: Engine, run_id: str) -> list[dict[str, Any]]:
    """Bir run'a ait tüm sinyalleri, kolon adı çözümlenmiş biçimde döner."""
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT s.id, s.type, c.name, s.severity, s.score, s.evidence, s.source
                FROM signals s
                LEFT JOIN columns c ON c.id = s.column_id
                WHERE s.run_id = :run_id
                ORDER BY s.score DESC
                """
            ),
            {"run_id": run_id},
        ).all()
    return [
        {
            "id": str(r[0]),
            "type": r[1],
            "column": r[2],
            "severity": r[3],
            "score": r[4],
            "evidence": r[5] or {},
            "source": r[6],
        }
        for r in rows
    ]


def get_schema_diff(engine: Engine, run_id: str, dataset_id: str) -> dict[str, list[str]]:
    """`columns` (tarihsel — hiç silinmez) ile bu run'ın `profiles`
    kayıtlarını karşılaştırarak schema diff çıkarır (§9.3 "get_schema_diff"):

    - dropped: dataset için daha önce bilinen ama bu run'ın profilinde
      olmayan kolonlar (F01 gibi bir kolon silme senaryosunu yakalar).
    - added: bu run'dan önce hiç profillenmemiş, ilk kez bu run'da görülen
      kolonlar.
    """
    with engine.begin() as conn:
        all_columns = conn.execute(
            text("SELECT id, name FROM columns WHERE dataset_id = :dataset_id"),
            {"dataset_id": dataset_id},
        ).all()
        profiled_ids = {
            str(r[0])
            for r in conn.execute(
                text("SELECT DISTINCT column_id FROM profiles WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).all()
        }
        earlier_profiled_ids = {
            str(r[0])
            for r in conn.execute(
                text(
                    """
                    SELECT DISTINCT p.column_id
                    FROM profiles p
                    JOIN job_runs jr ON jr.id = p.run_id
                    JOIN job_runs current_run ON current_run.id = :run_id
                    WHERE p.run_id != :run_id AND jr.started_at < current_run.started_at
                    """
                ),
                {"run_id": run_id},
            ).all()
        }

    dropped = [name for cid, name in all_columns if str(cid) not in profiled_ids]
    added = [name for cid, name in all_columns if str(cid) in profiled_ids and str(cid) not in earlier_profiled_ids]
    return {"dropped_columns": dropped, "added_columns": added}


def get_incident_context(engine: Engine, run_id: str) -> dict[str, Any]:
    """RCA'nın (`rca.py`) girdisi olan tam kanıt paketi."""
    dataset_id = get_run_dataset_id(engine, run_id)
    if dataset_id is None:
        raise ValueError(f"run_id {run_id} için dataset bulunamadı (lineage senkronize edilmemiş olabilir)")

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT namespace, name FROM datasets WHERE id = :id"), {"id": dataset_id}
        ).first()
        run_row = conn.execute(
            text("SELECT started_at, fault_id, job_id FROM job_runs WHERE id = :id"), {"id": run_id}
        ).first()

    return {
        "run_id": run_id,
        "dataset_id": dataset_id,
        "dataset_name": row[1] if row else None,
        "job_id": str(run_row[2]) if run_row else None,
        "started_at": run_row[0] if run_row else None,
        "fault_id": run_row[1] if run_row else None,
        "signals": get_signals_for_run(engine, run_id),
        "schema_diff": get_schema_diff(engine, run_id, dataset_id),
    }


def save_incident(
    engine: Engine,
    dataset_id: str,
    run_id: str,
    report: IncidentReport,
) -> str:
    """`incidents` tablosuna yazar (idempotent: run_id UNIQUE — tekrar
    analiz aynı run'ın incident'ını günceller)."""
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO incidents
                    (dataset_id, run_id, severity, summary, root_cause, impact, uncertainties, recommended_actions)
                VALUES
                    (:dataset_id, :run_id, :severity, :summary, CAST(:root_cause AS JSONB),
                     CAST(:impact AS JSONB), CAST(:uncertainties AS JSONB), CAST(:recommended_actions AS JSONB))
                ON CONFLICT (run_id) DO UPDATE SET
                    severity = EXCLUDED.severity,
                    summary = EXCLUDED.summary,
                    root_cause = EXCLUDED.root_cause,
                    impact = EXCLUDED.impact,
                    uncertainties = EXCLUDED.uncertainties,
                    recommended_actions = EXCLUDED.recommended_actions
                RETURNING id
                """
            ),
            {
                "dataset_id": dataset_id,
                "run_id": run_id,
                "severity": report.severity,
                "summary": report.summary,
                "root_cause": json.dumps([h.to_dict() for h in report.root_causes]),
                "impact": json.dumps(report.affected_assets),
                "uncertainties": json.dumps(report.uncertainties),
                "recommended_actions": json.dumps(report.recommended_actions),
            },
        ).first()
        return str(row[0])


def save_agent_run(
    engine: Engine,
    incident_id: str,
    evidence_count: int,
    llm_used: bool,
    model: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> str:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO agent_runs (incident_id, evidence_count, llm_used, model, metadata)
                VALUES (:incident_id, :evidence_count, :llm_used, :model, CAST(:metadata AS JSONB))
                RETURNING id
                """
            ),
            {
                "incident_id": incident_id,
                "evidence_count": evidence_count,
                "llm_used": llm_used,
                "model": model,
                "metadata": json.dumps(metadata or {}),
            },
        ).first()
        return str(row[0])


# ---------------------------------------------------------------------------
# Faz 5 (apps/api) — dashboard/API'nin ihtiyaç duyduğu okuma yardımcıları
# ---------------------------------------------------------------------------

_SEVERITY_RANK = {"critical": 3, "high": 2, "medium": 1, "low": 0}


def get_run_severity(engine: Engine, run_id: str) -> str:
    """Bir run'ın sinyallerinden en kötü severity'yi döner (sinyal yoksa 'low')."""
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT severity FROM signals WHERE run_id = :run_id"), {"run_id": run_id}).all()
    if not rows:
        return "low"
    return max((r[0] for r in rows), key=lambda s: _SEVERITY_RANK.get(s, 0))


def get_run_info(engine: Engine, run_id: str) -> dict[str, Any] | None:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT jr.id, jr.status, jr.started_at, jr.ended_at, jr.fault_id, jr.metadata,
                       le.target_id AS dataset_id, d.name AS dataset_name
                FROM job_runs jr
                LEFT JOIN lineage_edges le
                    ON le.source_id = jr.job_id AND le.edge_type = 'WRITES_TO' AND le.target_type = 'dataset'
                LEFT JOIN datasets d ON d.id = le.target_id
                WHERE jr.id = :run_id
                """
            ),
            {"run_id": run_id},
        ).first()
    if row is None:
        return None
    return {
        "run_id": str(row[0]),
        "status": row[1],
        "started_at": row[2],
        "ended_at": row[3],
        "fault_id": row[4],
        "metadata": row[5],
        "dataset_id": str(row[6]) if row[6] else None,
        "dataset_name": row[7],
    }


def list_datasets(engine: Engine) -> list[dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT id, namespace, name, type, owner FROM datasets ORDER BY name")).all()
    return [
        {"id": str(r[0]), "namespace": r[1], "name": r[2], "type": r[3], "owner": r[4]} for r in rows
    ]


def list_incidents(
    engine: Engine, dataset: str | None = None, severity: str | None = None, limit: int = 50
) -> list[dict[str, Any]]:
    clauses = []
    params: dict[str, Any] = {"limit": limit}
    if dataset:
        clauses.append("d.name = :dataset")
        params["dataset"] = dataset
    if severity:
        clauses.append("i.severity = :severity")
        params["severity"] = severity
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT i.id, d.name, i.run_id, i.status, i.severity, i.summary, i.created_at
                FROM incidents i
                JOIN datasets d ON d.id = i.dataset_id
                {where}
                ORDER BY i.created_at DESC
                LIMIT :limit
                """
            ),
            params,
        ).all()
    return [
        {
            "id": str(r[0]),
            "dataset": r[1],
            "run_id": str(r[2]),
            "status": r[3],
            "severity": r[4],
            "summary": r[5],
            "created_at": r[6],
        }
        for r in rows
    ]


def get_incident(engine: Engine, incident_id: str) -> dict[str, Any] | None:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT i.id, d.name, i.run_id, i.status, i.severity, i.summary,
                       i.root_cause, i.impact, i.uncertainties, i.recommended_actions, i.created_at
                FROM incidents i
                JOIN datasets d ON d.id = i.dataset_id
                WHERE i.id = :id
                """
            ),
            {"id": incident_id},
        ).first()
    if row is None:
        return None
    return {
        "id": str(row[0]),
        "dataset": row[1],
        "run_id": str(row[2]),
        "status": row[3],
        "severity": row[4],
        "summary": row[5],
        "root_causes": row[6],
        "affected_assets": row[7],
        "uncertainties": row[8],
        "recommended_actions": row[9],
        "created_at": row[10],
    }


# ---------------------------------------------------------------------------
# Faz 7 — Repair plan / approval workflow (§10.1, §14.3).
#
# `actions`, bir incident'ın `recommended_actions` taslaklarının stabil
# id'li kalıcı halidir (§6.1 API tasarımındaki `/actions/{id}` uçlarının
# hedeflediği kaynak). Hiçbir aksiyon burada otomatik uygulanmaz —
# yalnızca bir insan kararı (approve/reject) kaydedilir.
# ---------------------------------------------------------------------------


def sync_incident_actions(engine: Engine, incident_id: str, actions: list[dict[str, Any]]) -> None:
    """`rca.py`'nin ürettiği taslak aksiyonları `actions` tablosuna
    idempotent olarak yazar. Aynı (incident_id, type, description) zaten
    varsa dokunulmaz — böylece bir kez verilmiş approve/reject kararı,
    incident yeniden analiz edildiğinde (aynı hipotez aynı aksiyonu
    üretiyorsa) kaybolmaz."""
    with engine.begin() as conn:
        for action in actions:
            conn.execute(
                text(
                    """
                    INSERT INTO actions (incident_id, type, description, requires_approval)
                    VALUES (:incident_id, :type, :description, :requires_approval)
                    ON CONFLICT (incident_id, type, description) DO NOTHING
                    """
                ),
                {
                    "incident_id": incident_id,
                    "type": action["type"],
                    "description": action["description"],
                    "requires_approval": action["requires_approval"],
                },
            )


def list_actions_for_incident(engine: Engine, incident_id: str) -> list[dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, incident_id, type, description, requires_approval, status, created_at
                FROM actions
                WHERE incident_id = :incident_id
                ORDER BY created_at
                """
            ),
            {"incident_id": incident_id},
        ).all()
    return [
        {
            "id": str(r[0]),
            "incident_id": str(r[1]),
            "type": r[2],
            "description": r[3],
            "requires_approval": r[4],
            "status": r[5],
            "created_at": r[6],
        }
        for r in rows
    ]


def get_action(engine: Engine, action_id: str) -> dict[str, Any] | None:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, incident_id, type, description, requires_approval, status, created_at
                FROM actions
                WHERE id = :id
                """
            ),
            {"id": action_id},
        ).first()
    if row is None:
        return None
    return {
        "id": str(row[0]),
        "incident_id": str(row[1]),
        "type": row[2],
        "description": row[3],
        "requires_approval": row[4],
        "status": row[5],
        "created_at": row[6],
    }


def decide_action(
    engine: Engine, action_id: str, decision: str, actor: str, note: str | None = None
) -> dict[str, Any] | None:
    """Bir aksiyonu onaylar/reddeder ve audit kaydını (`approvals`) yazar.

    `decision` 'approve' ya da 'reject' olmalı. Aksiyon bulunamazsa None
    döner. Hiçbir SQL/dbt çalıştırılmaz — yalnızca durum değişir (§14.3).
    """
    if decision not in ("approve", "reject"):
        raise ValueError(f"Geçersiz decision: {decision!r} (approve|reject olmalı)")

    with engine.begin() as conn:
        exists = conn.execute(text("SELECT 1 FROM actions WHERE id = :id"), {"id": action_id}).first()
        if exists is None:
            return None

        conn.execute(
            text("INSERT INTO approvals (action_id, decision, actor, note) VALUES (:action_id, :decision, :actor, :note)"),
            {"action_id": action_id, "decision": decision, "actor": actor, "note": note},
        )
        new_status = "approved" if decision == "approve" else "rejected"
        conn.execute(
            text("UPDATE actions SET status = :status WHERE id = :id"),
            {"status": new_status, "id": action_id},
        )

    return get_action(engine, action_id)


def list_approvals_for_action(engine: Engine, action_id: str) -> list[dict[str, Any]]:
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT id, decision, actor, note, created_at FROM approvals "
                "WHERE action_id = :action_id ORDER BY created_at"
            ),
            {"action_id": action_id},
        ).all()
    return [
        {"id": str(r[0]), "decision": r[1], "actor": r[2], "note": r[3], "created_at": r[4]} for r in rows
    ]
