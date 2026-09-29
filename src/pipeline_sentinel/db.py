"""SQLAlchemy engine ve migration runner.

MVP kural: `infra/migrations/*.sql` dosyaları dosya adı sırasına göre,
idempotent (``CREATE ... IF NOT EXISTS``) olarak çalıştırılır. Ayrı bir
migration framework (alembic vb.) Faz 2+ için bırakılmıştır — bkz.
docs/pipeline-sentinel-design.md §20.1.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine, text

from .config import get_settings

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "infra" / "migrations"


def get_engine(database_url: str | None = None) -> Engine:
    url = database_url or get_settings().database_url
    return create_engine(url, future=True)


def run_migrations(engine: Engine) -> list[str]:
    """infra/migrations altındaki .sql dosyalarını sırayla uygular.

    Returns
    -------
    Uygulanan dosya adlarının listesi.
    """
    applied: list[str] = []
    sql_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(f"Migration bulunamadı: {MIGRATIONS_DIR}")

    with engine.begin() as conn:
        for path in sql_files:
            sql = path.read_text(encoding="utf-8")
            conn.execute(text(sql))
            applied.append(path.name)
        from .reliability_store import metadata
        metadata.create_all(conn)
        applied.append("reliability_metadata_v1")
    return applied


def reset_schema(engine: Engine) -> None:
    """Geliştirme kolaylığı: demo şemalarını sıfırlar (yalnızca yerel/dev)."""
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS commerce CASCADE"))
        conn.execute(
            text(
                "DROP TABLE IF EXISTS signals, profiles, contracts, "
                "lineage_edges, job_runs, jobs, columns, datasets CASCADE"
            )
        )
