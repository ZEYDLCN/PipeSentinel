"""FastAPI bağımlılıkları."""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy import Engine

from pipeline_sentinel import db as db_module


@lru_cache
def _engine() -> Engine:
    return db_module.get_engine()


def get_engine() -> Engine:
    """FastAPI `Depends(get_engine)` için — process başına tek engine
    (SQLAlchemy engine'ler bağlantı havuzu tutar, her istek için yeniden
    oluşturulmamalıdır)."""
    return _engine()
