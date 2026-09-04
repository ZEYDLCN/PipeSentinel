"""Ortam yapılandırması.

Tek kaynak: ``DATABASE_URL`` ortam değişkeni. ``infra/docker-compose.yml``
ve ``infra/.env.example`` bu değeri tanımlar.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_DATABASE_URL = (
    "postgresql+psycopg://sentinel:sentinel@localhost:5432/pipeline_sentinel"
)


@dataclass(frozen=True)
class Settings:
    database_url: str


def get_settings() -> Settings:
    return Settings(
        database_url=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL),
    )
