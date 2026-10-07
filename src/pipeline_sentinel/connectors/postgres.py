from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Protocol

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from ..contract_io import validate_contract


def identifier(value):
    if not isinstance(value, str) or not value or len(value) > 128 or "\x00" in value:
        raise ValueError("Geçersiz SQL tanımlayıcısı")
    return '"' + value.replace('"', '""') + '"'


CONNECTOR_TYPES = ("postgresql", "duckdb")


def validate_source(config: dict) -> dict:
    """Bütün connector türleri için ortak kaynak doğrulaması (`type` yoksa postgresql)."""
    allowed = {"connection_env", "schema", "table", "allowlist", "max_rows", "timeout_seconds",
               "contract", "partition", "order_by", "segment_by", "seasonal", "retain_snapshot", "sensitive_columns", "lineage_asset",
               "schedule_minutes", "auto_baseline", "learned_freshness", "type", "adaptive_detection", "multivariate"}
    if not isinstance(config, dict) or set(config) - allowed:
        raise ValueError("Kaynak yapılandırmasında bilinmeyen alan")
    if config.get("type", "postgresql") not in CONNECTOR_TYPES:
        raise ValueError("type: " + " veya ".join(CONNECTOR_TYPES))
    result = {"schema": "public", "max_rows": 10000, "timeout_seconds": 30,
              "order_by": [], "segment_by": [], "sensitive_columns": [], "seasonal": False,
              "retain_snapshot": False, **config}
    if not re.fullmatch(r"SENTINEL_SOURCE_[A-Z0-9_]+", result.get("connection_env", "")):
        raise ValueError("Bağlantı değişkeni SENTINEL_SOURCE_ ile başlamalı")
    for key in ("schema", "table"):
        identifier(result.get(key))
    if not isinstance(result.get("allowlist"), list) or f"{result['schema']}.{result['table']}" not in result["allowlist"]:
        raise ValueError("Tablo açık allowlist içinde olmalı")
    for key, lo, hi in (("max_rows", 1, 100000), ("timeout_seconds", 1, 120)):
        if type(result[key]) is not int or not lo <= result[key] <= hi:
            raise ValueError(f"{key}: {lo}–{hi} arası tam sayı gerekli")
    result["contract"] = validate_contract(result.get("contract"))
    schema = result["contract"]["expected_schema"]
    for key in ("order_by", "segment_by", "sensitive_columns"):
        if not isinstance(result[key], list) or any(col not in schema for col in result[key]):
            raise ValueError(f"{key}: sözleşmedeki kolonlardan oluşmalı")
    if len(result["segment_by"]) > 2:
        raise ValueError("En fazla iki segment kolonu")
    if set(result["segment_by"]) & set(result["sensitive_columns"]):
        raise ValueError("Hassas kolon segment anahtarı olamaz")
    for key in ("seasonal", "retain_snapshot"):
        if type(result[key]) is not bool:
            raise ValueError(f"{key}: boolean gerekli")
    schedule = result.get("schedule_minutes")
    if schedule is not None and (type(schedule) is not int or not 5 <= schedule <= 10080):
        raise ValueError("schedule_minutes: 5–10080 arası tam sayı gerekli")
    for key in ("auto_baseline", "adaptive_detection", "multivariate"):
        if type(result.get(key, False)) is not bool:
            raise ValueError(f"{key}: boolean gerekli")
    if result.get("multivariate") and not result["retain_snapshot"]:
        raise ValueError("multivariate için retain_snapshot açık olmalı (referans veri kopyası gerekir)")
    learned = result.get("learned_freshness", [])
    if (not isinstance(learned, list) or len(learned) > 10
            or any(col not in schema or schema[col] != "datetime" for col in learned)):
        raise ValueError("learned_freshness: sözleşmedeki datetime kolonlarından oluşan en fazla 10 kolon")
    if result["sensitive_columns"] and result["retain_snapshot"]:
        raise ValueError("Hassas kolon içeren kaynakta snapshot saklama kapalı olmalı")
    part = result.get("partition")
    if part is not None:
        if not isinstance(part, dict) or set(part) != {"column", "start", "end"} or part["column"] not in schema:
            raise ValueError("partition: column/start/end gerekli")
        if schema[part["column"]] not in {"datetime", "integer", "float"}:
            raise ValueError("Partition zaman veya sayısal kolon olmalı")
        start, end = part["start"], part["end"]
        if schema[part["column"]] == "datetime":
            start, end = pd.Timestamp(start), pd.Timestamp(end)
        elif type(start) not in (int, float) or type(end) not in (int, float):
            raise ValueError("Sayısal partition sınırları gerekli")
        if not start < end:
            raise ValueError("partition.start < end gerekli")
    return result


@dataclass
class ScanResult:
    frame: pd.DataFrame
    coverage: dict


class Connector(Protocol):
    def scan(self, config: dict) -> ScanResult: ...


class PostgresConnector:
    def scan(self, config: dict) -> ScanResult:
        config = validate_source(config)
        url = os.environ.get(config["connection_env"])
        if not url:
            raise ValueError("Kaynak bağlantı ortam değişkeni tanımlı değil")
        parsed = make_url(url)
        if parsed.get_backend_name() != "postgresql":
            raise ValueError("Yalnızca PostgreSQL bağlantısı desteklenir")
        engine = create_engine(parsed.set(drivername="postgresql+psycopg"), connect_args={"connect_timeout": 5})
        started = time.monotonic()
        relation = f"{identifier(config['schema'])}.{identifier(config['table'])}"
        params = {"limit": config["max_rows"] + 1}
        where = ""
        if config.get("partition"):
            p = config["partition"]
            where = f" WHERE {identifier(p['column'])} >= :start AND {identifier(p['column'])} < :end"
            params.update(start=p["start"], end=p["end"])
        order = " ORDER BY " + ", ".join(identifier(c) for c in config["order_by"]) if config["order_by"] else ""
        try:
            with engine.connect() as conn, conn.begin():
                conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                conn.execute(text("SELECT set_config('statement_timeout', :timeout, true)"), {"timeout": str(config["timeout_seconds"] * 1000)})
                conn.execute(text("SELECT set_config('lock_timeout', '2000', true)"))
                # Exact partition volume is computed at the source, without loading the table.
                count = int(conn.execute(text(f"SELECT count(*) FROM {relation}{where}"), params).scalar_one())
                df = pd.read_sql_query(text(f"SELECT * FROM {relation}{where}{order} LIMIT :limit"), conn, params=params)
            truncated = len(df) > config["max_rows"]
            df = df.head(config["max_rows"])
            return ScanResult(df, {"scope": "partition" if where else "table", "method": "ordered_prefix" if order else "unordered_prefix",
                                  "total_rows": count, "scanned_rows": len(df), "complete": not truncated,
                                  "partition": config.get("partition"), "duration_seconds": round(time.monotonic() - started, 3),
                                  "max_rows": config["max_rows"], "timeout_seconds": config["timeout_seconds"],
                                  "limitation": None if not truncated else "Kalite kontrolleri yalnızca sınırlı örneği kapsar; hacim tam sayımdır."})
        finally:
            engine.dispose()
