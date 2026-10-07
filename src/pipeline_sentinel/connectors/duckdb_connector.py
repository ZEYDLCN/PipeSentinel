"""Salt okunur DuckDB connector (yerel `.duckdb` dosyası; dbt-duckdb, Parquet/CSV görünümleri).

`connection_env` ortam değişkeni veritabanı dosyasının yolunu içerir. Dosya `read_only=True` ile
açılır, tablo adı allowlist'te olmalıdır, serbest SQL kabul edilmez ve sorgu süresi
`timeout_seconds` ile sınırlıdır (zaman aşımında bağlantı kesilir).
"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import pandas as pd

from .postgres import ScanResult, identifier, validate_source


class DuckDBConnector:
    def scan(self, config: dict) -> ScanResult:
        config = validate_source(config)
        try:
            import duckdb
        except ImportError:
            raise ValueError('DuckDB için paket gerekli: pip install "pipeline-sentinel[duckdb]"') from None
        path = os.environ.get(config["connection_env"])
        if not path:
            raise ValueError("Kaynak bağlantı ortam değişkeni tanımlı değil")
        if "://" in path or not Path(path).is_file():
            raise ValueError("DuckDB bağlantı değişkeni mevcut bir veritabanı dosyasının yolunu içermeli")
        relation = f"{identifier(config['schema'])}.{identifier(config['table'])}"
        params: list = []
        where = ""
        if config.get("partition"):
            part = config["partition"]
            kind = config["contract"]["expected_schema"][part["column"]]
            bounds = [part["start"], part["end"]]
            params = [pd.Timestamp(v).to_pydatetime() for v in bounds] if kind == "datetime" else bounds
            where = f" WHERE {identifier(part['column'])} >= ? AND {identifier(part['column'])} < ?"
        order = " ORDER BY " + ", ".join(identifier(c) for c in config["order_by"]) if config["order_by"] else ""
        started = time.monotonic()
        connection = duckdb.connect(path, read_only=True)
        timer = threading.Timer(config["timeout_seconds"], connection.interrupt)
        timer.start()
        try:
            count = int(connection.execute(f"SELECT count(*) FROM {relation}{where}", params).fetchone()[0])
            frame = connection.execute(f"SELECT * FROM {relation}{where}{order} LIMIT ?",
                                       [*params, config["max_rows"] + 1]).df()
        except duckdb.InterruptException:
            raise ValueError(f"Sorgu {config['timeout_seconds']} saniyelik süre sınırını aştı") from None
        finally:
            timer.cancel()
            connection.close()
        truncated = len(frame) > config["max_rows"]
        frame = frame.head(config["max_rows"])
        return ScanResult(frame, {
            "scope": "partition" if where else "table", "method": "ordered_prefix" if order else "unordered_prefix",
            "total_rows": count, "scanned_rows": len(frame), "complete": not truncated,
            "partition": config.get("partition"), "duration_seconds": round(time.monotonic() - started, 3),
            "max_rows": config["max_rows"], "timeout_seconds": config["timeout_seconds"],
            "limitation": None if not truncated else "Kalite kontrolleri yalnızca sınırlı örneği kapsar; hacim tam sayımdır."})
