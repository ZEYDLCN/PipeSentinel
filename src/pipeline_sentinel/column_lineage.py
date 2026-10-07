"""dbt'nin derlenmiş SQL'inden kolon seviyesinde bağımlılık çıkarımı (en iyi çaba, sqlglot).

Her kenar `[kaynak_varlık, kaynak_kolon, hedef_varlık, hedef_kolon]` biçimindedir; varlıklar dbt
`unique_id`'leridir, kolon adları küçük harfe çevrilir. Çıkarım yalnızca manifest'teki varlıklara
çözülebilen ilişkileri içerir; çözülemeyen yıldızlar (`select *` ve şema yok), desteklenmeyen SQL ve
zaman bütçesi aşımı sayılar halinde `stats`'ta raporlanır, bağımlılık uydurulmaz.
"""
from __future__ import annotations

import time

ADAPTER_DIALECTS = {"postgres": "postgres", "snowflake": "snowflake", "bigquery": "bigquery", "duckdb": "duckdb",
                    "redshift": "redshift", "databricks": "databricks", "spark": "spark", "mysql": "mysql",
                    "sqlserver": "tsql", "trino": "trino"}
MAX_MODELS = 2000
MAX_COLUMNS_PER_MODEL = 200
MAX_SQL_CHARS = 200_000
TIME_BUDGET_SECONDS = 30.0


def available() -> bool:
    try:
        import sqlglot  # noqa: F401
    except ImportError:
        return False
    return True


def _keys(catalog, db, name) -> list[tuple]:
    parts = tuple((part or "").lower() for part in (catalog, db, name))
    return [parts, parts[1:], parts[2:]]


def dbt_column_edges(nodes: dict, adapter: str | None) -> tuple[list[list[str]], dict]:
    """`nodes`: manifest düğümleri (kaynaklar dahil). Dönüş: (kenarlar, istatistikler)."""
    import sqlglot
    from sqlglot import exp
    from sqlglot.lineage import lineage

    dialect = ADAPTER_DIALECTS.get((adapter or "").lower())
    stats = {"models": 0, "columns": 0, "edges": 0, "skipped_columns": 0, "unresolved_tables": 0,
             "unresolved_columns": 0, "truncated": False, "dialect": dialect}

    def parse_relation(name: str):
        try:
            table = sqlglot.parse_one(name, into=exp.Table, dialect=dialect)
            return table.catalog, table.db, table.name
        except Exception:
            parts = [p.strip('"`[]') for p in name.split(".")]
            return ("", *parts[-2:]) if len(parts) >= 2 else ("", "", parts[-1])

    relations: dict[tuple, set[str]] = {}
    full_schema: dict = {}
    for uid, node in nodes.items():
        name = node.get("relation_name")
        if not isinstance(name, str) or not name:
            continue
        catalog, db, table = parse_relation(name)
        for key in _keys(catalog, db, table):
            relations.setdefault(key, set()).add(uid)
        columns = node.get("columns")
        if catalog and db and table and isinstance(columns, dict) and columns:
            full_schema.setdefault(catalog, {}).setdefault(db, {})[table] = {str(c).lower(): "UNKNOWN" for c in columns}

    def resolve(table) -> str | None:
        for key in _keys(table.catalog, table.db, table.name):
            candidates = relations.get(key)
            if candidates and len(candidates) == 1:
                return next(iter(candidates))
        return None

    edges: set[tuple[str, str, str, str]] = set()
    started = time.monotonic()
    for uid, node in nodes.items():
        sql = node.get("compiled_code") or node.get("compiled_sql")
        if node.get("resource_type") not in {"model", "snapshot"} or not isinstance(sql, str) or not sql.strip():
            continue
        if stats["models"] >= MAX_MODELS or time.monotonic() - started > TIME_BUDGET_SECONDS:
            stats["truncated"] = True
            break
        if len(sql) > MAX_SQL_CHARS:
            stats["skipped_columns"] += 1
            continue
        stats["models"] += 1
        try:
            from sqlglot.optimizer.qualify import qualify
            names = [s.alias_or_name for s in qualify(sqlglot.parse_one(sql, dialect=dialect), schema=full_schema or None,
                                                      dialect=dialect, validate_qualify_columns=False).selects]
        except Exception:
            stats["skipped_columns"] += 1
            continue
        for name in [n for n in names if n][:MAX_COLUMNS_PER_MODEL]:
            if name == "*":
                stats["skipped_columns"] += 1
                continue
            # Kısmi şemada, şemada olmayan tablonun nitelendirilmemiş kolonları çözülemez (yer tutucu yaprak);
            # bu durumda şemasız ikinci deneme (tek tabloluysa sqlglot kolonu tabloya bağlar) yapılır.
            tree = None
            for attempt in ([full_schema, None] if full_schema else [None]):
                try:
                    candidate = lineage(name, sql, schema=attempt, dialect=dialect)
                except Exception:
                    continue
                tree = candidate
                if not any(isinstance(n.source, exp.Placeholder) for n in candidate.walk()):
                    break
            if tree is None:
                stats["skipped_columns"] += 1
                continue
            stats["columns"] += 1
            if any(isinstance(n.source, exp.Placeholder) for n in tree.walk()):
                stats["unresolved_columns"] += 1
            for leaf in tree.walk():
                if not isinstance(leaf.source, exp.Table):
                    continue
                upstream = resolve(leaf.source)
                column = leaf.name.rsplit(".", 1)[-1].lower()
                if upstream is None:
                    stats["unresolved_tables"] += 1
                elif column and column != "*" and upstream != uid:
                    edges.add((upstream, column, uid, name.lower()))
    stats["edges"] = len(edges)
    return [list(e) for e in sorted(edges)], stats
