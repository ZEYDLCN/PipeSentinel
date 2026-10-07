"""Bounded, read-only source connectors.

Yeni bir connector eklemek için `scan(config) -> ScanResult` sağlayan bir sınıf yazın,
`postgres.CONNECTOR_TYPES` listesine türünü ekleyin ve `get_connector`'a bağlayın.
Salt okunur erişimi sunucu tarafında zorlayın (hesap/rol/oturum); istemci tarafı kontrol yetmez.
"""
from .postgres import CONNECTOR_TYPES, PostgresConnector, ScanResult, validate_source


def get_connector(config: dict):
    """`config["type"]` (varsayılan postgresql) için connector örneği."""
    kind = config.get("type", "postgresql")
    if kind == "postgresql":
        return PostgresConnector()
    if kind == "duckdb":
        from .duckdb_connector import DuckDBConnector
        return DuckDBConnector()
    raise ValueError("type: " + " veya ".join(CONNECTOR_TYPES))


__all__ = ["CONNECTOR_TYPES", "PostgresConnector", "ScanResult", "get_connector", "validate_source"]
