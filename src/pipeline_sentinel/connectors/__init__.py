"""Bounded, read-only source connectors."""
from .postgres import PostgresConnector, ScanResult, validate_source

__all__ = ["PostgresConnector", "ScanResult", "validate_source"]
