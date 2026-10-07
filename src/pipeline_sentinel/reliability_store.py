"""Portable metadata tables for the real-source workflow (PostgreSQL/SQLite)."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import Column, Float, Integer, JSON, MetaData, String, Table, Text, UniqueConstraint, select

metadata = MetaData()


def table(name, *columns, constraints=()):
    return Table("sentinel_" + name, metadata,
                 Column("id", String(36), primary_key=True),
                 Column("created_at", String(40), nullable=False), *columns, *constraints)


sources = table("sources", Column("name", String(160), unique=True, nullable=False),
                Column("config", JSON, nullable=False), Column("actor", String(160)))
contracts = table("contract_versions", Column("source_id", String(36), nullable=False),
                  Column("hash", String(64), nullable=False), Column("body", JSON, nullable=False),
                  constraints=(UniqueConstraint("source_id", "hash"),))
jobs = table("analysis_jobs", Column("source_id", String(36), nullable=False),
             Column("key", String(160), nullable=False), Column("status", String(20), nullable=False),
             Column("actor", String(160)), Column("attempts", Integer, nullable=False),
             Column("lease_until", String(40)), Column("error", Text), Column("result_id", String(36)),
             Column("config", JSON, nullable=False),
             constraints=(UniqueConstraint("source_id", "key"),))
observations = table("observations", Column("source_id", String(36), nullable=False),
                     Column("job_id", String(36), unique=True, nullable=False),
                     Column("quality", String(20), nullable=False), Column("contract_hash", String(64)),
                     Column("body", JSON, nullable=False))
snapshots = table("snapshots", Column("observation_id", String(36), unique=True, nullable=False),
                  Column("data", JSON, nullable=False))
workers = table("workers", Column("name", String(160), unique=True, nullable=False),
                Column("last_seen", String(40), nullable=False), Column("started_at", String(40)),
                Column("details", JSON))
triage = table("triage", Column("observation_id", String(36), unique=True, nullable=False),
               Column("status", String(20), nullable=False), Column("assignee", String(160)),
               Column("note", Text), Column("actor", String(160)))
mutes = table("mutes", Column("source_id", String(36), nullable=False), Column("signal_type", String(60), nullable=False),
              Column("column_name", String(160)), Column("until", String(40), nullable=False),
              Column("reason", Text), Column("actor", String(160)))
baselines = table("baseline_versions", Column("source_id", String(36), nullable=False),
                  Column("observation_id", String(36), unique=True, nullable=False), Column("actor", String(160)))
events = table("change_events", Column("kind", String(40)), Column("source_id", String(36)),
               Column("actor", String(160)), Column("body", JSON, nullable=False))
graphs = table("lineage", Column("namespace", String(160), unique=True, nullable=False), Column("body", JSON, nullable=False))
repairs = table("repair_attempts", Column("observation_id", String(36), nullable=False),
                Column("status", String(30)), Column("actor", String(160)), Column("body", JSON, nullable=False))
feedback = table("feedback", Column("observation_id", String(36), nullable=False),
                 Column("verdict", String(40)), Column("note", Text), Column("actor", String(160)))
policies = table("asset_policies", Column("asset", String(300), unique=True, nullable=False),
                 Column("owner", String(160)), Column("criticality", Integer), Column("sla_minutes", Integer))
notifications = table("notifications", Column("observation_id", String(36), unique=True, nullable=False),
                      Column("status", String(20)), Column("attempts", Integer), Column("error", Text),
                      Column("lease_until", String(40)),
                      Column("body", JSON, nullable=False))


def now():
    return datetime.now(timezone.utc).isoformat()


def insert(conn, target, **values):
    values = {"id": str(uuid4()), "created_at": now(), **values}
    conn.execute(target.insert().values(**values))
    return values


def get(conn, target, id):
    row = conn.execute(select(target).where(target.c.id == id)).mappings().first()
    if row is None:
        raise LookupError("Kayıt bulunamadı")
    return dict(row)


def rows(conn, target, condition=None, limit=100):
    query = select(target).order_by(target.c.created_at.desc(), target.c.id.desc()).limit(limit)
    if condition is not None:
        query = query.where(condition)
    return [dict(r) for r in conn.execute(query).mappings()]


def event(conn, kind, actor, body, source_id=None):
    return insert(conn, events, kind=kind, actor=actor, body=body, source_id=source_id)
