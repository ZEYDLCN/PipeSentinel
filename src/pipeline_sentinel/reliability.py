"""Real-source analysis, durable jobs, lineage context and evidence lifecycle."""
from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from . import baseline, reliability_store as store
from .connectors import PostgresConnector, validate_source
from .contract_io import at_time, digest
from .detector import AnomalySignal, compare_row_counts, compute_incident_risk, run_detection, severity_from_score
from .ingestion import dbt_graph, openlineage_graph, reachable
from .profiler import profile_dataset
from .rca import generate_report
from .repair import sandbox, snapshot

RULE_VERSION = "reliability-1"


def rule_fingerprint():
    return digest({name: Path(__file__).with_name(name + ".py").read_text(encoding="utf-8")
                   for name in ("rca", "detector", "contracts", "baseline")})


def register_source(engine, name, config, actor):
    if not isinstance(name, str) or not 1 <= len(name) <= 160:
        raise ValueError("Kaynak adı 1–160 karakter olmalı")
    config = validate_source(config)
    with engine.begin() as conn:
        existing = store.rows(conn, store.sources, store.sources.c.name == name, 1)
        if existing:
            source = existing[0]
            conn.execute(update(store.sources).where(store.sources.c.id == source["id"]).values(config=config, actor=actor))
            source["config"] = config
        else:
            source = store.insert(conn, store.sources, name=name, config=config, actor=actor)
        h = digest(config["contract"])
        if not store.rows(conn, store.contracts, (store.contracts.c.source_id == source["id"]) & (store.contracts.c.hash == h), 1):
            store.insert(conn, store.contracts, source_id=source["id"], hash=h, body=config["contract"])
        store.event(conn, "source_configured", actor, {"contract_hash": h, "config_hash": digest(config)}, source["id"])
        return source


def enqueue(engine, source_id, key, actor):
    if not key or len(key) > 160:
        raise ValueError("Idempotency anahtarı 1–160 karakter olmalı")
    try:
        with engine.begin() as conn:
            source = store.get(conn, store.sources, source_id)
            return store.insert(conn, store.jobs, source_id=source_id, key=key, actor=actor,
                                config=source["config"], status="pending", attempts=0)
    except IntegrityError:
        with engine.connect() as conn:
            return store.rows(conn, store.jobs, (store.jobs.c.source_id == source_id) & (store.jobs.c.key == key), 1)[0]


def retry_job(engine, job_id, actor):
    with engine.begin() as conn:
        job = store.get(conn, store.jobs, job_id)
        if job["status"] != "failed" or job["attempts"] >= 3:
            raise ValueError("Yalnızca başarısız işler en fazla üç denemeye kadar tekrarlanabilir")
        conn.execute(update(store.jobs).where((store.jobs.c.id == job_id) & (store.jobs.c.status == "failed"))
                     .values(status="pending", error=None, lease_until=None))
        store.event(conn, "job_retried", actor, {"job_id": job_id}, job["source_id"])


def claim(engine):
    timestamp = store.now()
    with engine.begin() as conn:
        expired = (store.jobs.c.status == "running") & (store.jobs.c.lease_until < timestamp)
        conn.execute(update(store.jobs).where(expired).values(status="failed", error="İş zaman aşımı; tekrar denenebilir"))
        query = select(store.jobs).where(store.jobs.c.status == "pending").order_by(store.jobs.c.created_at).limit(1)
        if engine.dialect.name == "postgresql":
            query = query.with_for_update(skip_locked=True)
        job = conn.execute(query).mappings().first()
        if not job:
            return None
        job = dict(job)
        # Lease covers two bounded source queries and local analysis.
        lease = (datetime.now(timezone.utc) + timedelta(seconds=job["config"]["timeout_seconds"] * 2 + 180)).isoformat()
        result = conn.execute(update(store.jobs).where((store.jobs.c.id == job["id"]) & (store.jobs.c.status == "pending"))
                              .values(status="running", attempts=job["attempts"] + 1, lease_until=lease))
        if result.rowcount != 1:
            return None
        job.update(status="running", attempts=job["attempts"] + 1, lease_until=lease)
        return job


def history_for(conn, source_id, config):
    history = store.rows(conn, store.observations, (store.observations.c.source_id == source_id) &
                         (store.observations.c.quality == "accepted"), 500)
    # Comparing different scan budgets, partitions or contract versions is misleading.
    return [h for h in history if h["body"].get("config_hash") == digest(config)]


def analyze_frame(frame, config, coverage, timestamp, history, run_id):
    profile = profile_dataset(frame)
    selected = baseline.choose(history, timestamp, config.get("seasonal", False))
    report = run_detection(config["table"], frame, at_time(config["contract"], datetime.fromisoformat(timestamp)),
                           profile, selected["profile"], None)
    if frame.empty:
        report.signals.append(AnomalySignal("empty_dataset", None, "high", .8, {"row_count": 0}, "contract"))
    if selected["row_count"] is not None:
        signal = compare_row_counts(selected["row_count"], coverage["total_rows"])
        if signal:
            report.signals.append(signal)
    segments = baseline.segment_profiles(frame, config.get("segment_by", []))
    if selected["state"] == "ready":
        report.signals.extend(baseline.compare_segments(selected["segments"], segments))
    sensitive = set(config.get("sensitive_columns", []))
    for column in sensitive:
        if column in profile:
            profile[column].pop("top_values", None)
            profile[column].pop("sample_hash", None)
    for group in segments.values():
        for column in sensitive:
            if column in group["profile"]:
                group["profile"][column].pop("top_values", None)
                group["profile"][column].pop("sample_hash", None)
    signals = []
    for i, signal in enumerate(report.signals):
        item = signal.to_dict()
        item["id"] = f"{run_id}:{i}"
        if signal.column in sensitive:
            item["evidence"] = {k: v for k, v in item["evidence"].items() if k not in
                                {"sample", "sample_value", "unexpected_values", "allowed_values", "message"}}
        signals.append(item)
    risk = compute_incident_risk(report.signals)
    return {"profile": profile, "signals": signals, "risk_score": risk,
            "severity": severity_from_score(risk) if signals else "low", "segments": segments,
            "baseline": selected, "coverage": coverage, "contract": config["contract"],
            "config_hash": digest(config), "rule_version": RULE_VERSION, "rule_fingerprint": rule_fingerprint(),
            "snapshot": snapshot(frame) if config.get("retain_snapshot") else None}


def graph_context(conn, asset):
    graphs = store.rows(conn, store.graphs, limit=100)
    edges = [edge for graph in graphs for edge in graph["body"]["edges"]]
    return edges, sorted(reachable(edges, asset)), sorted(reachable(edges, asset, reverse=True))


def business_impact(conn, severity, downstream, signals=()):
    policies = store.rows(conn, store.policies, limit=1000)
    affected = [{"asset": p["asset"], "owner": p["owner"], "criticality": p["criticality"],
                 "sla_minutes": p["sla_minutes"]} for p in policies if p["asset"] in downstream]
    severity_points = {"low": 10, "medium": 35, "high": 60, "critical": 80}[severity]
    business_points = min(20, sum(p["criticality"] * 4 for p in affected))
    lag = max((s["evidence"].get("lag_minutes", 0) for s in signals if s["type"] == "freshness"), default=0)
    breached = [p["asset"] for p in affected if lag > p["sla_minutes"]]
    sla_points = 5 if breached else 0
    return {"priority": min(100, severity_points + business_points + sla_points),
            "components": {"severity": severity_points, "asset_criticality": business_points, "sla": sla_points}, "sla_breaches": breached,
            "assets": affected, "interpretation": "potential downstream impact; not confirmed corruption"}


def work_once(engine, connector=None):
    job = claim(engine)
    if job is None:
        return None
    try:
        with engine.connect() as conn:
            source = store.get(conn, store.sources, job["source_id"])
            history = history_for(conn, source["id"], job["config"])
        scanned = (connector or PostgresConnector()).scan(job["config"])
        observed_at = store.now()
        body = analyze_frame(scanned.frame, job["config"], scanned.coverage, observed_at, history, job["id"])
        if len(json.dumps(body, default=str)) > 12_000_000:
            raise ValueError("Analiz sonucu 12 MB sınırını aştı; tarama kapsamını azaltın")
        with engine.begin() as conn:
            current = store.get(conn, store.jobs, job["id"])
            if current["status"] != "running" or current["attempts"] != job["attempts"] or current["lease_until"] < store.now():
                raise ValueError("İşin çalışma süresi doldu")
            asset = job["config"].get("lineage_asset") or f"{job['config']['schema']}.{job['config']['table']}"
            edges, downstream, upstream = graph_context(conn, asset)
            body.update(asset=asset, downstream=downstream, upstream=upstream, observed_at=observed_at)
            body["impact"] = business_impact(conn, body["severity"], [asset, *downstream], body["signals"])
            body["rca"] = generate_report(source["name"], job["id"], body["signals"], {}, downstream).to_dict()
            related = store.rows(conn, store.observations, limit=200)
            window = (datetime.fromisoformat(observed_at) - timedelta(hours=1)).isoformat()
            candidates = [r for r in related if r["created_at"] >= window and r["body"]["signals"] and
                          (r["body"].get("asset") in {asset, *upstream, *downstream})]
            body["group_id"] = candidates[0]["body"].get("group_id", candidates[0]["id"]) if candidates else job["id"]
            body["group_reason"] = "same/connected asset within one hour; correlation, not proven cause"
            observation = store.insert(conn, store.observations, source_id=source["id"], job_id=job["id"],
                                       quality="rejected" if body["signals"] else "candidate",
                                       contract_hash=digest(job["config"]["contract"]), body=body)
            conn.execute(update(store.jobs).where(store.jobs.c.id == job["id"]).values(
                status="succeeded", result_id=observation["id"], lease_until=None, error=None))
            store.event(conn, "analysis_completed", job["actor"], {"observation_id": observation["id"], "signals": len(body["signals"])}, source["id"])
            if body["signals"]:
                store.insert(conn, store.notifications, observation_id=observation["id"], status="pending", attempts=0,
                             body={"observation_id": observation["id"], "source": source["name"],
                                   "severity": body["severity"], "signal_count": len(body["signals"]),
                                   "priority": body["impact"]["priority"]})
            return observation
    except Exception as exc:
        # Database exceptions can contain credentials, SQL and row values. Never persist their text.
        error = str(exc) if isinstance(exc, ValueError) else f"Analiz başarısız ({type(exc).__name__}); bağlantı ve kaynak yapılandırmasını kontrol edin"
        with engine.begin() as conn:
            conn.execute(update(store.jobs).where((store.jobs.c.id == job["id"]) &
                         (store.jobs.c.attempts == job["attempts"]) & (store.jobs.c.status == "running"))
                         .values(status="failed", error=error[:500], lease_until=None))
        return {"job_id": job["id"], "status": "failed", "error": error[:500]}


def accept_baseline(engine, observation_id, actor):
    with engine.begin() as conn:
        item = store.get(conn, store.observations, observation_id)
        if item["body"]["signals"]:
            raise ValueError("Sinyalli analiz baseline olarak kabul edilemez")
        conn.execute(update(store.observations).where(store.observations.c.id == observation_id).values(quality="accepted"))
        if not store.rows(conn, store.baselines, store.baselines.c.observation_id == observation_id, 1):
            store.insert(conn, store.baselines, source_id=item["source_id"], observation_id=observation_id, actor=actor)
        store.event(conn, "baseline_accepted", actor, {"observation_id": observation_id}, item["source_id"])
        return {"quality": "accepted", "observation_id": observation_id}


def ingest(engine, namespace, payload, actor, results=None, kind="dbt"):
    if not namespace or len(namespace) > 160:
        raise ValueError("Lineage namespace 1–160 karakter olmalı")
    graph = dbt_graph(payload, results) if kind == "dbt" else openlineage_graph(payload)
    with engine.begin() as conn:
        previous = store.rows(conn, store.graphs, store.graphs.c.namespace == namespace, 1)
        old = previous[0]["body"] if previous else {"nodes": {}, "edges": []}
        if kind == "openlineage":
            graph["nodes"] = {**old["nodes"], **graph["nodes"]}
            graph["edges"] = sorted({tuple(e) for e in old["edges"] + graph["edges"]})
        changed = sorted(k for k in set(old["nodes"]) | set(graph["nodes"]) if old["nodes"].get(k) != graph["nodes"].get(k))
        if previous:
            conn.execute(update(store.graphs).where(store.graphs.c.id == previous[0]["id"]).values(body=graph))
        else:
            store.insert(conn, store.graphs, namespace=namespace, body=graph)
        store.event(conn, kind + "_ingested", actor, {"namespace": namespace, "changed_assets": changed,
                    "artifact_hash": graph["hash"], "runs": graph["runs"]})
        return {"nodes": len(graph["nodes"]), "edges": len(graph["edges"]), "changed_assets": changed, "coverage": graph["coverage"]}


def observation_detail(engine, observation_id):
    with engine.connect() as conn:
        item = store.get(conn, store.observations, observation_id)
        asset_set = {item["body"]["asset"], *item["body"]["upstream"], *item["body"]["downstream"]}
        timeline = store.rows(conn, store.events, limit=500)
        item["timeline"] = [e for e in timeline if e["source_id"] == item["source_id"] or asset_set.intersection(
                            e["body"].get("changed_assets", []) + [r.get("asset") for r in e["body"].get("runs", [])])]
        item["feedback"] = store.rows(conn, store.feedback, store.feedback.c.observation_id == observation_id)
        item["repairs"] = store.rows(conn, store.repairs, store.repairs.c.observation_id == observation_id)
        # The snapshot is only consumed server-side; API does not export raw rows.
        item["body"] = deepcopy(item["body"])
        item["body"]["snapshot_available"] = item["body"].pop("snapshot", None) is not None
        item["similar_incidents"] = similar_incidents(conn, item)
        return item


def similar_incidents(conn, item):
    signature = {(s["type"], s.get("column")) for s in item["body"]["signals"]}
    if not signature:
        return []
    feedbacks = store.rows(conn, store.feedback, limit=500)
    latest = {f["observation_id"]: f for f in reversed(feedbacks)}
    resolved = {id: f for id, f in latest.items() if f["verdict"] == "resolved"}
    result = []
    for old in store.rows(conn, store.observations, store.observations.c.source_id == item["source_id"], 200):
        if old["id"] == item["id"] or old["id"] not in resolved or old["contract_hash"] != item["contract_hash"]:
            continue
        other = {(s["type"], s.get("column")) for s in old["body"]["signals"]}
        overlap = len(signature & other) / len(signature | other)
        if overlap:
            result.append({"observation_id": old["id"], "similarity": overlap,
                           "resolution": resolved[old["id"]]["note"], "automatic_approval": False})
    return sorted(result, key=lambda r: r["similarity"], reverse=True)[:5]


def record_feedback(engine, observation_id, verdict, note, actor):
    if verdict not in {"correct_cause", "false_positive", "repair_failed", "resolved"} or len(note) > 2000:
        raise ValueError("Geçersiz geri bildirim veya not çok uzun")
    with engine.begin() as conn:
        item = store.get(conn, store.observations, observation_id)
        result = store.insert(conn, store.feedback, observation_id=observation_id, verdict=verdict, note=note, actor=actor)
        store.event(conn, "feedback_recorded", actor, {"observation_id": observation_id, "verdict": verdict}, item["source_id"])
        return result


def replay(engine, observation_id):
    with engine.connect() as conn:
        item = store.get(conn, store.observations, observation_id)
    body = item["body"]
    if body["rule_version"] != RULE_VERSION or body.get("rule_fingerprint") != rule_fingerprint():
        raise ValueError("Kural sürümü değişmiş; eski sürümle replay gerekli")
    original = body["rca"]
    report = generate_report(original["dataset"], item["job_id"], body["signals"], {}, body["downstream"]).to_dict()
    return {"matches": report == original, "scope": "recorded signal evidence to deterministic RCA",
            "rule_version": RULE_VERSION, "evidence_hash": digest(body["signals"]), "report": report}


def try_repair(engine, observation_id, action, actor):
    with engine.connect() as conn:
        item = store.get(conn, store.observations, observation_id)
    body = item["body"]
    if body.get("snapshot") is None:
        raise ValueError("Bu analizde snapshot saklanmamış; kaynakta retain_snapshot açıp yeni analiz başlatın")
    result = sandbox(body["snapshot"], body["contract"], action, body["observed_at"])
    result["source_coverage"] = body["coverage"]
    with engine.begin() as conn:
        attempt = store.insert(conn, store.repairs, observation_id=observation_id, actor=actor,
                               status="validated" if result["passed"] else "failed", body={"action": action, "validation": result})
        store.event(conn, "repair_validated", actor, {"observation_id": observation_id, "repair_id": attempt["id"], "passed": result["passed"]}, item["source_id"])
        return attempt


def decide_repair(engine, repair_id, decision, actor):
    if decision not in {"approve", "reject"}:
        raise ValueError("approve/reject gerekli")
    with engine.begin() as conn:
        attempt = store.get(conn, store.repairs, repair_id)
        if attempt["status"] not in {"validated", "failed"}:
            raise ValueError("Bu onarım için karar zaten verilmiş")
        if decision == "approve" and not attempt["body"]["validation"]["passed"]:
            raise ValueError("Doğrulanmamış onarım onaylanamaz")
        item = store.get(conn, store.observations, attempt["observation_id"])
        source = store.get(conn, store.sources, item["source_id"])
        newest = store.rows(conn, store.observations, store.observations.c.source_id == item["source_id"], 1)
        if decision == "approve" and (newest[0]["id"] != item["id"] or digest(source["config"]) != item["body"]["config_hash"]):
            raise ValueError("Yeni analiz veya sözleşme var; güncel snapshot ile yeniden doğrulayın")
        status = "approved" if decision == "approve" else "rejected"
        result = conn.execute(update(store.repairs).where((store.repairs.c.id == repair_id) & (store.repairs.c.status == attempt["status"])).values(status=status))
        if result.rowcount != 1:
            raise ValueError("Eşzamanlı karar: kayıt değişti")
        store.event(conn, "repair_" + status, actor, {"repair_id": repair_id, "validation_hash": digest(attempt["body"]), "production_executed": False}, item["source_id"])
        return {"id": repair_id, "status": status, "production_executed": False}


def set_policy(engine, asset, owner, criticality, sla_minutes, actor):
    if not asset or len(asset) > 300 or not owner or len(owner) > 160 or not 1 <= criticality <= 5 or sla_minutes < 1:
        raise ValueError("Varlık, sahip, 1–5 kritiklik ve pozitif SLA gerekli")
    with engine.begin() as conn:
        values = dict(asset=asset, owner=owner, criticality=criticality, sla_minutes=sla_minutes)
        existing = store.rows(conn, store.policies, store.policies.c.asset == asset, 1)
        if existing:
            conn.execute(update(store.policies).where(store.policies.c.id == existing[0]["id"]).values(**values))
        else:
            store.insert(conn, store.policies, **values)
        store.event(conn, "asset_policy_changed", actor, values)
        return values


def metrics(engine):
    """Observed operational outcomes, not an invented model-accuracy estimate."""
    from statistics import median
    with engine.connect() as conn:
        observations = store.rows(conn, store.observations, limit=1000)
        feedback = store.rows(conn, store.feedback, limit=5000)
        repairs = store.rows(conn, store.repairs, limit=1000)
    latest = {f["observation_id"]: f for f in reversed(feedback)}
    reviewed = [latest[o["id"]] for o in observations if o["id"] in latest]
    durations = [o["body"]["coverage"].get("duration_seconds", 0) for o in observations]
    return {"scope": "most recent 1000 observations and repair attempts", "observations": len(observations),
            "incidents": sum(bool(o["body"]["signals"]) for o in observations), "reviewed": len(reviewed),
            "false_positive_reviews": sum(f["verdict"] == "false_positive" for f in reviewed),
            "resolved_reviews": sum(f["verdict"] == "resolved" for f in reviewed),
            "median_scan_seconds": median(durations) if durations else None,
            "repair_attempts": len(repairs), "validated_repairs": sum(r["body"]["validation"]["passed"] for r in repairs),
            "accuracy": None, "accuracy_note": "Accuracy/recall require separately labelled evaluation data"}
