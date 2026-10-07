"""Real-source analysis, durable jobs, lineage context and evidence lifecycle."""
from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from . import baseline, reliability_store as store
from .anomaly import history_anomalies, multivariate_drift
from .connectors import get_connector, validate_source
from .contract_io import at_time, digest
from .detector import AnomalySignal, compare_row_counts, compute_incident_risk, run_detection, severity_from_score
from .ingestion import dbt_graph, openlineage_graph, reachable, reachable_columns
from .profiler import profile_dataset
from .rca import generate_report
from .repair import restore, sandbox, snapshot

RULE_VERSION = "reliability-2"
# Zamanlama ve otomatik baseline, taranan veriyi değiştirmez; bu alanlar değişince
# eski analizlerle karşılaştırılabilirlik (ve onarım onayı güncelliği) bozulmamalı.
OPERATIONAL_KEYS = {"schedule_minutes", "auto_baseline", "adaptive_detection", "multivariate"}


def config_digest(config):
    return digest({k: v for k, v in config.items() if k not in OPERATIONAL_KEYS})


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


def enqueue_job(engine, source_id, key, actor):
    """(iş, yeni_mi) döndürür; aynı kaynak/anahtar çifti mevcut işi döndürür."""
    if not key or len(key) > 160:
        raise ValueError("Idempotency anahtarı 1–160 karakter olmalı")
    try:
        with engine.begin() as conn:
            source = store.get(conn, store.sources, source_id)
            return store.insert(conn, store.jobs, source_id=source_id, key=key, actor=actor,
                                config=source["config"], status="pending", attempts=0), True
    except IntegrityError:
        with engine.connect() as conn:
            return store.rows(conn, store.jobs, (store.jobs.c.source_id == source_id) & (store.jobs.c.key == key), 1)[0], False


def enqueue(engine, source_id, key, actor):
    return enqueue_job(engine, source_id, key, actor)[0]


def enqueue_due(engine, now=None):
    """`schedule_minutes` tanımlı kaynaklar için vadesi gelen analizi kuyruğa alır.

    Anahtar zaman dilimine (slot) bağlıdır: birden fazla zamanlayıcı süreci aynı
    dilim için yalnızca bir iş üretir. Bitmemiş bir iş varsa yenisi eklenmez;
    yavaş kaynak için işler birikmez.
    """
    now = now or datetime.now(timezone.utc)
    with engine.connect() as conn:
        sources = store.rows(conn, store.sources, limit=1000)
    queued = []
    for source in sources:
        minutes = source["config"].get("schedule_minutes")
        if not minutes:
            continue
        with engine.connect() as conn:
            active = store.rows(conn, store.jobs, (store.jobs.c.source_id == source["id"]) &
                                store.jobs.c.status.in_(["pending", "running"]), 1)
        if active:
            continue
        slot = int(now.timestamp() // (minutes * 60))
        job, created = enqueue_job(engine, source["id"], f"schedule:{slot}", "scheduler")
        if created:
            queued.append(job)
    return queued


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
    return [h for h in history if h["body"].get("config_hash") == config_digest(config)]


def reference_frame(engine, history):
    """Çok değişkenli tespit için en yeni kabul edilmiş analizin saklanan veri kopyası (yoksa None)."""
    if not history:
        return None
    inline = history[0]["body"].get("snapshot")  # ayrı tabloya geçmeden önceki kayıtlar
    if inline is None:
        with engine.connect() as conn:
            stored = store.rows(conn, store.snapshots, store.snapshots.c.observation_id == history[0]["id"], 1)
        inline = stored[0]["data"] if stored else None
    return restore(inline) if inline is not None else None


def learned_staleness(column, profile, history, timestamp, minimum_runs=5):
    """Sıfır-konfigürasyon tazelik: kullanıcı SLA girmeden, kolonun geçmişteki "gecikme"
    düzenini (analiz anı − en yeni değer) öğrenir ve bundan belirgin sapmayı bildirir."""
    latest = profile.get(column, {}).get("max")
    if not latest:
        return None

    def lag(observed_at, newest):
        return (datetime.fromisoformat(observed_at) - datetime.fromisoformat(newest)).total_seconds() / 60

    lags = []
    for item in history[:30]:
        past = item["body"].get("profile", {}).get(column, {}).get("max")
        if past and item["body"].get("observed_at"):
            lags.append(lag(item["body"]["observed_at"], past))
    if len(lags) < minimum_runs:
        return None
    typical = median(lags)
    sigma = 1.4826 * median(abs(value - typical) for value in lags)
    threshold = max(typical + 6 * sigma, 2 * typical, typical + 15)
    current = lag(timestamp, latest)
    if current <= threshold:
        return None
    score = min(1.0, 0.5 + min(current / threshold - 1, 1.0) * 0.4)
    return AnomalySignal(
        "staleness", column, severity_from_score(score), score,
        {"message": f"{column} en yeni değeri {current:.0f} dk önce; öğrenilen olağan gecikme {typical:.0f} dk "
                    f"(eşik {threshold:.0f} dk, {len(lags)} analiz)",
         "method": "learned", "lag_minutes": round(current, 1), "typical_lag_minutes": round(typical, 1),
         "threshold_minutes": round(threshold, 1), "history_runs": len(lags), "latest_value": latest},
        "baseline")


def analyze_frame(frame, config, coverage, timestamp, history, run_id, reference_frame=None):
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
    if config.get("adaptive_detection", True):
        report.signals.extend(history_anomalies(
            profile, coverage, history, {(s.type, s.column) for s in report.signals},
            baseline.cohort if config.get("seasonal") else None, timestamp))
    if reference_frame is not None:
        joint = multivariate_drift(reference_frame, frame)
        if joint:
            report.signals.append(joint)
    for column in config.get("learned_freshness", []):
        stale = learned_staleness(column, profile, history, timestamp)
        if stale:
            report.signals.append(stale)
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
            "config_hash": config_digest(config), "rule_version": RULE_VERSION, "rule_fingerprint": rule_fingerprint(),
            "snapshot": snapshot(frame) if config.get("retain_snapshot") else None}


def graph_context(conn, asset):
    graphs = store.rows(conn, store.graphs, limit=100)
    edges = [edge for graph in graphs for edge in graph["body"]["edges"]]
    return edges, sorted(reachable(edges, asset)), sorted(reachable(edges, asset, reverse=True))


CHANGE_NOTE = "Zamansal örtüşme; bu değişikliklerin nedeni oldukları kanıtlanmadı (korelasyon, nedensellik değil)"


def change_candidates(events, asset, upstream, since):
    """`since`'den beri varlığı veya yukarı akışını etkileyen dbt/OpenLineage değişiklikleri.

    Bir analizdeki sinyalle aynı zaman penceresine düşen yukarı akış değişikliklerini
    sıralar. İlk içe aktarma (`initial`) değişiklik sayılmaz: tüm varlıklar yeni görünür.
    """
    scope = {asset: "self", **{name: "upstream" for name in upstream}}
    found = []
    for event in events:
        if event["kind"] not in {"dbt_ingested", "openlineage_ingested"} or event["created_at"] < since:
            continue
        body = event["body"]
        label = "definition_changed" if event["kind"] == "dbt_ingested" else "new_or_changed_node"
        common = {"at": event["created_at"], "event_id": event["id"], "namespace": body.get("namespace")}
        if not body.get("initial"):
            found += [{"asset": name, "relation": scope[name], "kind": label, **common}
                      for name in body.get("changed_assets", []) if name in scope]
        found += [{"asset": run["asset"], "relation": scope[run["asset"]], "kind": "run_failed",
                   "status": run.get("status"), **common}
                  for run in body.get("runs", [])
                  if run.get("asset") in scope and str(run.get("status", "")).lower() in {"error", "fail", "abort"}]
    return sorted(found, key=lambda c: c["at"], reverse=True)[:20]


def column_impact(conn, asset, signals):
    """Sinyalli kolonların yukarı/aşağı akıştaki kolon bağımlılıkları (kolon lineage'ı varsa)."""
    column_edges = [tuple(e) for graph in store.rows(conn, store.graphs, limit=100)
                    for e in graph["body"].get("column_edges", [])]
    if not column_edges:
        return []
    impact, seen = [], set()
    for signal in signals:
        column = signal.get("column")
        if not column or column.lower() in seen:
            continue
        seen.add(column.lower())
        downstream = reachable_columns(column_edges, asset, column)
        upstream = reachable_columns(column_edges, asset, column, reverse=True)
        if downstream or upstream:
            impact.append({"column": column, "upstream": [{"asset": a, "column": c} for a, c in upstream],
                           "downstream": [{"asset": a, "column": c} for a, c in downstream]})
    return impact[:20]


def business_impact(conn, severity, downstream, signals=()):
    policies = store.rows(conn, store.policies, limit=1000)
    affected = [{"asset": p["asset"], "owner": p["owner"], "criticality": p["criticality"],
                 "sla_minutes": p["sla_minutes"]} for p in policies if p["asset"] in downstream]
    severity_points = {"low": 10, "medium": 35, "high": 60, "critical": 80}[severity]
    business_points = min(20, sum(p["criticality"] * 4 for p in affected))
    lag = max((s["evidence"].get("lag_minutes", 0) for s in signals if s["type"] in {"freshness", "staleness"}), default=0)
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
        scanned = (connector or get_connector(job["config"])).scan(job["config"])
        observed_at = store.now()
        reference = reference_frame(engine, history) if job["config"].get("multivariate") else None
        body = analyze_frame(scanned.frame, job["config"], scanned.coverage, observed_at, history, job["id"], reference)
        # Ham veri kopyası ayrı tabloda tutulur: geçmiş/ilişkili analiz sorguları yalnızca
        # küçük özet gövdelerini okur, 12 MB'a varan kopyaları değil.
        snapshot_data = body.pop("snapshot", None)
        body["snapshot"], body["snapshot_available"] = None, snapshot_data is not None
        if len(json.dumps(body, default=str)) > 12_000_000 or (
                snapshot_data is not None and len(json.dumps(snapshot_data)) > 12_000_000):
            raise ValueError("Analiz sonucu 12 MB sınırını aştı; tarama kapsamını azaltın")
        with engine.begin() as conn:
            current = store.get(conn, store.jobs, job["id"])
            if current["status"] != "running" or current["attempts"] != job["attempts"] or current["lease_until"] < store.now():
                raise ValueError("İşin çalışma süresi doldu")
            asset = job["config"].get("lineage_asset") or f"{job['config']['schema']}.{job['config']['table']}"
            edges, downstream, upstream = graph_context(conn, asset)
            body.update(asset=asset, downstream=downstream, upstream=upstream, observed_at=observed_at)
            known_good = [h["created_at"] for h in history if h["id"] in body["baseline"]["ids"]]
            since = max(known_good) if known_good else (datetime.fromisoformat(observed_at) - timedelta(days=7)).isoformat()
            body["change_candidates"] = (change_candidates(store.rows(conn, store.events, limit=500), asset, upstream, since)
                                         if body["signals"] else [])
            body["change_candidates_note"] = CHANGE_NOTE
            body["column_impact"] = column_impact(conn, asset, body["signals"])
            body["impact"] = business_impact(conn, body["severity"], [asset, *downstream], body["signals"])
            body["rca"] = generate_report(source["name"], job["id"], body["signals"], {}, downstream).to_dict()
            related = store.rows(conn, store.observations, limit=200)
            window = (datetime.fromisoformat(observed_at) - timedelta(hours=1)).isoformat()
            candidates = [r for r in related if r["created_at"] >= window and r["body"]["signals"] and
                          (r["body"].get("asset") in {asset, *upstream, *downstream})]
            body["group_id"] = candidates[0]["body"].get("group_id", candidates[0]["id"]) if candidates else job["id"]
            body["group_reason"] = "same/connected asset within one hour; correlation, not proven cause"
            # Susturma yalnızca bildirimi bastırır; analiz, sinyaller ve kanıtlar aynen kaydedilir.
            active = [m for m in store.rows(conn, store.mutes, store.mutes.c.source_id == source["id"], 200)
                      if m["until"] > store.now()]
            body["mute_ids"] = sorted({m["id"] for s in body["signals"] for m in active if mute_matches(m, s)})
            body["muted"] = bool(body["signals"]) and all(any(mute_matches(m, s) for m in active) for s in body["signals"])
            observation = store.insert(conn, store.observations, source_id=source["id"], job_id=job["id"],
                                       quality="rejected" if body["signals"] else "candidate",
                                       contract_hash=digest(job["config"]["contract"]), body=body)
            if snapshot_data is not None:
                store.insert(conn, store.snapshots, observation_id=observation["id"], data=snapshot_data)
            conn.execute(update(store.jobs).where(store.jobs.c.id == job["id"]).values(
                status="succeeded", result_id=observation["id"], lease_until=None, error=None))
            store.event(conn, "analysis_completed", job["actor"], {"observation_id": observation["id"], "signals": len(body["signals"])}, source["id"])
            # Opt-in sürekli referans: yalnızca daha önce insan onaylı bir referans varken
            # (state == ready) ve hiç sinyal yokken; böylece büyüyen tablo eski hacimle kıyaslanmaz.
            if job["config"].get("auto_baseline") and not body["signals"] and body["baseline"]["state"] == "ready":
                _accept(conn, observation, "auto-baseline", "baseline_auto_accepted")
                observation["quality"] = "accepted"
            if body["signals"] and not body["muted"]:
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


def _accept(conn, item, actor, event="baseline_accepted", note=None):
    conn.execute(update(store.observations).where(store.observations.c.id == item["id"]).values(quality="accepted"))
    if not store.rows(conn, store.baselines, store.baselines.c.observation_id == item["id"], 1):
        store.insert(conn, store.baselines, source_id=item["source_id"], observation_id=item["id"], actor=actor)
    body = {"observation_id": item["id"], **({"note": note} if note else {})}
    store.event(conn, event, actor, body, item["source_id"])
    return {"quality": "accepted", "observation_id": item["id"]}


def accept_baseline(engine, observation_id, actor):
    with engine.begin() as conn:
        item = store.get(conn, store.observations, observation_id)
        if item["body"]["signals"]:
            raise ValueError("Sinyalli analiz baseline olarak kabul edilemez")
        return _accept(conn, item, actor)


# Yalnızca geçmişle karşılaştırmadan doğan sinyaller "iş değişimi" olabilir; sözleşme
# ihlali ise sözleşmenin (sürümlü olarak) güncellenmesi gereken bir durumdur.
EXPECTED_CHANGE_SOURCES = {"baseline", "segment"}


def accept_expected_change(engine, observation_id, note, actor):
    """Gerçek bir iş değişimini (kampanya, yeni pazar, fiyat değişimi) yeni referans yapar.

    Aksi halde baseline eski dağılımda kalır ve değişim sonrası her analiz sonsuza
    kadar alarm üretir. Gerekçe zorunludur ve denetim kaydına yazılır.
    """
    note = (note or "").strip()
    if not 10 <= len(note) <= 2000:
        raise ValueError("Beklenen değişim için 10–2000 karakterlik gerekçe notu gerekli")
    with engine.begin() as conn:
        item = store.get(conn, store.observations, observation_id)
        signals = item["body"]["signals"]
        if not signals:
            raise ValueError("Sinyalsiz analiz doğrudan baseline olarak kabul edilebilir")
        blocked = sorted({s["type"] for s in signals if s.get("source") not in EXPECTED_CHANGE_SOURCES})
        if blocked:
            raise ValueError("Sözleşme ihlali içeren analiz referans olamaz; sözleşmeyi güncelleyin: " + ", ".join(blocked))
        newest = store.rows(conn, store.observations, store.observations.c.source_id == item["source_id"], 1)
        if newest[0]["id"] != item["id"]:
            raise ValueError("Yalnızca kaynağın en son analizi beklenen değişim olarak kabul edilebilir")
        store.insert(conn, store.feedback, observation_id=observation_id, verdict="expected_change", note=note, actor=actor)
        return _accept(conn, item, actor, "expected_change_accepted", note)


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
            graph["column_edges"] = sorted({tuple(e) for e in old.get("column_edges", []) + graph["column_edges"]})
        changed = sorted(k for k in set(old["nodes"]) | set(graph["nodes"]) if old["nodes"].get(k) != graph["nodes"].get(k))
        if previous:
            conn.execute(update(store.graphs).where(store.graphs.c.id == previous[0]["id"]).values(body=graph))
        else:
            store.insert(conn, store.graphs, namespace=namespace, body=graph)
        store.event(conn, kind + "_ingested", actor, {"namespace": namespace, "changed_assets": changed,
                    "artifact_hash": graph["hash"], "runs": graph["runs"], "initial": not previous})
        return {"nodes": len(graph["nodes"]), "edges": len(graph["edges"]), "column_edges": len(graph["column_edges"]),
                "column_stats": graph.get("column_stats"), "changed_assets": changed, "coverage": graph["coverage"]}


def observation_detail(engine, observation_id):
    with engine.connect() as conn:
        item = store.get(conn, store.observations, observation_id)
        asset_set = {item["body"]["asset"], *item["body"]["upstream"], *item["body"]["downstream"]}
        timeline = store.rows(conn, store.events, limit=500)
        item["timeline"] = [e for e in timeline if e["source_id"] == item["source_id"] or asset_set.intersection(
                            e["body"].get("changed_assets", []) + [r.get("asset") for r in e["body"].get("runs", [])])]
        item["feedback"] = store.rows(conn, store.feedback, store.feedback.c.observation_id == observation_id)
        item["triage"] = triage_map(conn, [observation_id]).get(observation_id)
        item["mutes"] = [m for m in store.rows(conn, store.mutes, store.mutes.c.source_id == item["source_id"], 200)
                         if m["until"] > store.now()]
        item["repairs"] = store.rows(conn, store.repairs, store.repairs.c.observation_id == observation_id)
        # The snapshot is only consumed server-side; API does not export raw rows.
        item["body"] = deepcopy(item["body"])
        legacy_inline = item["body"].pop("snapshot", None) is not None  # ayrı tabloya geçmeden önceki kayıtlar
        item["body"]["snapshot_available"] = legacy_inline or bool(item["body"].get("snapshot_available"))
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
    if verdict not in {"correct_cause", "false_positive", "repair_failed", "resolved", "expected_change"} or len(note) > 2000:
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
    data = body.get("snapshot")
    if data is None:
        with engine.connect() as conn:
            stored = store.rows(conn, store.snapshots, store.snapshots.c.observation_id == observation_id, 1)
        data = stored[0]["data"] if stored else None
    if data is None:
        raise ValueError("Bu analizde snapshot saklanmamış; kaynakta retain_snapshot açıp yeni analiz başlatın")
    result = sandbox(data, body["contract"], action, body["observed_at"])
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
        if decision == "approve" and (newest[0]["id"] != item["id"] or config_digest(source["config"]) != item["body"]["config_hash"]):
            raise ValueError("Yeni analiz veya sözleşme var; güncel snapshot ile yeniden doğrulayın")
        status = "approved" if decision == "approve" else "rejected"
        result = conn.execute(update(store.repairs).where((store.repairs.c.id == repair_id) & (store.repairs.c.status == attempt["status"])).values(status=status))
        if result.rowcount != 1:
            raise ValueError("Eşzamanlı karar: kayıt değişti")
        store.event(conn, "repair_" + status, actor, {"repair_id": repair_id, "validation_hash": digest(attempt["body"]), "production_executed": False}, item["source_id"])
        return {"id": repair_id, "status": status, "production_executed": False}


TRIAGE_STATUSES = {"open", "acknowledged", "resolved"}


def set_triage(engine, observation_id, status, assignee, note, actor):
    """Olay yaşam döngüsü: açık → üstlenildi → çözüldü, sorumlu ve notla (denetim kaydıyla)."""
    if status not in TRIAGE_STATUSES:
        raise ValueError("Durum open, acknowledged veya resolved olmalı")
    assignee, note = (assignee or "").strip(), (note or "").strip()
    if len(assignee) > 160 or len(note) > 2000:
        raise ValueError("Sorumlu en fazla 160, not en fazla 2000 karakter olabilir")
    with engine.begin() as conn:
        item = store.get(conn, store.observations, observation_id)
        if not item["body"]["signals"]:
            raise ValueError("Sinyalsiz analiz için olay yönetimi yapılamaz")
        values = dict(status=status, assignee=assignee or None, note=note or None, actor=actor)
        existing = store.rows(conn, store.triage, store.triage.c.observation_id == observation_id, 1)
        if existing:
            conn.execute(update(store.triage).where(store.triage.c.id == existing[0]["id"]).values(**values))
        else:
            store.insert(conn, store.triage, observation_id=observation_id, **values)
        store.event(conn, "triage_changed", actor, {"observation_id": observation_id, "status": status,
                                                    "assignee": values["assignee"]}, item["source_id"])
        return {"observation_id": observation_id, **values}


def triage_map(conn, observation_ids):
    ids = list(observation_ids)
    if not ids:
        return {}
    return {t["observation_id"]: t for t in store.rows(conn, store.triage, store.triage.c.observation_id.in_(ids), len(ids))}


def mute_matches(mute, signal):
    return mute["signal_type"] == signal["type"] and mute["column_name"] in (None, signal.get("column"))


def add_mute(engine, source_id, signal_type, column, hours, reason, actor):
    """Tekrarlayan bir alarmı süreli sustur: bildirim gitmez, analiz ve kanıtlar kaydedilmeye devam eder."""
    reason = (reason or "").strip()
    if not signal_type or len(signal_type) > 60 or (column is not None and len(column) > 160):
        raise ValueError("Geçersiz sinyal türü veya kolon")
    if type(hours) is not int or not 1 <= hours <= 720:
        raise ValueError("Susturma süresi 1–720 saat olmalı")
    if not 5 <= len(reason) <= 500:
        raise ValueError("5–500 karakterlik gerekçe gerekli")
    until = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
    with engine.begin() as conn:
        store.get(conn, store.sources, source_id)
        mute = store.insert(conn, store.mutes, source_id=source_id, signal_type=signal_type, column_name=column or None,
                            until=until, reason=reason, actor=actor)
        store.event(conn, "mute_added", actor, {"mute_id": mute["id"], "signal_type": signal_type,
                                                "column": column or None, "until": until, "reason": reason}, source_id)
        return mute


def remove_mute(engine, mute_id, actor):
    with engine.begin() as conn:
        mute = store.get(conn, store.mutes, mute_id)
        conn.execute(update(store.mutes).where(store.mutes.c.id == mute_id).values(until=store.now()))
        store.event(conn, "mute_removed", actor, {"mute_id": mute_id}, mute["source_id"])
        return {"id": mute_id, "active": False}


def active_mutes(engine):
    with engine.connect() as conn:
        return [m for m in store.rows(conn, store.mutes, limit=500) if m["until"] > store.now()]


def metric_history(engine, source_id, limit=60):
    """Aynı yapılandırmadaki son analizlerden hacim ve kolon metriklerinin zaman serisi (eskiden yeniye)."""
    if type(limit) is not int or not 1 <= limit <= 200:
        raise ValueError("limit 1–200 olmalı")
    with engine.connect() as conn:
        store.get(conn, store.sources, source_id)
        rows = store.rows(conn, store.observations, store.observations.c.source_id == source_id, limit)
    if not rows:
        return {"points": [], "columns": []}
    newest_hash = rows[0]["body"].get("config_hash")
    points, columns = [], set()
    for row in reversed([r for r in rows if r["body"].get("config_hash") == newest_hash]):
        body = row["body"]
        cols = {name: {"null_ratio": p.get("null_ratio"), **({"mean": p["mean"]} if "mean" in p else {})}
                for name, p in body.get("profile", {}).items()}
        columns.update(name for name, p in cols.items() if "mean" in p)
        points.append({"id": row["id"], "at": row["created_at"], "rows": body["coverage"].get("total_rows"),
                       "signals": len(body["signals"]), "quality": row["quality"], "columns": cols})
    return {"points": points, "columns": sorted(columns)[:12]}


WORKER_TTL_SECONDS = 90  # worker her ~10 sn'de bir kalp atışı yazar
STALL_GRACE_MINUTES = 10


def heartbeat(engine, name, details=None):
    """Worker canlılık kaydı. Eski kayıtlar 7 günden sonra temizlenir."""
    timestamp = store.now()
    with engine.begin() as conn:
        existing = store.rows(conn, store.workers, store.workers.c.name == name, 1)
        if existing:
            conn.execute(update(store.workers).where(store.workers.c.id == existing[0]["id"])
                         .values(last_seen=timestamp, details=details))
        else:
            store.insert(conn, store.workers, name=name, last_seen=timestamp, started_at=timestamp, details=details)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        conn.execute(store.workers.delete().where(store.workers.c.last_seen < cutoff))


def monitoring_status(engine, now=None):
    """İzlemenin kendisinin sağlığı: worker canlı mı, zamanlanmış kaynaklar zamanında analiz ediliyor mu?

    `ok` | `degraded` (gecikmiş kaynak veya birikmiş iş) | `down` (iş var ama canlı worker yok).
    Worker çökerse webhook da gidemez; bu yüzden durum API ve `status` komutuyla dışarıdan izlenebilir.
    """
    now = now or datetime.now(timezone.utc)
    ttl = int(os.environ.get("SENTINEL_WORKER_TTL_SECONDS", WORKER_TTL_SECONDS))

    def age(value):
        return (now - datetime.fromisoformat(value)).total_seconds()

    with engine.connect() as conn:
        workers = store.rows(conn, store.workers, limit=50)
        sources = store.rows(conn, store.sources, limit=1000)
        pending = store.rows(conn, store.jobs, store.jobs.c.status == "pending", 500)
        running = store.rows(conn, store.jobs, store.jobs.c.status == "running", 500)
        last_success = {s["id"]: next(iter(store.rows(conn, store.jobs, (store.jobs.c.source_id == s["id"]) &
                                                       (store.jobs.c.status == "succeeded"), 1)), None) for s in sources}
    worker_rows = [{"name": w["name"], "last_seen": w["last_seen"], "age_seconds": round(age(w["last_seen"])),
                    "alive": age(w["last_seen"]) <= ttl} for w in workers]
    alive = any(w["alive"] for w in worker_rows)
    source_rows, stalled = [], []
    for source in sources:
        minutes = source["config"].get("schedule_minutes")
        job = last_success[source["id"]]
        entry = {"id": source["id"], "name": source["name"], "schedule_minutes": minutes,
                 "last_success_at": job["created_at"] if job else None, "state": "manual"}
        if minutes:
            limit = (2 * minutes + STALL_GRACE_MINUTES) * 60
            reference = job["created_at"] if job else source["created_at"]
            overdue = age(reference) > limit
            entry["age_minutes"] = round(age(reference) / 60, 1)
            entry["state"] = "stalled" if overdue else ("ok" if job else "waiting")
            if overdue:
                stalled.append(source["name"])
        source_rows.append(entry)
    oldest = max((age(j["created_at"]) for j in pending), default=0)
    backlog = oldest > 15 * 60
    scheduled = any(s["schedule_minutes"] for s in source_rows)
    if not alive and (pending or scheduled):
        status = "down"
    elif stalled or backlog:
        status = "degraded"
    else:
        status = "ok"
    notes = []
    if not alive:
        notes.append("Canlı worker yok" + ("; kuyruktaki işler ve zamanlanmış analizler bekliyor" if pending or scheduled else ""))
    if stalled:
        notes.append("Zamanında analiz edilmeyen kaynaklar: " + ", ".join(stalled))
    if backlog:
        notes.append(f"En eski bekleyen iş {oldest / 60:.0f} dakikadır işlenmedi")
    return {"status": status, "worker_alive": alive, "workers": worker_rows, "notes": notes,
            "queue": {"pending": len(pending), "running": len(running), "oldest_pending_seconds": round(oldest)},
            "sources": source_rows, "stalled": stalled, "checked_at": now.isoformat()}


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
