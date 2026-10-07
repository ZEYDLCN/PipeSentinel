"""Operator CLI for sources, durable worker, artifacts and PR data gates."""
from __future__ import annotations

import json
import os
import socket
import time
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import typer

from . import db, reliability as service, reliability_store as store
from .contract_io import load_contract
from .ci_report import render_markdown
from .contracts import check_contract
from .notifications import deliver_one
from .repair import data_diff
from .suggest import frame_from_csv, suggest_contract

app = typer.Typer(help="Gerçek kaynaklar, onarım doğrulaması ve CI veri kontrolü")


def emit(value):
    typer.echo(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def read_json(path):
    if path.stat().st_size > 20_000_000:
        raise typer.BadParameter("Dosya 20 MB sınırını aşıyor")
    return json.loads(path.read_text(encoding="utf-8"))


@app.command("validate-contract")
def validate_contract(path: Path):
    try:
        emit(load_contract(path.read_text(encoding="utf-8")))
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None


@app.command("suggest-contract")
def suggest_contract_command(csv: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True),
                             output: Path | None = None):
    """CSV örneğinden taslak YAML sözleşmesi çıkarır (kaydetmeden önce gözden geçirin)."""
    if csv.stat().st_size > 20_000_000:
        raise typer.BadParameter("CSV dosyası 20 MB sınırını aşıyor")
    try:
        text = suggest_contract(frame_from_csv(csv)).to_yaml()
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    typer.echo(text)


@app.command("source-add")
def source_add(name: str, config: Path, actor: str = "local-cli"):
    emit(service.register_source(db.get_engine(), name, read_json(config), actor))


@app.command("analyze")
def analyze(source_id: str, key: str, actor: str = "local-cli"):
    emit(service.enqueue(db.get_engine(), source_id, key, actor))


@app.command("worker")
def worker(once: bool = False, notifications: bool = False, schedule: bool = True):
    """Kuyruktaki işleri işler; schedule_minutes tanımlı kaynakları zamanında kuyruğa alır.

    --notifications HTTPS webhook gönderimini açar; --no-schedule zamanlayıcıyı kapatır.
    """
    engine = db.get_engine()
    last_tick = last_beat = float("-inf")
    name = f"{socket.gethostname()}:{os.getpid()}"
    while True:
        if time.monotonic() - last_beat >= 10:
            last_beat = time.monotonic()
            service.heartbeat(engine, name, {"schedule": schedule, "notifications": notifications})
        if schedule and time.monotonic() - last_tick >= 30:
            last_tick = time.monotonic()
            for job in service.enqueue_due(engine):
                emit({"scheduled_job": job["id"], "source_id": job["source_id"]})
        result = service.work_once(engine)
        if result:
            emit({k: v for k, v in result.items() if k != "body"})
        if notifications:
            delivered = deliver_one(engine)
            if delivered:
                emit(delivered)
        if once:
            break
        if not result:
            time.sleep(2)


@app.command("status")
def status(fail_on: str = typer.Option("degraded", help="Hangi durumda exit kodu sıfırdan farklı olsun: degraded | down")):
    """İzlemenin sağlığı: worker canlı mı, zamanlanmış kaynaklar zamanında analiz ediliyor mu?

    Çıkış kodu: 0 sağlıklı, 1 degraded, 2 down. Harici izleme (cron, uptime kontrolü) için;
    worker çökerse webhook da gönderilemez.
    """
    if fail_on not in {"degraded", "down"}:
        raise typer.BadParameter("--fail-on degraded veya down olmalı")
    report = service.monitoring_status(db.get_engine())
    emit(report)
    code = {"ok": 0, "degraded": 1, "down": 2}[report["status"]]
    if code and (fail_on == "degraded" or report["status"] == "down"):
        raise typer.Exit(code)


@app.command("baseline-accept")
def baseline_accept(observation_id: str, actor: str = "local-cli"):
    emit(service.accept_baseline(db.get_engine(), observation_id, actor))


@app.command("expected-change")
def expected_change(observation_id: str, note: str = typer.Option(..., help="Değişimin gerekçesi (10+ karakter)"),
                    actor: str = "local-cli"):
    """Sinyalli analizi gerçek bir iş değişimi olarak yeni referans yapar (gerekçe denetim kaydına yazılır)."""
    try:
        emit(service.accept_expected_change(db.get_engine(), observation_id, note, actor))
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None


@app.command("ingest-dbt")
def ingest_dbt(namespace: str, manifest: Path, run_results: Path | None = None, actor: str = "local-cli"):
    emit(service.ingest(db.get_engine(), namespace, read_json(manifest), actor, read_json(run_results) if run_results else None))


@app.command("ingest-openlineage")
def ingest_openlineage(namespace: str, event: Path, actor: str = "local-cli"):
    emit(service.ingest(db.get_engine(), namespace, read_json(event), actor, kind="openlineage"))


@app.command("replay")
def replay(observation_id: str):
    emit(service.replay(db.get_engine(), observation_id))


@app.command("repair")
def repair(observation_id: str, action: Path, actor: str = "local-cli"):
    emit(service.try_repair(db.get_engine(), observation_id, read_json(action), actor))


@app.command("ci-check")
def ci_check(before: Path, after: Path, contract: Path = typer.Option(...), keys: str = typer.Option(...), output: Path = Path("reports/data-quality.json"),
             max_change_ratio: float = 0.0, markdown: Path | None = typer.Option(None, help="PR/CI için Markdown özeti")):
    """CSV snapshot'larında veri farkı + sözleşme kontrolü; başarısızlıkta exit 1."""
    if max(before.stat().st_size, after.stat().st_size) > 20_000_000:
        raise typer.BadParameter("CSV dosyası 20 MB sınırını aşıyor")
    spec = load_contract(contract.read_text(encoding="utf-8"))
    frames = [pd.read_csv(path, nrows=100001) for path in (before, after)]
    if any(len(frame) > 100000 for frame in frames):
        raise typer.BadParameter("CI veri karşılaştırması en fazla 100000 satır")
    for frame in frames:
        for col, dtype in spec["expected_schema"].items():
            if col in frame and dtype == "datetime":
                frame[col] = pd.to_datetime(frame[col], utc=True, errors="raise")
    diff = data_diff(*frames, [key.strip() for key in keys.split(",")], max_change_ratio)
    violations = check_contract(frames[1], spec)
    report = {"diff": diff, "violations": [asdict(v) for v in violations], "passed": diff["passed"] and not violations}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    if markdown:
        markdown.parent.mkdir(parents=True, exist_ok=True)
        markdown.write_text(render_markdown(report), encoding="utf-8")
    emit(report)
    if not report["passed"]:
        raise typer.Exit(1)
