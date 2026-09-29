"""Operator CLI for sources, durable worker, artifacts and PR data gates."""
from __future__ import annotations

import json
import time
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import typer

from . import db, reliability as service, reliability_store as store
from .contract_io import load_contract
from .contracts import check_contract
from .notifications import deliver_one
from .repair import data_diff

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


@app.command("source-add")
def source_add(name: str, config: Path, actor: str = "local-cli"):
    emit(service.register_source(db.get_engine(), name, read_json(config), actor))


@app.command("analyze")
def analyze(source_id: str, key: str, actor: str = "local-cli"):
    emit(service.enqueue(db.get_engine(), source_id, key, actor))


@app.command("worker")
def worker(once: bool = False, notifications: bool = False):
    """Kuyruktaki işleri işler; --notifications HTTPS webhook gönderimini açar."""
    engine = db.get_engine()
    while True:
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


@app.command("baseline-accept")
def baseline_accept(observation_id: str, actor: str = "local-cli"):
    emit(service.accept_baseline(db.get_engine(), observation_id, actor))


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
             max_change_ratio: float = 0.0):
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
    emit(report)
    if not report["passed"]:
        raise typer.Exit(1)
