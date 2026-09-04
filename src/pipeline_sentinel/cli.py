"""Pipeline Sentinel AI — Faz 1 + Faz 3 + Faz 4 + Faz 7 CLI.

    sentinel migrate                    # şemayı kur
    sentinel bootstrap --runs 14        # sağlıklı baseline geçmişi oluştur (§8.1)
    sentinel run                        # yeni (sağlıklı) pipeline run'ı çalıştır
    sentinel run --fault F05            # kontrollü hata enjekte ederek çalıştır
    sentinel faults                     # F01-F06 kataloğunu listele
    sentinel report --dataset orders    # en son run'ın tespit raporunu göster
    sentinel lineage graph              # tüm lineage graph'ını listele (§9)
    sentinel lineage impact --dataset orders --column total_amount
    sentinel lineage upstream --dataset payments --column amount
    sentinel analyze --dataset orders   # Root Cause Agent: kanıtlı kök neden raporu (§10-§12)
    sentinel actions list --incident <id>          # önerilen aksiyonları listele (§14.3)
    sentinel actions approve <action_id> --actor ben
    sentinel actions reject <action_id> --actor ben --note "..."
"""

from __future__ import annotations

import typer
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.table import Table

from . import db as db_module
from . import lineage as lineage_module
from . import llm as llm_module
from . import pipeline as pipeline_module
from .analyze import analyze_run
from .config import get_settings
from .detector import DetectionReport
from .faults import FAULT_CATALOG
from .orchestrator import RunOutcome, execute_pipeline_run

app = typer.Typer(add_completion=False, help="Pipeline Sentinel AI — Faz 1 + Faz 3 + Faz 4 + Faz 7 CLI")
lineage_app = typer.Typer(add_completion=False, help="Lineage graph komutları (§9)")
app.add_typer(lineage_app, name="lineage")
actions_app = typer.Typer(add_completion=False, help="Önerilen aksiyonlar / approval workflow (§14.3)")
app.add_typer(actions_app, name="actions")
console = Console()


@app.command()
def migrate() -> None:
    """infra/migrations altındaki SQL dosyalarını uygular."""
    engine = db_module.get_engine()
    applied = db_module.run_migrations(engine)
    console.print(f"[green]Uygulanan migration'lar:[/green] {', '.join(applied)}")


@app.command()
def reset(yes: bool = typer.Option(False, "--yes", help="Onayı atla")) -> None:
    """Demo şemasını sıfırlar (yalnızca yerel geliştirme)."""
    if not yes:
        typer.confirm("commerce şeması ve metadata tabloları silinecek. Emin misin?", abort=True)
    engine = db_module.get_engine()
    db_module.reset_schema(engine)
    applied = db_module.run_migrations(engine)
    console.print(f"[yellow]Şema sıfırlandı ve yeniden kuruldu:[/yellow] {', '.join(applied)}")


@app.command()
def faults() -> None:
    """F01-F06 kontrollü hata kataloğunu listeler (§16.1)."""
    table = Table(title="Kontrollü Hata Kataloğu")
    table.add_column("ID")
    table.add_column("Dataset")
    table.add_column("Açıklama")
    for fault_id, entry in FAULT_CATALOG.items():
        table.add_row(fault_id, entry["dataset"], entry["label"])
    console.print(table)


def _print_reports(outcome: RunOutcome) -> None:
    if outcome.fault_id:
        console.print(f"[bold red]Enjekte edilen hata:[/bold red] {outcome.fault_id}")

    table = Table(title="Tespit Raporu")
    table.add_column("Dataset")
    table.add_column("Run ID", overflow="fold")
    table.add_column("Risk Skoru")
    table.add_column("Severity")
    table.add_column("Sinyal Sayısı")

    for dataset, report in outcome.reports.items():
        severity_color = {
            "critical": "bold red",
            "high": "red",
            "medium": "yellow",
            "low": "green",
        }.get(report.severity, "white")
        table.add_row(
            dataset,
            outcome.run_ids[dataset],
            f"{report.risk_score:.2f}",
            f"[{severity_color}]{report.severity}[/{severity_color}]",
            str(len(report.signals)),
        )
    console.print(table)

    for dataset, report in outcome.reports.items():
        if not report.signals:
            continue
        _print_signal_detail(dataset, report)


def _print_signal_detail(dataset: str, report: DetectionReport) -> None:
    table = Table(title=f"{dataset} — sinyal detayı")
    table.add_column("Tip")
    table.add_column("Kolon")
    table.add_column("Skor")
    table.add_column("Kaynak")
    table.add_column("Mesaj")
    for signal in sorted(report.signals, key=lambda s: s.score, reverse=True):
        table.add_row(
            signal.type,
            signal.column or "-",
            f"{signal.score:.2f}",
            signal.source,
            str(signal.evidence.get("message", "")),
        )
    console.print(table)


@app.command()
def run(
    fault: str = typer.Option(None, "--fault", help="Enjekte edilecek fault_id (F01-F06)"),
    customers: int = typer.Option(50, help="Bu run'da üretilecek yeni müşteri sayısı"),
    orders: int = typer.Option(500, help="Bu run'da üretilecek sipariş sayısı"),
    seed: int = typer.Option(None, help="Determinizm için rastgelelik tohumu"),
) -> None:
    """Tek bir pipeline run'ı çalıştırır: üret → yükle → profille → tespit et."""
    if fault and fault not in FAULT_CATALOG:
        console.print(f"[red]Bilinmeyen fault_id: {fault}. Geçerli: {sorted(FAULT_CATALOG)}[/red]")
        raise typer.Exit(code=1)

    engine = db_module.get_engine()
    outcome = execute_pipeline_run(engine, fault_id=fault, n_customers=customers, n_orders=orders, seed=seed)
    _print_reports(outcome)


@app.command()
def bootstrap(
    runs: int = typer.Option(14, help="Baseline için çalıştırılacak sağlıklı run sayısı (§8.1)"),
    customers: int = typer.Option(50, help="Run başına yeni müşteri sayısı (varsayılan `run` ile aynı)"),
    orders: int = typer.Option(500, help="Run başına sipariş sayısı (varsayılan `run` ile aynı)"),
) -> None:
    """Baseline oluşturmak için art arda sağlıklı (hatasız) run'lar çalıştırır.

    Not: `volume` sinyali baseline'a göre satır sayısı değişimini ölçtüğü
    için burada kullanılan `--customers`/`--orders` değerlerini sonraki
    `sentinel run` çağrılarıyla tutarlı tutmak, sahte volume alarmını önler.
    """
    engine = db_module.get_engine()
    with console.status(f"{runs} sağlıklı run çalıştırılıyor..."):
        for i in range(runs):
            execute_pipeline_run(engine, fault_id=None, n_customers=customers, n_orders=orders, seed=1000 + i)
    console.print(f"[green]{runs} sağlıklı run tamamlandı. Baseline hazır.[/green]")


@app.command()
def report(
    dataset: str = typer.Option(..., help="orders | customers | payments"),
) -> None:
    """Verilen dataset için en son kaydedilmiş sinyalleri gösterir."""
    from sqlalchemy import text

    engine = db_module.get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT jr.id, jr.started_at, jr.fault_id
                FROM job_runs jr
                JOIN jobs j ON j.id = jr.job_id
                WHERE j.name = :job_name
                ORDER BY jr.started_at DESC
                LIMIT 1
                """
            ),
            {"job_name": f"{dataset}_etl"},
        ).first()
        if row is None:
            console.print(f"[yellow]{dataset} için henüz run bulunamadı. Önce `sentinel run` çalıştırın.[/yellow]")
            raise typer.Exit(code=1)

        run_id, started_at, fault_id = row
        signal_rows = conn.execute(
            text(
                """
                SELECT type, c.name, score, severity, evidence
                FROM signals s
                LEFT JOIN columns c ON c.id = s.column_id
                WHERE s.run_id = :run_id
                ORDER BY score DESC
                """
            ),
            {"run_id": str(run_id)},
        ).all()

    console.print(f"[bold]Run:[/bold] {run_id}  [bold]Zaman:[/bold] {started_at}  [bold]Fault:[/bold] {fault_id or '-'}")
    if not signal_rows:
        console.print("[green]Sinyal yok — anomali tespit edilmedi.[/green]")
        return

    table = Table(title=f"{dataset} — sinyaller")
    table.add_column("Tip")
    table.add_column("Kolon")
    table.add_column("Skor")
    table.add_column("Severity")
    table.add_column("Mesaj")
    for rtype, column, score, severity, evidence in signal_rows:
        table.add_row(rtype, column or "-", f"{score:.2f}", severity, str((evidence or {}).get("message", "")))
    console.print(table)


@app.command()
def settings() -> None:
    """Aktif yapılandırmayı gösterir."""
    console.print(get_settings())


def _resolve_node(engine, dataset: str, column: str | None) -> tuple[str, str]:
    resolved = pipeline_module.resolve_lineage_node(engine, dataset, column)
    if resolved is None:
        label = f"{dataset}.{column}" if column else dataset
        console.print(f"[red]Bulunamadı: {label}[/red]")
        raise typer.Exit(code=1)
    return resolved


@lineage_app.command("graph")
def lineage_graph() -> None:
    """Tüm lineage graph'ını (kenar listesi) yazdırır (§9.2)."""
    engine = db_module.get_engine()
    edges = pipeline_module.fetch_all_edges(engine)
    if not edges:
        console.print("[yellow]Lineage graph boş. Önce `sentinel run` veya `sentinel bootstrap` çalıştırın.[/yellow]")
        return

    table = Table(title="Lineage graph")
    table.add_column("Source")
    table.add_column("Edge")
    table.add_column("Target")
    for e in edges:
        source_name = pipeline_module.resolve_node_name(engine, e.source_id, e.source_type)
        target_name = pipeline_module.resolve_node_name(engine, e.target_id, e.target_type)
        table.add_row(f"{source_name} ({e.source_type})", e.edge_type, f"{target_name} ({e.target_type})")
    console.print(table)


def _print_graph_hits(engine, title: str, hits: list) -> None:
    if not hits:
        console.print(f"[yellow]{title}: sonuç yok.[/yellow]")
        return
    table = Table(title=title)
    table.add_column("Hop")
    table.add_column("Düğüm")
    table.add_column("Tip")
    table.add_column("Kenar")
    for hit in hits:
        name = pipeline_module.resolve_node_name(engine, hit.node_id, hit.node_type)
        table.add_row(str(hit.hop), name, hit.node_type, hit.via_edge_type)
    console.print(table)


@lineage_app.command("impact")
def lineage_impact(
    dataset: str = typer.Option(..., help="orders | customers | payments"),
    column: str = typer.Option(None, help="Kolon adı (verilmezse dataset seviyesi düğüm kullanılır)"),
    max_hops: int = typer.Option(6, help="Maksimum traversal derinliği (§9.3)"),
) -> None:
    """Bir düğüm bozulursa etkilenecek downstream varlıkları gösterir (§9.3)."""
    engine = db_module.get_engine()
    node_id, node_type = _resolve_node(engine, dataset, column)
    edges = pipeline_module.fetch_all_edges(engine)
    hits = lineage_module.downstream_impact(edges, node_id, max_hops=max_hops)
    label = f"{dataset}.{column}" if column else dataset
    _print_graph_hits(engine, f"Downstream etki: {label}", hits)


@lineage_app.command("upstream")
def lineage_upstream(
    dataset: str = typer.Option(..., help="orders | customers | payments"),
    column: str = typer.Option(None, help="Kolon adı (verilmezse dataset seviyesi düğüm kullanılır)"),
    max_hops: int = typer.Option(6, help="Maksimum traversal derinliği (§9.3)"),
) -> None:
    """Bir düğümü etkileyebilecek olası upstream kaynakları, en yakından en
    uzağa sıralı gösterir (§9.3 "İlk olası upstream kaynak")."""
    engine = db_module.get_engine()
    node_id, node_type = _resolve_node(engine, dataset, column)
    edges = pipeline_module.fetch_all_edges(engine)
    hits = lineage_module.upstream_sources(edges, node_id, max_hops=max_hops)
    label = f"{dataset}.{column}" if column else dataset
    _print_graph_hits(engine, f"Olası upstream kaynaklar: {label}", hits)


@app.command()
def analyze(
    dataset: str = typer.Option(..., help="orders | customers | payments"),
    run_id: str = typer.Option(None, help="Analiz edilecek run id (verilmezse en son run kullanılır)"),
    llm: bool = typer.Option(
        None,
        help="LLM sentezini zorla aç/kapat (varsayılan: ANTHROPIC_API_KEY varsa otomatik açık, §10-§12)",
    ),
) -> None:
    """Root Cause Agent: bir run'ın sinyallerinden kanıtlı kök neden raporu
    üretir (§10-§12). Deterministik kural motoru (rca.py) her zaman
    çalışır; LLM yalnızca mevcutsa ve kanıtlanabilir olduğunda hipotez
    metnini rafine eder — asla yeni kanıt icat etmez."""
    engine = db_module.get_engine()

    target_run_id = run_id
    if target_run_id is None:
        latest = pipeline_module.get_latest_run(engine, dataset)
        if latest is None:
            console.print(f"[yellow]{dataset} için run bulunamadı. Önce `sentinel run` çalıştırın.[/yellow]")
            raise typer.Exit(code=1)
        target_run_id = latest["run_id"]

    report, meta = analyze_run(engine, target_run_id, use_llm=llm)

    severity_color = {"critical": "bold red", "high": "red", "medium": "yellow", "low": "green"}.get(
        report.severity, "white"
    )
    mode = "LLM ile rafine edildi" if report.llm_used else "deterministik (kural tabanlı)"
    console.print(
        Panel(
            f"[{severity_color}]{report.severity.upper()}[/{severity_color}]  —  {escape(report.summary)}\n"
            f"[dim]run={target_run_id}  incident={meta['incident_id']}  mod={mode}[/dim]",
            title=f"Root Cause Agent — {dataset}",
        )
    )

    if not llm_module.is_llm_available() and llm is not False:
        console.print(
            "İpucu: ANTHROPIC_API_KEY tanımlı değil, deterministik moddasınız. "
            "LLM sentezi için ANTHROPIC_API_KEY ortam değişkenini ayarlayın ve "
            'pip install "pipeline-sentinel[llm]" ile ekstra bağımlılığı kurun.',
            style="dim",
            markup=False,
        )
    if meta.get("llm_meta", {}).get("error"):
        console.print(f"[yellow]LLM notu: {escape(str(meta['llm_meta']['error']))}[/yellow]")

    if report.root_causes:
        table = Table(title="Kök neden hipotezleri (§12.1)")
        table.add_column("Hipotez")
        table.add_column("Güven")
        table.add_column("Kanıt (evidence_ids)")
        for h in report.root_causes:
            table.add_row(h.hypothesis, f"{h.confidence:.2f}", ", ".join(h.evidence_ids))
        console.print(table)

    if report.affected_assets:
        console.print(f"[bold]Etkilenen varlıklar (§9 lineage):[/bold] {', '.join(report.affected_assets)}")

    if report.recommended_actions:
        table = Table(title="Önerilen aksiyonlar (taslak — §14.3 insan onayı gerekir)")
        table.add_column("Tip")
        table.add_column("Açıklama")
        table.add_column("Onay gerekir mi?")
        for a in report.recommended_actions:
            table.add_row(a["type"], a["description"], "Evet" if a["requires_approval"] else "Hayır")
        console.print(table)

    if report.uncertainties:
        console.print("[bold]Belirsizlikler:[/bold]")
        for u in report.uncertainties:
            console.print(f"  • {u}")


_ACTION_STATUS_COLOR = {"approved": "green", "rejected": "red", "pending": "yellow"}


@actions_app.command("list")
def actions_list(
    incident: str = typer.Option(..., help="Incident id"),
) -> None:
    """Bir incident'ın önerilen aksiyonlarını (taslak — §14.3) listeler."""
    engine = db_module.get_engine()
    if pipeline_module.get_incident(engine, incident) is None:
        console.print(f"[red]Incident bulunamadı: {incident}[/red]")
        raise typer.Exit(code=1)

    rows = pipeline_module.list_actions_for_incident(engine, incident)
    if not rows:
        console.print("[yellow]Aksiyon yok.[/yellow]")
        return

    table = Table(title=f"Aksiyonlar — incident {incident}")
    table.add_column("ID")
    table.add_column("Tip")
    table.add_column("Açıklama")
    table.add_column("Onay gerekir mi?")
    table.add_column("Durum")
    for a in rows:
        color = _ACTION_STATUS_COLOR.get(a["status"], "white")
        table.add_row(
            a["id"],
            a["type"],
            a["description"],
            "Evet" if a["requires_approval"] else "Hayır",
            f"[{color}]{a['status']}[/{color}]",
        )
    console.print(table)


@actions_app.command("approve")
def actions_approve(
    action_id: str,
    actor: str = typer.Option(..., help="Onaylayan kişi/sistem"),
    note: str = typer.Option(None, help="Opsiyonel not"),
) -> None:
    """Bir aksiyonu onaylar. Hiçbir SQL/dbt çalıştırmaz — yalnızca insan
    kararını kaydeder (§14.3); gerçek execution kasıtlı olarak kapsam
    dışıdır (§22 "v1.5 Repair sandbox")."""
    engine = db_module.get_engine()
    result = pipeline_module.decide_action(engine, action_id, "approve", actor=actor, note=note)
    if result is None:
        console.print(f"[red]Aksiyon bulunamadı: {action_id}[/red]")
        raise typer.Exit(code=1)
    console.print(f"[green]Onaylandı:[/green] {escape(result['description'])} (actor={actor})")


@actions_app.command("reject")
def actions_reject(
    action_id: str,
    actor: str = typer.Option(..., help="Reddeden kişi/sistem"),
    note: str = typer.Option(None, help="Opsiyonel not"),
) -> None:
    engine = db_module.get_engine()
    result = pipeline_module.decide_action(engine, action_id, "reject", actor=actor, note=note)
    if result is None:
        console.print(f"[red]Aksiyon bulunamadı: {action_id}[/red]")
        raise typer.Exit(code=1)
    console.print(f"[red]Reddedildi:[/red] {escape(result['description'])} (actor={actor})")


if __name__ == "__main__":
    app()
