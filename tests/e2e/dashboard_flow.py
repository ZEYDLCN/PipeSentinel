"""Gerçek tarayıcıda (Playwright) dashboard uçtan uca akışı.

pytest tarafından toplanmaz (dosya adı test_ ile başlamaz). Kendi API ve worker
süreçlerini başlatır; boş, ayrı bir PostgreSQL veritabanı ister (demo şemasını
sıfırlayan testlerle aynı uyarı geçerli):

    pip install -e ".[dev,api]" playwright
    python tests/e2e/dashboard_flow.py --db-url postgresql+psycopg://user@127.0.0.1:5441/sentinel_e2e \
        --out reports/e2e --browser msedge

Akış: CSV'den taslak sözleşme → kaynak kaydı (zamanlamalı) → zamanlayıcının otomatik analizi →
baseline kabulü → veri kayması (+%15, eski z-testiyle yakalanmaz) → olay durumu, susturma,
metrik grafikleri → beklenen değişim kabulü → temiz analiz; ardından pilot modunda rol denetimi
ve dar ekran düzen kontrolü.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from playwright.sync_api import Page, expect, sync_playwright
from sqlalchemy import create_engine, text

from pipeline_sentinel import reliability_store

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = "e2e"


def wait_http(url: str, timeout: float = 40) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2).read()
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError(f"Sunucu açılmadı: {url}")


def api(base: str, path: str, token: str | None = None):
    request = urllib.request.Request(base + "/api/v1/reliability" + path,
                                     headers={"Authorization": "Bearer " + token} if token else {})
    return json.loads(urllib.request.urlopen(request, timeout=10).read())


def poll(fn, what: str, timeout: float = 90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(1)
    raise AssertionError(f"Zaman aşımı: {what}")


def prepare_source(url: str) -> list[str]:
    engine = create_engine(url)
    rows = []
    reliability_store.metadata.drop_all(engine)  # her koşu temiz metadata ile başlar
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE'))
        conn.execute(text(f"CREATE SCHEMA {SCHEMA}"))
        conn.execute(text(f"CREATE TABLE {SCHEMA}.orders (order_id integer, status text, total_amount double precision, created_at timestamptz)"))
        conn.execute(text(f"""INSERT INTO {SCHEMA}.orders
            SELECT g, (ARRAY['paid','open','void'])[1 + g % 3], 300 + (g * 37) % 400, now() - (g % 30) * interval '1 minute'
            FROM generate_series(1, 300) g"""))
        for row in conn.execute(text(f"SELECT order_id, status, total_amount, created_at FROM {SCHEMA}.orders ORDER BY order_id")):
            rows.append(f"{row[0]},{row[1]},{row[2]},{row[3].isoformat()}")
    engine.dispose()
    return ["order_id,status,total_amount,created_at", *rows]


class Browserless:
    """Konsol/ağ hatalarını toplar; beklenmeyen her hata testi başarısız kılar."""

    def __init__(self, page: Page):
        self.errors: list[str] = []
        page.on("pageerror", lambda e: self.errors.append("pageerror: " + str(e)))
        page.on("console", lambda m: m.type == "error" and self.errors.append("console: " + m.text))
        page.on("response", lambda r: r.status >= 400 and not r.url.endswith("/favicon.ico")
                and self.errors.append(f"http {r.status}: {r.url}"))


def run(args) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    csv_path = out / "sample.csv"
    csv_path.write_text("\n".join(prepare_source(args.db_url)) + "\n", encoding="utf-8")
    env = {**os.environ, "DATABASE_URL": args.db_url, "SENTINEL_SOURCE_E2E": args.db_url, "SENTINEL_MODE": "development",
           "PYTHONIOENCODING": "utf-8", "SENTINEL_WORKER_TTL_SECONDS": "25"}
    env.pop("SENTINEL_API_TOKENS", None)
    duck_path = prepare_duckdb(out / "warehouse.duckdb")
    if duck_path:
        env["SENTINEL_SOURCE_DUCK"] = str(duck_path)
    subprocess.run([sys.executable, "-c", "from pipeline_sentinel.cli import app; app()", "migrate"], env=env, cwd=ROOT, check=True)
    base = f"http://127.0.0.1:{args.port}"
    procs = [
        subprocess.Popen([sys.executable, "-m", "uvicorn", "apps.api.main:app", "--port", str(args.port), "--log-level", "warning"],
                         env=env, cwd=ROOT),
        subprocess.Popen([sys.executable, "-c", "from pipeline_sentinel.cli import app; app()", "reliability", "worker"],
                         env=env, cwd=ROOT),
    ]
    try:
        wait_http(base + "/api/v1/health")
        with sync_playwright() as p:
            browser = p.chromium.launch(channel=args.browser, headless=not args.headed)
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            watcher = Browserless(page)
            flow(page, watcher, base, out, csv_path, args.db_url, procs[1], duck_path is not None)
            narrow(browser, watcher, base, out)
            pilot(browser, base, out, args, env)
            browser.close()
        unexpected = [e for e in watcher.errors if "422" not in e]
        assert not unexpected, "Tarayıcıda beklenmeyen hatalar:\n" + "\n".join(unexpected)
        print("E2E OK — ekran görüntüleri:", out)
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()


def prepare_duckdb(path: Path) -> Path | None:
    try:
        import duckdb
    except ImportError:
        return None
    path.unlink(missing_ok=True)
    connection = duckdb.connect(str(path))
    connection.execute("CREATE TABLE orders AS SELECT i AS id, 100.0 + i AS amount FROM range(1, 201) t(i)")
    connection.close()
    return path


def demo_manifest() -> dict:
    def node(kind, name, schema, sql, deps, columns=()):
        return {"name": name, "resource_type": kind, "relation_name": f'"db"."{schema}"."{name}"', "raw_code": sql,
                **({"compiled_code": sql} if sql else {}), "depends_on": {"nodes": deps}, "columns": {c: {} for c in columns}}
    columns = ["order_id", "status", "total_amount", "created_at"]
    return {"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json", "adapter_type": "postgres"},
            "sources": {"source.demo.raw.orders": node("source", "orders", "raw", "", [], columns)},
            "nodes": {
                "model.demo.stg_orders": node("model", "stg_orders", "analytics",
                                              'select order_id, status, total_amount, created_at from "db"."raw"."orders"',
                                              ["source.demo.raw.orders"]),
                "model.demo.orders": node("model", "orders", "analytics",
                                          'select order_id, status, total_amount, created_at from "db"."analytics"."stg_orders"',
                                          ["model.demo.stg_orders"])},
            "exposures": {"exposure.demo.finance": {"name": "Finance", "depends_on": {"nodes": ["model.demo.orders"]}}}}


def flow(page: Page, watcher: Browserless, base: str, out: Path, csv_path: Path, db_url: str, worker, has_duckdb: bool) -> None:
    page.goto(base + "/")
    expect(page).to_have_title("Pipeline Sentinel AI · Veri güvenilirliği")
    expect(page.locator("#session-state")).to_contain_text("local-dev")
    page.screenshot(path=out / "01-genel-bakis.png")

    # 1) CSV'den taslak sözleşme
    page.click('button[data-tab="sources"]')
    form = page.locator("#source-form")
    page.set_input_files("#suggest-csv", str(csv_path))
    page.click("#suggest-contract")
    expect(page.locator("#message")).to_contain_text("Taslak sözleşme yerleştirildi")
    contract = form.locator("[name=contract]").input_value()
    assert "total_amount" in contract and "allowed_values" in contract and "# Taslak sözleşme" in contract
    page.screenshot(path=out / "02-taslak-sozlesme.png")

    # 2) Kaynak (zamanlamalı) kaydet
    for name, value in {"name": "Siparişler", "connection_env": "SENTINEL_SOURCE_E2E", "schema": SCHEMA, "table": "orders",
                        "order_by": "order_id", "schedule_minutes": "5", "learned_freshness": "created_at",
                        "lineage_asset": "model.demo.orders"}.items():
        form.locator(f"[name={name}]").fill(value)
    form.locator("[name=multivariate]").check()
    expect(form.locator("[name=retain_snapshot]")).to_be_checked()  # çok değişkenli izleme veri kopyası ister
    page.click('#source-form .toolbar button:has-text("Kaydet")')
    expect(page.locator("#message")).to_contain_text("Kaynak kaydedildi")
    expect(page.locator("#sources")).to_contain_text("Her 5 dk")
    source_id = api(base, "/sources")[0]["id"]

    # 3) Worker'ın zamanlayıcısı kendiliğinden analiz başlatmalı (en geç ~35 sn)
    job = poll(lambda: next((j for j in api(base, "/jobs") if j["actor"] == "scheduler" and j["status"] == "succeeded"), None),
               "zamanlanmış analiz")
    assert job["key"].startswith("schedule:")
    # Düzenleme akışı: zamanlamayı kaldır (testin ortasında yeni otomatik analiz araya girmesin).
    page.click('button[data-tab="sources"]')
    page.locator("#sources").get_by_role("button", name="Düzenle").click()
    expect(form.locator("[name=schedule_minutes]")).to_have_value("5")
    expect(form.locator("[name=learned_freshness]")).to_have_value("created_at")
    form.locator("[name=schedule_minutes]").fill("")
    page.click('#source-form .toolbar button:has-text("Kaydet")')
    expect(page.locator("#sources")).to_contain_text("Elle")
    expect(page.locator("#monitoring-banner")).to_be_hidden()  # worker canlı, kaynak zamanında analiz edildi

    # dbt manifest (derlenmiş SQL ile) → kolon bağlantıları
    manifest_path = out / "manifest.json"
    manifest_path.write_text(json.dumps(demo_manifest()), encoding="utf-8")
    page.click('button[data-tab="lineage"]')
    ingest = page.locator("#ingest-form")
    ingest.locator("[name=namespace]").fill("demo")
    ingest.locator("[name=artifact]").set_input_files(str(manifest_path))
    ingest.get_by_role("button", name="İçeri al").click()
    expect(page.locator("#message")).to_contain_text("kolon bağlantısı çıkarıldı")
    expect(page.locator("#graphs")).to_contain_text("Kolon bağlantıları (")
    expect(page.locator("#graphs")).to_contain_text("model.demo.stg_orders.total_amount")
    page.screenshot(path=out / "08-kolon-lineage.png")
    page.click('button[data-tab="monitor"]')
    page.click("#refresh")
    first_row = page.locator("#observations tr:has(td)").first
    expect(first_row).to_contain_text("0 sinyal")
    first_row.get_by_role("button", name="Kanıtları aç").click()
    detail = page.locator("#observation-detail")
    expect(detail).to_be_visible()
    expect(detail.get_by_text("Grafik için en az iki analiz gerekir.")).to_be_visible()
    detail.get_by_role("button", name="Baseline olarak kabul et").click()
    expect(page.locator("#observations tr:has(td)").first).to_contain_text("accepted", timeout=15000)

    # 4) Veri +%15 kaysın (z-testi ile yakalanmayan, standart hata testiyle yakalanan kayma)
    shift(db_url, 1.15)
    analyze_via_ui(page, base, 1)
    incident = open_latest(page, base)
    expect(incident.get_by_role("heading", name="Gerçek bir iş değişimi mi?")).to_be_visible()
    expect(incident.get_by_role("heading", name="Olay durumu")).to_be_visible()
    expect(incident.get_by_role("heading", name="Alarmı sustur")).to_be_visible()
    expect(incident.get_by_role("heading", name="Metrik geçmişi")).to_be_visible()
    assert incident.locator(".spark").count() >= 3, "satır sayısı + ortalama + boş oranı grafikleri"
    assert incident.locator(".spark circle.bad").count() >= 1, "sinyalli analiz kırmızı işaretli"
    assert incident.locator(".spark circle.current").count() >= 1
    expect(incident.locator("table").first).to_contain_text("distribution")
    expect(incident.get_by_role("heading", name="Kolon etkisi")).to_be_visible()
    expect(incident.locator(".column-impact")).to_contain_text("model.demo.stg_orders.total_amount")
    expect(incident.locator(".column-impact")).to_contain_text("source.demo.raw.orders.total_amount")
    page.screenshot(path=out / "03-olay-detay.png", full_page=True)

    # 5) Olay durumu + susturma
    incident.locator(".triage-panel select").select_option("acknowledged")
    incident.locator(".triage-panel input[aria-label=Sorumlu]").fill("ayse")
    incident.locator(".triage-panel input[aria-label='Olay notu']").fill("Kampanya mı bakıyorum")
    incident.get_by_role("button", name="Durumu kaydet").click()
    expect(page.locator("#message")).to_contain_text("Olay durumu kaydedildi")
    expect(page.locator("#observations tr:has(td)").first).to_contain_text("Üstlenildi · ayse")
    incident = page.locator("#observation-detail")
    incident.locator(".mute-panel select").first.select_option("distribution|total_amount")
    incident.locator(".mute-panel input[aria-label='Susturma gerekçesi']").fill("Kampanya haftası, bilerek yüksek")
    incident.get_by_role("button", name="Sustur").click()
    expect(page.locator("#message")).to_contain_text("Alarm susturuldu")
    expect(page.locator("#observation-detail .mute-panel table")).to_contain_text("Kampanya haftası")
    notifications_before = len(api(base, "/notifications"))
    assert notifications_before >= 1

    # 6) Aynı kayma yeniden gelir: susturulduğu için bildirim üretmemeli, kanıt yine kaydedilmeli
    analyze_via_ui(page, base, 2)
    expect(page.locator("#observations tr:has(td)").first).to_contain_text("susturuldu")
    assert len(api(base, "/notifications")) == notifications_before
    muted = open_latest(page, base)
    expect(muted.get_by_role("heading", name="Olay durumu · susturulmuş alarm")).to_be_visible()

    # 7) Beklenen değişim: gerekçe zorunlu, sonra yeni referans
    note = muted.locator("input[placeholder^='Değişimin gerekçesi']")
    note.fill("kısa")
    muted.get_by_role("button", name="Beklenen değişim olarak kabul et").click()
    expect(page.locator("#message")).to_contain_text("gerekçe")  # 10 karakterden kısa → API 422 mesajı
    note.fill("Fiyat listesi %15 güncellendi")
    muted.get_by_role("button", name="Beklenen değişim olarak kabul et").click()
    expect(page.locator("#message")).to_contain_text("Yeni referans kaydedildi", timeout=15000)
    expect(page.locator("#observations tr:has(td)").first).to_contain_text("accepted")

    # 8) Yeni referanstan sonra aynı veri temiz çıkmalı
    analyze_via_ui(page, base, 3)
    expect(page.locator("#observations tr:has(td)").first).to_contain_text("0 sinyal")
    clean = open_latest(page, base)
    assert clean.locator(".spark").count() >= 3
    page.screenshot(path=out / "04-temiz-analiz.png", full_page=True)

    # 9) Ağ ve diğer sekmeler açılıyor
    for tab, heading in (("lineage", "Gerçek bağımlılıklar"), ("policies", "Varlık sahipleri"), ("notifications", "Bildirim kutusu")):
        page.click(f'button[data-tab="{tab}"]')
        expect(page.get_by_role("heading", name=heading, exact=False).first).to_be_visible()
    page.click('button[data-tab="notifications"]')
    expect(page.locator("#notifications")).to_contain_text("Siparişler")
    page.screenshot(path=out / "05-bildirimler.png")
    assert source_id

    # 10) İkinci connector: DuckDB dosyası, tarayıcıdan kaynak ekle ve analiz et
    if has_duckdb:
        page.click('button[data-tab="sources"]')
        for name, value in {"name": "Depo", "connection_env": "SENTINEL_SOURCE_DUCK", "schema": "main", "table": "orders",
                            "order_by": "id", "schedule_minutes": "", "learned_freshness": "", "lineage_asset": ""}.items():
            form.locator(f"[name={name}]").fill(value)
        form.locator("[name=type]").select_option("duckdb")
        form.locator("[name=multivariate]").uncheck()
        form.locator("[name=retain_snapshot]").uncheck()
        form.locator("[name=contract]").fill("version: 1\nexpected_schema:\n  id: integer\n  amount: float\nrules:\n  - type: uniqueness\n    column: id\n")
        page.click('#source-form .toolbar button:has-text("Kaydet")')
        expect(page.locator("#message")).to_contain_text("Kaynak kaydedildi")
        expect(page.locator("#sources tr", has_text="Depo")).to_be_visible()
        analyze_via_ui(page, base, 4, "Depo")
        expect(page.locator("#observations tr:has(td)").first).to_contain_text("Depo")
        expect(page.locator("#observations tr:has(td)").first).to_contain_text("0 sinyal")
        page.screenshot(path=out / "09-duckdb-kaynak.png")

    # 11) Worker çökerse izleme bunu söyler: bekleyen iş + canlı worker yok → banner
    worker.terminate()
    worker.wait(10)
    page.click('button[data-tab="sources"]')
    page.locator("#sources tr", has_text="Siparişler").get_by_role("button", name="Analiz başlat").click()
    banner = page.locator("#monitoring-banner")
    expect(banner).to_be_visible(timeout=90000)
    expect(banner).to_contain_text("İzleme durdu")
    expect(banner).to_contain_text("Canlı worker yok")
    page.click('button[data-tab="monitor"]')
    page.screenshot(path=out / "10-izleme-durdu.png")
    assert api(base, "/monitoring")["status"] == "down"


def shift(db_url: str, factor: float) -> None:
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text(f"UPDATE {SCHEMA}.orders SET total_amount = total_amount * :f"), {"f": factor})
    engine.dispose()


def analyze_via_ui(page: Page, base: str, expected_total: int, source: str = "Siparişler") -> None:
    before = len(api(base, "/observations", None))
    page.click('button[data-tab="sources"]')
    page.locator("#sources tr", has_text=source).get_by_role("button", name="Analiz başlat").click()
    expect(page.locator("#message")).to_contain_text("Analiz kuyruğa alındı")
    poll(lambda: len(api(base, "/observations")) > before, f"{expected_total}. analiz tamamlanmadı")
    page.click('button[data-tab="monitor"]')
    page.click("#refresh")
    expect(page.locator("#observations tr:has(td)")).to_have_count(before + 1)  # tablo yeni analizle yenilendi


def open_latest(page: Page, base: str):
    """En yeni analizin detayını açar ve panelin bu analiz için tamamen yüklenmesini bekler."""
    latest = api(base, "/observations")[0]["id"]
    page.locator("#observations tr:has(td)").first.get_by_role("button", name="Kanıtları aç").click()
    detail = page.locator("#observation-detail")
    expect(detail).to_have_attribute("data-ready", latest)
    expect(detail.get_by_role("heading", name="Metrik geçmişi")).to_be_visible()
    return detail


def narrow(browser, watcher: Browserless, base: str, out: Path) -> None:
    page = browser.new_page(viewport={"width": 390, "height": 844})
    narrow_watcher = Browserless(page)
    page.goto(base + "/")
    expect(page.locator("#session-state")).to_contain_text("local-dev")
    page.locator("#observations tr:has(td)").first.get_by_role("button", name="Kanıtları aç").click()
    expect(page.locator("#observation-detail")).to_be_visible()
    overflow = page.evaluate("document.documentElement.scrollWidth - window.innerWidth")
    page.screenshot(path=out / "06-dar-ekran.png", full_page=True)
    assert overflow <= 1, f"Dar ekranda yatay taşma: {overflow}px"
    assert not narrow_watcher.errors, narrow_watcher.errors
    page.close()


def pilot(browser, base: str, out: Path, args, env) -> None:
    """Pilot modunda rol denetimi: okuyucu salt okur, operatör işlem yapabilir."""
    tokens = {"r" * 32: {"actor": "reader-1", "role": "reader"}, "o" * 32: {"actor": "operator-1", "role": "operator"}}
    pilot_port = args.port + 1
    proc = subprocess.Popen([sys.executable, "-m", "uvicorn", "apps.api.main:app", "--port", str(pilot_port), "--log-level", "warning"],
                            env={**env, "SENTINEL_MODE": "pilot", "SENTINEL_API_TOKENS": json.dumps(tokens)}, cwd=ROOT)
    try:
        pilot_base = f"http://127.0.0.1:{pilot_port}"
        wait_http(pilot_base + "/api/v1/health")
        for token, actor, can_write in (("r" * 32, "reader-1 · reader", False), ("o" * 32, "operator-1 · operator", True)):
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            page.goto(pilot_base + "/")
            page.click("#login")
            page.fill("#login-form [name=token]", token)
            page.click('#login-form button:has-text("Bağlan")')
            expect(page.locator("#session-state")).to_have_text(actor)
            expect(page.locator("#message")).to_have_text("")  # oturum öncesi 401 mesajı ekranda kalmamalı
            page.click('button[data-tab="sources"]')
            button = page.locator("#sources").get_by_role("button", name="Analiz başlat").first
            expect(button).to_be_enabled() if can_write else expect(button).to_be_disabled()
            expect(page.locator("#source-form button.btn:has-text('Kaydet')")).to_be_enabled() if can_write else \
                expect(page.locator("#source-form button.btn:has-text('Kaydet')")).to_be_disabled()
            if not can_write:
                page.screenshot(path=out / "07-pilot-okuyucu.png")
            page.close()
    finally:
        proc.terminate()
        proc.wait(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--out", default="reports/e2e")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--browser", default="msedge", help="msedge | chrome | chromium")
    parser.add_argument("--headed", action="store_true")
    run(parser.parse_args())
