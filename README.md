# Pipeline Sentinel

**Human-in-the-loop AI reliability platform for data pipelines.**

Pipeline Sentinel is an early-stage, founder-led B2B SaaS project founded in 2026.
It detects silent data failures, traces their impact, explains likely root causes
with evidence, and validates proposed fixes on isolated snapshots for human review.

- **Founded in 2026**
- **Founder:** Zeyd Alcan
- **Status:** Working MVP / prototype — PostgreSQL analysis, data contracts,
  lineage, evidence-based RCA, and isolated fix validation are implemented.
- Website: https://pipelinesentinel.dev
- Contact: founder@pipelinesentinel.dev
- GitHub: https://github.com/ZEYDLCN/PipeSentinel

The prototype supports technical evaluation and pilot preparation. Real-world
customer outcomes still need pilot validation; human approval remains central
to the workflow.

## Gerçek kaynaklar ve onarım doğrulaması

PostgreSQL connector, YAML sözleşmeleri, kalite onaylı/mevsimsel baseline,
dbt/OpenLineage ingestion, olay gruplama, iş etkisi, çözüm hafızası,
izole onarım doğrulaması, rol kontrolü, webhook outbox ve CI veri farkı kapısı
eklendi. Yeni dashboard: `/` (ve `/reliability.html`); veri laboratuvarı: `/demo`.

Kurulum, worker, pilot erişimi ve kapsam sınırları:
[`docs/reliability-guide-tr.md`](docs/reliability-guide-tr.md).

Self-healing data pipeline agent — veri pipeline bozulmalarını saptar, kök
nedeni açıklar ve kontrollü onarım üretir. Tam mimari ve ürün tasarımı için
[`docs/pipeline-sentinel-design.md`](docs/pipeline-sentinel-design.md)
dokümanına bakın.

Bu repo, o tasarımın 7 fazlık planının (§17) **çekirdeğini uçtan uca**
uygulanabilir kod olarak içerir — Faz 1'den Faz 7'ye:

- Sentetik `customers` / `orders` / `payments` commerce pipeline'ı (PostgreSQL)
- Kolon/veri kümesi profilleme (null oranı, distinct oranı, mean/stddev/quantile)
- Deterministik data contract kontrolleri (schema, null, range, uniqueness, freshness, allowed values)
- Baseline'a karşı istatistiksel karşılaştırma (robust z-score, oransal değişim)
- F01-F06 kontrollü hata enjeksiyon kataloğu
- Dataset/job/column lineage graph'ı + downstream etki & upstream kaynak sorguları (§9)
- Root Cause Agent: kural tabanlı, kanıtlı kök neden hipotezleri + taslak onarım aksiyonları + opsiyonel LLM sentezi (§10-§12)
- FastAPI REST API + bağımlılıksız statik dashboard: incident listesi, kök neden detayı, lineage explorer, tek tıkla hata enjeksiyonu (§13)
- Agent regression eval seti (RCA Top-1 accuracy, evidence precision) + canlı PostgreSQL'e karşı otomatik CI (§16, §20)
- Approval workflow: önerilen aksiyonları onayla/reddet — hiçbir SQL/dbt otomatik çalıştırılmaz (§14.3)
- `sentinel` CLI: bir pipeline run'ını üret → yükle → profille → tespit et → etkisini graf üzerinde göster → kök nedeni analiz et → aksiyonu onayla/reddet

Gerçek kaynak akışı OpenLineage ingestion ve repair sandbox'ı içerir.
Üretimde otomatik execution ve vector search kapsam dışındadır; aşağıdaki
faz listesi ilk sentetik demo sürümünü anlatır. Güncel kapsam için
[gerçek kaynak kılavuzuna](docs/reliability-guide-tr.md) bakın.

## Hızlı başlangıç

```bash
# 1. Bağımlılıkları kur
pip install -e ".[dev]"

# 2. PostgreSQL'i ayağa kaldır
docker compose -f infra/docker-compose.yml up -d
cp infra/.env.example .env   # veya DATABASE_URL'i doğrudan export edin
export DATABASE_URL=postgresql+psycopg://sentinel:sentinel@localhost:5432/pipeline_sentinel

# 3. Şemayı kur
sentinel migrate

# 4. Baseline oluştur (14 sağlıklı run, §8.1)
sentinel bootstrap --runs 14

# 5. Sağlıklı bir run çalıştır — sinyal beklenmez
sentinel run

# 6. Kontrollü bir hata enjekte et (§3.4 örnek senaryo: TL → kuruş)
sentinel run --fault F05

# 7. Kataloğu ve son raporu incele
sentinel faults
sentinel report --dataset orders

# 8. Lineage graph'ını ve downstream etkisini incele (§9, §3.4 örnek senaryo)
sentinel lineage graph
sentinel lineage impact --dataset orders --column total_amount
sentinel lineage upstream --dataset payments --column amount

# 9. Root Cause Agent: kanıtlı kök neden raporu (§10-§12)
sentinel analyze --dataset orders

# 10. Önerilen aksiyonları incele ve onayla/reddet (§14.3) — hiçbir SQL/dbt
#     otomatik çalıştırılmaz, yalnızca karar kaydedilir. <incident_id>,
#     bir önceki adımın çıktısındaki "incident=..." değeridir.
sentinel actions list --incident <incident_id>
sentinel actions approve <action_id> --actor "sizin-adınız"
```

### Dashboard / API (Faz 5 + Faz 7)

```bash
pip install -e ".[dev,api]"
python -m uvicorn apps.api.main:app --reload --port 8000
```

- Genel bakış: <http://localhost:8000/> — gerçek kaynaklar, aktivite ve veri sağlığı.
- Veri laboratuvarı: <http://localhost:8000/demo> — datasets, incidents, lineage explorer;
  tek tıkla sağlıklı run / hata enjeksiyonu (F01-F06 seçilebilir; enjeksiyon
  sonrası RCA agent'ı otomatik tetiklenir). Incident detayında her önerilen
  aksiyon için Onayla/Reddet butonları (§14.3) — hiçbir SQL/dbt otomatik
  çalıştırılmaz.
- API: <http://localhost:8000/api/v1/...> — Swagger UI: <http://localhost:8000/docs>

CLI ve dashboard aynı PostgreSQL'i paylaşır — birinde yaptığınız değişiklik
diğerinde anında görünür.

`docker compose` yoksa (veya erişilemiyorsa) yerel bir PostgreSQL 16
kurulumuna karşı da doğrudan çalışır — tek gereken doğru `DATABASE_URL`.

`sentinel analyze`, `ANTHROPIC_API_KEY` tanımlı değilse tamamen
deterministik (kural tabanlı) çalışır — bu varsayılan moddur ve LLM
olmadan da eksiksizdir. LLM ile hipotez metnini rafine etmek isterseniz:

```bash
pip install -e ".[dev,llm]"
export ANTHROPIC_API_KEY=sk-ant-...
sentinel analyze --dataset orders --llm
```

## Test

```bash
pip install -e ".[dev,api]"
pytest                  # profiler/detector/faults/contracts/lineage/rca/llm birim testleri (DB gerektirmez)
pytest -m integration   # orchestrator + analyze + API uçtan uca testi (canlı Postgres gerektirir, DATABASE_URL)
```

## Kontrollü hata kataloğu (F01-F06)

| ID | Senaryo | Beklenen sinyal | Root Cause Agent hipotezi |
|---|---|---|---|
| F01 | Kolon silme (`customer_id`) | `schema_missing_column` (contract) | "Kolon upstream'de kaldırılmış olabilir" |
| F02 | String → integer tip değişimi (`status`) | `schema_type_mismatch` + `semantic_drift` (contract) | "Veri tipi upstream'de değişmiş olabilir" |
| F03 | Null oranı ~%2 → ~%40 (`total_amount`) | `completeness` (contract + baseline) | "Upstream'de veri toplama adımı eksik değer üretiyor" |
| F04 | Kayıtları iki kez yükleme | `volume` (baseline) + `uniqueness` (contract) | "Duplicate load / idempotency hatası" |
| F05 | TL → kuruş ölçek hatası (`total_amount` × 100) | `range` (contract) + `distribution` (baseline) | "Olası birim/ölçek hatası (TL→kuruş)" |
| F06 | Üç saatlik veri gecikmesi (`created_at`) | `freshness` (contract) | "SLA aşıldı; upstream job gecikmiş olabilir" |

Her senaryo `evals/incidents/` altında beklenen sinyal tipi + RCA
`rule_id`/güven eşiğiyle altın (golden) bir test vakası olarak da
tanımlıdır ve CI'da her push/PR'da otomatik koşar (`root_cause_top1_accuracy`
şu an %100) — bkz. [`evals/README.md`](evals/README.md).

## Repository yapısı

Bkz. tasarım dokümanı §18. Çekirdek mantık `src/pipeline_sentinel/`
altında modüler monolith olarak yaşıyor (§4.2, §24 — "İlk sürümde
mikroservis zorunlu değil"); `apps/api` ve `apps/web` onu tüketen ince
bir sunum katmanı. `services/*` ve `packages/*` altındaki `README.md`
dosyaları hangi kodun hangi fazda oraya taşınacağını işaret ediyor.

```
src/pipeline_sentinel/
├── synthetic.py     # sentetik customers/orders/payments üretimi
├── contracts.py     # data contract tanımları + deterministik kontroller (§7.1)
├── faults.py         # F01-F06 kontrollü hata enjeksiyonu (§16.1)
├── profiler.py        # kolon/dataset profilleme (§6.2)
├── detector.py          # sinyal üretimi + risk skoru (§7)
├── lineage.py             # saf graph traversal: downstream_impact / upstream_sources (§9)
├── rca.py                  # kural tabanlı Root Cause hipotez motoru + structured output (§10-§12)
├── llm.py                   # opsiyonel LLM sentezi + kanıt doğrulama (ANTHROPIC_API_KEY)
├── analyze.py                 # analyze_run: agent döngüsü (§10.2 Plan→Kanıt→Hipotez→Doğrula→Raporla)
├── db.py, config.py             # Postgres bağlantısı + migration runner
├── pipeline.py                    # metadata + lineage/incident/agent_run/action/approval kalıcılığı
├── orchestrator.py                 # execute_pipeline_run + sync_commerce_lineage: uçtan uca akış
└── cli.py                           # `sentinel` komutu (run/report/lineage/analyze/actions/...)

apps/
├── api/main.py       # FastAPI — §13 REST uçları (bkz. apps/api/README.md)
└── web/index.html     # Bağımlılıksız statik dashboard (bkz. apps/web/README.md)
```

## Yol haritası

Tasarım dokümanındaki 7 fazlık plana (§17) göre bu repo **tüm fazların
çekirdeğini** tamamlar:

- [x] **Faz 1 — Veri laboratuvarı:** PostgreSQL, sentetik veri, F01-F06
- [x] **Faz 2 (çekirdek) — Profiling & anomaly engine:** contract + baseline karşılaştırması
- [x] **Faz 3 — Lineage:** dataset/job/column graph'ı, downstream etki + upstream kaynak sorguları (§9) — gerçek OpenLineage ingestion hariç
- [x] **Faz 4 (çekirdek) — Agent RCA:** kural tabanlı hipotez motoru, structured output, opsiyonel LLM rafinesi (§10-§12) — gerçek RAG (§11) hariç, `search_logs`/`get_deploy_changes`/`run_validation_sql` araçları hariç
- [x] **Faz 5 (çekirdek) — Dashboard/API:** FastAPI REST API + statik dashboard — incident listesi, kök neden detayı, lineage explorer (§13) — Next.js yerine statik ikame, `/sources` hariç
- [x] **Faz 6 — Evaluation:** agent regression seti (RCA Top-1 accuracy, evidence precision), CI'da canlı Postgres'e karşı otomatik integration testleri (§16.2-§16.3, §20)
- [x] **Faz 7 (çekirdek) — Repair plan:** taslak aksiyonlar (Faz 4) + approval workflow — CLI/API/dashboard'dan onayla/reddet, audit trail (§10.1, §14.3) — dry-run sandbox ve gerçek execution kasıtlı olarak hariç (§22 "v1.5")

## Lisans

MIT
