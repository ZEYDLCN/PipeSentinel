# services/agent

Faz 4 kapsamı (§10-§12): Root Cause Agent. Kod bağımsız bir servis olarak
değil, modüler monolith paketinde yaşıyor:

- `src/pipeline_sentinel/rca.py` — kural tabanlı hipotez motoru (§10.3),
  §12.1 structured output şeması (`IncidentReport`), taslak aksiyon
  şablonları (§10.1 `generate_test`/`propose_patch`). DB ve LLM bilmez,
  tamamen deterministik ve test edilebilir.
- `src/pipeline_sentinel/llm.py` — opsiyonel Claude sentezi. Yalnızca
  `ANTHROPIC_API_KEY` tanımlıysa devreye girer; hipotez METNİNİ rafine
  eder, yeni kanıt icat edemez (`_validate_and_merge` her `evidence_id`'i
  gerçek `signals` id'leriyle doğrular — §11.3).
- `src/pipeline_sentinel/analyze.py::analyze_run` — agent döngüsü (§10.2:
  Plan → Kanıt topla → Hipotez kur → Doğrula → Raporla). Kanıtı
  `pipeline.get_incident_context` ile toplar, etkiyi Faz 3
  `lineage.downstream_impact` ile hesaplar, sonucu `incidents`/
  `agent_runs` olarak kalıcı hale getirir.
- `sentinel analyze --dataset orders [--run-id ...] [--llm/--no-llm]` CLI
  komutu.

**Uygulanan §10.1 araçları:** `get_incident`, `compare_profiles`,
`get_schema_diff`, `query_lineage`, `generate_test`/`propose_patch`
(taslak şablonlar).

**Uygulanmayan:** `search_logs`, `get_deploy_changes`, `run_validation_sql`
(harici log/deploy kaynağı ve sandbox SQL çalıştırma altyapısı yok — bu
demo'da onlara ihtiyaç duyan bir senaryo da yok). Gerçek RAG (§11) —
henüz retrieval edilecek bir incident/runbook külliyatı birikmedi;
§11.3'ün "kaynak zorunluluğu" ilkesi RAG olmadan da tam uygulanıyor.
