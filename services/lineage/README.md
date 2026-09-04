# services/lineage

Faz 3 kapsamı (§9): dataset/job/column bağımlılık grafı ve downstream etki
analizi. Kod bağımsız bir servis olarak değil, modüler monolith paketinde
yaşıyor:

- `src/pipeline_sentinel/lineage.py` — saf traversal mantığı (BFS,
  `downstream_impact`/`upstream_sources`), DB bilmez.
- `src/pipeline_sentinel/pipeline.py` — `lineage_edges`/`external_assets`
  kalıcılığı (`register_external_asset`, `add_lineage_edge`,
  `fetch_all_edges`, `resolve_node_name`).
- `src/pipeline_sentinel/orchestrator.py::sync_commerce_lineage` —
  `examples/commerce-pipeline`'ın sabit yapısal graf'ını her run sonunda
  idempotent olarak bildirir.
- `sentinel lineage graph/impact/upstream` CLI komutları.

**Uygulanmayan:** Gerçek OpenLineage event ingestion'ı (harici
pipeline'lardan otomatik toplama — şu an graf, kodda sabit/bilinen bir
yapıdan türetiliyor) ve Neo4j'e ölçekte geçiş. Bunlar ölçek büyüdüğünde bu
dizine bağımsız bir servis olarak çıkarılabilir (§4.2, §24).
