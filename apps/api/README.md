# apps/api

Faz 5 + Faz 7 REST API (§13) — FastAPI. `src/pipeline_sentinel`'i (Faz
1-4) tüketir, kendi iş mantığını taşımaz.

```bash
pip install -e ".[dev,api]"
export DATABASE_URL=postgresql+psycopg://sentinel:sentinel@localhost:5432/pipeline_sentinel
python -m uvicorn apps.api.main:app --reload --port 8000
```

- API: `http://localhost:8000/api/v1/...`
- Swagger UI: `http://localhost:8000/docs`
- Dashboard (`apps/web`): `http://localhost:8000/`

## Uçlar

| Method | Endpoint | §13 karşılığı |
|---|---|---|
| GET | `/api/v1/datasets` | — (dashboard için ek) |
| GET | `/api/v1/faults` | — (dashboard için ek, §16.1 katalog) |
| POST | `/api/v1/pipeline-runs` | `/profile-runs` (genişletilmiş: tam pipeline run'ı) |
| POST | `/api/v1/bootstrap` | — (§8.1 baseline oluşturma) |
| GET | `/api/v1/pipeline-runs/{run_id}` | `/profile-runs/{id}` |
| POST | `/api/v1/pipeline-runs/{run_id}/analyze` | — (RCA'yı run_id ile ilk kez tetikler) |
| GET | `/api/v1/incidents` | `/incidents` |
| GET | `/api/v1/incidents/{id}` | `/incidents/{id}` |
| GET | `/api/v1/incidents/{id}/actions` | — (aksiyonları ayrı listelemek için) |
| POST | `/api/v1/incidents/{id}/analyze` | `/incidents/{id}/analyze` |
| GET | `/api/v1/lineage/{assetId}` | `/lineage/{assetId}` |
| POST | `/api/v1/actions/{id}/approve` | `/actions/{id}/approve` |
| POST | `/api/v1/actions/{id}/reject` | `/actions/{id}/reject` |
| GET | `/api/v1/actions/{id}/approvals` | — (audit geçmişi) |

`assetId` ya `orders` (dataset) ya da `orders.total_amount` (kolon)
biçiminde olabilir. `approve`/`reject` gövdesi `{"actor": "...", "note":
"..." }` (actor zorunlu, note opsiyonel).

**Önemli (§14.3):** `approve`/`reject` yalnızca bir insan kararını
`actions.status` ve `approvals` audit tablosuna yazar. **Hiçbir SQL/dbt
otomatik çalıştırılmaz.** Gerçek execution + dry-run sandbox kasıtlı
olarak kapsam dışıdır (§22 "v1.5 Repair sandbox").

**Uygulanmayan:** `/sources` (dinamik veri kaynağı tanımlama — MVP'nin
tek kaynağı `examples/commerce-pipeline`).

## Test

```bash
pytest -m integration tests/test_api_integration.py tests/test_actions_integration.py
```
