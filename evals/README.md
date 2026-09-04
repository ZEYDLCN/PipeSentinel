# evals

Faz 1/2/4/6 için deterministik regression eval seti (§16, §20
"Evaluation"). `incidents/` her F01-F06 senaryosu için şunları tanımlar:

- `expected_signal_types` — tespit motorunun (Faz 1-2) üretmesi gereken
  sinyal tipleri
- `expected_rule_id` — Root Cause Agent'ın (Faz 4, `rca.py`) Top-1
  hipotezinin `rule_id`'si
- `min_confidence` — Top-1 hipotezin en az ulaşması gereken güven skoru

`graders/grader.py` bu senaryoları saf Python'da (DB gerektirmeden)
çalıştırır — sentetik veri üretir, fault enjekte eder, tespit + RCA'yı
çalıştırır, sonuçları karşılaştırır ve §16.2 metriklerinin
basitleştirilmiş bir özetini (`detection_recall`,
`root_cause_top1_accuracy`, `evidence_precision`,
`mean_top1_confidence_when_correct`) yazdırır.

```bash
python -m evals.graders.grader
# veya
pytest tests/test_evals.py
```

CI'da her push/PR'da otomatik çalışır (`.github/workflows/ci.yml`).

## Kapsam dışı

- "Kabul edilen alternatif açıklamalar" ve "yasak aksiyonlar" alanları
  (§16.3) — şu an her senaryonun tek bir doğru `rule_id`'si var; LLM
  rafinesiyle hipotez metni çeşitlendiğinde eklenmelidir.
- Impact recall (etkilenen downstream varlıkları bulma oranı) — lineage
  graph DB gerektirir, bu grader'da değil `tests/test_analyze_integration.py`
  ve `tests/test_api_integration.py` içindeki integration testlerinde
  dolaylı olarak doğrulanıyor.
- Repair validation pass rate — Faz 7'nin gerçek execution'ı olmadan
  ölçülemez.
