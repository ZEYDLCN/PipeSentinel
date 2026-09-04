# PIPELINE SENTINEL AI — Self-Healing Data Pipeline Agent

**Detaylı Teknik Tasarım Dokümanı**
Mimari • Veri Kalitesi • Anomali Tespiti • Agentic AI • GraphRAG • MLOps

Sürüm 1.0 — Eylül 2026

> Bu doküman, kullanıcı tarafından proje başlangıcında referans olarak
> sağlanmıştır ve repo genelinde `§<bölüm>` biçiminde atıf yapılır. Faz 1
> uygulaması (`src/pipeline_sentinel/`) bu dokümanın §2, §6, §7 ve §16-17
> bölümlerini kapsar; kapsanmayan bölümler ileriki fazlar için referanstır
> (bkz. repo kökü `README.md` → "Uygulama durumu").

## Doküman bilgileri

| Alan | Değer |
|---|---|
| Proje | Pipeline Sentinel AI |
| Doküman türü | Yüksek seviyeli mimari + uygulanabilir teknik tasarım |
| Sürüm | 1.0 |
| Tarih | 4 Eylül 2026 |
| Hedef okuyucu | AI engineer, data engineer, backend engineer, teknik ürün yöneticisi |
| Temel amaç | Veri pipeline bozulmalarını saptamak, kök nedeni açıklamak ve kontrollü onarım üretmek |

### Dokümanın kullanım biçimi

Bu doküman, fikir seviyesinden çalışan MVP'ye geçmek için referans tasarım
olarak hazırlanmıştır. İlk bölümler ürün ve sistem sınırlarını; orta
bölümler veri, ML ve agent mimarisini; son bölümler ise test, güvenlik,
dağıtım ve yol haritasını tanımlar.

### Tasarım ilkesi

> Anomaliyi mümkün olduğunca deterministik istatistiksel yöntemler ve veri
> sözleşmeleri yakalar. LLM, ham veride keyfi karar vermek yerine kanıtları
> birleştirir, olası kök nedeni açıklar ve doğrulanabilir bir onarım planı
> üretir.

---

## 1. Yönetici özeti

Pipeline Sentinel AI, veri akışlarının yalnızca çalışıp çalışmadığını
değil, ürettiği verinin beklenen davranışı koruyup korumadığını denetleyen
bir data reliability platformudur. Sistem; şema, dağılım, hacim, gecikme ve
iş kuralı anomalilerini tespit eder. Data lineage graph üzerinden hatanın
kaynağını ve etkilenen varlıkları bulur. Root Cause Agent, loglar, kod
değişiklikleri ve veri profillerini birleştirerek kanıtlı açıklama
oluşturur.

`Data Observability` · `Anomaly Detection` · `Lineage Graph` · `Agentic RCA`

### Temel değer önerisi

| Bugünkü durum | Pipeline Sentinel sonrası |
|---|---|
| Job başarılıysa veri sağlıklı varsayılır. | Başarılı job çıktısı da kalite ve dağılım açısından kontrol edilir. |
| Kök neden loglarda elle aranır. | Lineage ve değişiklik geçmişi üzerinden aday nedenler sıralanır. |
| Etkilenen dashboard ve modeller geç fark edilir. | Downstream etki grafı olay anında çıkarılır. |
| Düzeltme kişiye bağlıdır. | SQL, test ve veri sözleşmesi önerisi standart formatta üretilir. |
| Otomasyon riskli biçimde doğrudan çalışabilir. | Üretim değişiklikleri insan onayına bağlanır. |

### Başarı tanımı

- Kontrollü hata senaryolarının en az %90'ını tespit etmek.
- Kök neden adaylarında Top-3 doğruluğunu en az %85'e çıkarmak.
- Yanlış pozitif oranını seçilen veri varlıklarında %5'in altında tutmak.
- Her agent iddiasını bir trace, schema diff, metric veya kod değişikliğiyle desteklemek.

---

## 2. Problem tanımı ve kapsam

Modern analitik ve ML sistemlerinde veri; API'ler, mesaj kuyrukları,
ETL/ELT işleri, veri ambarı modelleri, dashboard'lar ve feature store'lar
arasında ilerler. Bir upstream değişiklik teknik hata üretmeden downstream
anlamı bozabilir. Bu sınıf sorunlar 'silent data failure' olarak ele alınır.

### 2.1 Hedeflenen hata sınıfları

| Sınıf | Örnek | Algılama yaklaşımı |
|---|---|---|
| Schema drift | Kolon silindi veya tipi değişti | Contract diff + schema fingerprint |
| Completeness | Null oranı %2'den %45'e çıktı | Oran testi + dinamik eşik |
| Volume | Günlük kayıt sayısı yarıya indi | Seasonal baseline + change point |
| Distribution | Tutarlar 100 kat büyüdü | Robust z-score + PSI/KS |
| Uniqueness | order_id tekrar etmeye başladı | Cardinality ve duplicate testi |
| Freshness | Veri üç saat gecikti | SLA ve watermark kontrolü |
| Referential integrity | Tanımsız customer_id geldi | Foreign-key benzeri kalite testi |
| Semantic drift | Kategori anlamları değişti | Embedding cluster + sözlük kontrolü |

### 2.2 MVP kapsamı

- PostgreSQL tabanlı üç sentetik veri pipeline'ı.
- Batch veri profilleme ve altı kontrollü hata senaryosu.
- Kolon seviyesinde lineage graph ve downstream etki analizi.
- Kanıtlı kök neden raporu ve önerilen dbt/SQL testleri.
- Manuel analiz başlatma ve olay dashboard'u.

### 2.3 Kapsam dışı

- İlk sürümde üretim verisini kendiliğinden değiştirmek.
- Gerçek zamanlı stream processing için milisaniye seviyesinde SLA.
- Tüm veri platformları için evrensel connector desteği.
- LLM'in tek başına anomali kararı vermesi.

---

## 3. Kullanım senaryoları

### 3.1 Veri mühendisi

Bir pipeline çalışması tamamlandığında sistem otomatik profil çıkarır.
Null, volume veya schema anomalisi varsa incident açar. Mühendis, hangi
upstream değişikliğin sorunu doğurduğunu ve düzeltmenin hangi downstream
tabloları etkilediğini tek ekranda görür.

### 3.2 Analitik ekip

Dashboard metriği beklenmedik biçimde değiştiğinde kullanıcı ilgili
metriği seçer. Sistem lineage graph üzerinde geriye doğru ilerler, değişen
kolonları ve son pipeline çalıştırmalarını listeler.

### 3.3 ML ekibi

Feature dağılımı eğitim verisinden uzaklaştığında sistem model girdisindeki
drift'i bildirir. Drift kaynağının veri üretiminden mi, dönüşümden mi
yoksa gerçek kullanıcı davranışından mı geldiğine ilişkin kanıtları ayırır.

### 3.4 Örnek uçtan uca olay

> Birim değişir → Profil sapar → Lineage taranır → Agent açıklar → Onay bekler
>
> *Şekil 1 — Tutar biriminin TL'den kuruşa dönüşmesi halinde olay akışı*

Olayın sonucunda sistem yalnızca 'değer yükseldi' uyarısı vermez.
`orders_api.total_amount` kolonunun son dağılımını, önceki günlerle
oranını, deploy zamanını ve bu kolonu tüketen dönüşümleri ilişkilendirir.

---

## 4. Sistem mimarisi

Mimari beş mantıksal katmana ayrılır. MVP tek repository ve az sayıda
servisle başlayabilir; sınırlar daha sonra bağımsız servisler haline
getirilebilir.

| Katman | Sorumluluk | Örnek teknoloji |
|---|---|---|
| Ingestion | Schema, profil, job ve lineage olaylarını toplar | OpenLineage, REST, SQLAlchemy |
| Detection | Deterministik kontroller ve ML anomalileri | Pandas/Polars, scikit-learn, SciPy |
| Knowledge | Metadata, incident ve ilişkileri saklar | PostgreSQL, pgvector; opsiyonel Neo4j |
| Agent | Kanıt toplama, RCA ve çözüm planı | Agent SDK/LangGraph, MCP araçları |
| Experience | Dashboard, API ve onay akışı | React/Next.js, FastAPI |

### 4.1 Mantıksal bileşenler

| Bileşen | Girdi | Çıktı |
|---|---|---|
| Profiler | Tablo örnekleri ve metadata | ColumnProfile, DatasetProfile |
| Rule Engine | Data contract ve iş kuralları | Deterministik ihlaller |
| Anomaly Engine | Profil zaman serileri | AnomalySignal |
| Lineage Service | Job/run/input/output olayları | Yönlü bağımlılık grafı |
| Incident Correlator | Yakın zamanlı sinyaller | Tekilleştirilmiş incident |
| Root Cause Agent | Incident + graph + log + diff | Kanıtlı hipotezler |
| Repair Planner | Kök neden ve repo bağlamı | Patch/test/rollback planı |
| Approval Service | Riskli öneri | Approve/reject/audit kaydı |

### 4.2 Dağıtım topolojisi

MVP'de bileşenler modüler monolith olarak çalıştırılabilir. Detection
job'ları zamanlanmış batch süreçleridir. API, incident sorgulama ve agent
çalıştırma uçlarını sunar. Üretim ölçeğinde profiler worker'ları kuyruk
üzerinden yatay ölçeklenebilir.

---

## 5. Veri ve olay akışı

### 5.1 Profil çıkarma akışı

> Kaynak → Örnekleme → Profil → Baseline → Sinyal

- Connector, yalnızca gerekli kolonları ve kontrollü örneklemi okur.
- Profiler sayısal, kategorik ve zamansal özetleri çıkarır.
- Yeni profil geçmiş baseline ile karşılaştırılır.
- Eşik aşılırsa açıklanabilir AnomalySignal kaydı oluşturulur.

### 5.2 Incident birleştirme

Aynı pipeline çalışmasına ait onlarca kolon alarmı tek tek kullanıcıya
gösterilmez. Correlator; zaman yakınlığı, ortak upstream job, benzer
sapma yönü ve aynı deploy penceresi kriterleriyle sinyalleri bir incident
altında toplar.

### 5.3 Kök neden arama sırası

| Adım | İşlem | Kanıt |
|---|---|---|
| 1 | Anomalinin başladığı ilk run bulunur | Profile timestamp |
| 2 | Upstream varlıklar graph üzerinden çıkarılır | Lineage edge |
| 3 | Aynı zaman aralığındaki değişiklikler aranır | Schema diff, commit, deploy |
| 4 | Hipotezler deterministik kontrollerle sınanır | SQL/statistical check |
| 5 | Agent hipotezleri puanlar ve açıklar | Evidence reference |
| 6 | Downstream etki ve onarım planı çıkarılır | Impact graph + tests |

---

## 6. Veri modeli

MVP için PostgreSQL yeterlidir. Graph ilişkileri adjacency tablolarında
tutulur; semantik arama için `pgvector` eklenir. Ölçek büyüdüğünde lineage
sorguları Neo4j'e ayrılabilir.

### 6.1 Temel tablolar

| Tablo | Önemli alanlar | Amaç |
|---|---|---|
| datasets | id, namespace, name, type, owner | Tablo, dosya, topic veya feature kaydı |
| columns | id, dataset_id, name, data_type | Kolon metadata'sı |
| jobs | id, namespace, name, code_ref | Pipeline/dönüşüm işi |
| job_runs | id, job_id, status, started_at, ended_at | Çalıştırma geçmişi |
| lineage_edges | source_id, target_id, edge_type | Dataset/job/column bağımlılığı |
| profiles | column_id, run_id, metrics_json | Zaman damgalı kalite profili |
| contracts | dataset_id, version, rules_json | Beklenen schema ve iş kuralları |
| signals | type, severity, score, evidence_json | Tekil anomali sinyali |
| incidents | status, root_cause_json, impact_json | Birleştirilmiş olay |
| agent_runs | trace_id, prompt_version, model, cost | Agent audit ve observability |
| approvals | action_id, decision, actor, timestamp | İnsan onayı kaydı |

> **Uygulama durumu:** `datasets`, `columns`, `jobs`, `job_runs`,
> `lineage_edges`, `profiles`, `contracts` (kod içinde) ve `signals`
> `infra/migrations/001_init.sql`'de (Faz 1); `external_assets` ve
> `lineage_edges.source_type/target_type` `003_lineage.sql`'de (Faz 3);
> `incidents` ve `agent_runs` `004_agent.sql`'de (Faz 4); `actions` (doküman
> tablosunda örtük — API'nin `/actions/{id}` hedeflediği kaynak) ve
> `approvals` `005_actions.sql`'de (Faz 7) kuruludur. Hiçbiri gerçek
> SQL/dbt çalıştırmaz — yalnızca bir insan kararını kaydeder (§14.3).

### 6.2 Profil örneği

```json
{
  "column": "orders.total_amount",
  "run_id": "run_2026_09_04_0900",
  "row_count": 125340,
  "null_ratio": 0.0012,
  "mean": 482.17,
  "stddev": 131.44,
  "p50": 449.90,
  "p95": 729.00,
  "distinct_ratio": 0.417,
  "sample_hash": "sha256:..."
}
```

Ham satırları mümkün olduğunca kalıcı saklamak yerine profil özetleri,
örnek hash'leri ve sınırlı maskelenmiş örnekler tutulur. Bu yaklaşım
maliyeti ve kişisel veri riskini azaltır.

---

## 7. Anomali tespit motoru

Tek bir algoritma bütün veri bozulmalarını güvenilir biçimde
yakalayamaz. Sistem, kural tabanlı kontrolleri ve istatistiksel/ML
yöntemlerini ensemble olarak kullanır.

### 7.1 Deterministik kontroller

| Kontrol | Örnek kural | Avantaj |
|---|---|---|
| Schema | `data_type == decimal(18,2)` | Kesin ve açıklanabilir |
| Null | `null_ratio < 0.01` | İş beklentisini doğrudan uygular |
| Range | `0 <= amount <= 1_000_000` | Aykırı birim ve iş hatasını yakalar |
| Uniqueness | `distinct(order_id) == row_count` | Duplicate'i kesin yakalar |
| Freshness | `now - max(event_time) < 30m` | SLA ihlalini yakalar |

### 7.2 İstatistiksel yöntemler

| Yöntem | Uygun kullanım | Not |
|---|---|---|
| Robust Z-score | Sayısal metriklerde ani sıçrama | Median ve MAD ile outlier'a dayanıklı |
| EWMA | Yavaş değişen ortalamalar | Yeni verilere daha yüksek ağırlık |
| Seasonal baseline | Gün/saat döngülü hacim | Hafta içi/hafta sonunu ayırır |
| Change-point | Kalıcı rejim değişimi | Tek seferlik outlier'dan ayrıştırır |
| KS testi | Sürekli dağılım değişimi | İki örneklem dağılımını karşılaştırır |
| PSI | Feature/popülasyon drift'i | Segment bazında izlenebilir |
| Isolation Forest | Çok değişkenli anomali | Etiket gerektirmeden çalışabilir |

> **Uygulama durumu (Faz 1):** Deterministik kontroller
> (`contracts.py::check_contract`) ve basitleştirilmiş bir robust z-score +
> oransal değişim karşılaştırması (`detector.py::compare_column_profiles`)
> uygulanmıştır. EWMA/seasonal/change-point/PSI/Isolation Forest Faz 2
> kapsamındadır.

### 7.3 Skor birleştirme

Her sinyal 0-1 aralığına normalize edilir. Örnek birleşik skor:

```
risk = 0.35 × contract + 0.25 × magnitude + 0.20 × persistence
     + 0.10 × downstream_impact + 0.10 × model_score
```

Contract ihlali varsa ağırlık yükseltilir. Kritik dashboard veya ML
feature'a giden lineage yolu, severity puanını artırır. Eşikler dataset
bazında ayrı kalibre edilmelidir.

> **Uygulama durumu (Faz 1):** `detector.py::compute_incident_risk`
> yalnızca `contract` ve `magnitude` terimlerini uygular (0.35/0.60,
> 0.25/0.60 olarak yeniden normalize edilmiş); `persistence`,
> `downstream_impact`, `model_score` sırasıyla baseline geçmişi (Faz 2),
> lineage graph (Faz 3) ve Root Cause Agent (Faz 4) gerektirir.

---

## 8. Baseline ve drift stratejisi

### 8.1 Başlangıç baseline'ı

- En az 14-28 sağlıklı run üzerinden oluşturulur.
- Gün, saat ve tatil etkileri ayrı segmentlenir.
- İlk kurulumda yüksek hassasiyet yerine gözlem modu kullanılır.

### 8.2 Baseline güncelleme

Doğrulanan sağlıklı run'lar rolling window'a eklenir. Incident içeren
run'lar baseline'a alınmaz. Kullanıcı gerçek iş değişimini onayladığında
yeni dağılım kontrollü biçimde baseline'a geçirilir; aksi halde sistem
hatayı normalleştirebilir.

### 8.3 Drift ile arıza ayrımı

| Özellik | Gerçek davranış değişimi | Veri arızası |
|---|---|---|
| Süre | Kademeli ve kalıcı olabilir | Genellikle deploy/run anında başlar |
| Kapsam | Birçok ilişkili metriği tutarlı etkiler | Belirli kaynak/kolonlarda yoğunlaşır |
| Schema/log | Teknik hata olmayabilir | Schema diff veya parse hatası görülebilir |
| Doğrulama | İş sahibi onayı gerekir | Deterministik testlerle kanıtlanabilir |

> **Uygulama durumu (Faz 1):** `sentinel bootstrap` komutu en az N sağlıklı
> run çalıştırarak baseline oluşturur (§8.1); baseline karşılaştırması yalnızca
> `fault_id IS NULL AND status = 'success'` olan en güncel run'ı kullanır.
> Rolling window/versioned baseline ve insan onayı akışı Faz 2 kapsamındadır.

---

## 9. Data lineage ve knowledge graph

OpenLineage modeli dataset, job ve run varlıklarını standart biçimde
tanımlar. Sistem bu olayları ingest ederek yönlü bir graph oluşturur.
Kolon seviyesi ilişki bulunmadığında dataset seviyesi ilişki korunur.

### 9.1 Düğüm türleri

`Dataset` · `Column` · `Job` · `Dashboard` · `ML Feature`

### 9.2 Kenar türleri

| Kenar | Anlam |
|---|---|
| READS_FROM | Job bir dataset'ten okur |
| WRITES_TO | Job bir dataset üretir |
| DERIVED_FROM | Kolon başka kolonlardan türetilir |
| FEEDS | Dataset dashboard/model girdisidir |
| DEPLOYED_WITH | Job belirli kod/deploy sürümüyle çalışır |
| TRIGGERED | Bir run başka run'ı tetikler |

### 9.3 Kritik graph sorguları

```sql
-- Downstream etki
MATCH (c:Column {name: 'orders.total_amount'})-[:DERIVED_FROM|FEEDS*1..6]-(x)
RETURN x;

-- İlk olası upstream kaynak
MATCH p=(a)-[:READS_FROM|DERIVED_FROM*1..6]->(target)
WHERE target.id = $anomalous_asset
RETURN p ORDER BY length(p) ASC;
```

Ücretsiz MVP'de aynı sorgular recursive CTE ve adjacency tablolarıyla
uygulanabilir. Neo4j, görselleştirme ve çok adımlı graph traversal
ihtiyacı büyüdüğünde eklenmelidir.

> **Uygulama durumu (Faz 3 — tamamlandı):** Gerçek OpenLineage ingestion
> yerine, `examples/commerce-pipeline`'ın sabit yapısal graf'ı her
> `sentinel run`/`bootstrap` sonunda idempotent olarak (yeniden) bildirilir
> (`orchestrator.py::sync_commerce_lineage`). Traversal saf Python BFS ile
> yapılır (`lineage.py::downstream_impact`/`upstream_sources`) — küçük graf
> için recursive CTE'den daha basit ve test edilebilir; DB katmanı
> (`pipeline.py`) tüm kenarları tek sorguyla belleğe çeker. Dashboard/ML
> Feature düğümleri `external_assets` tablosunda temsil edilir. `sentinel
> lineage graph/impact/upstream` komutlarıyla erişilir. Gerçek OpenLineage
> event ingestion'ı (harici pipeline'lardan otomatik toplama) ve Neo4j'e
> geçiş hâlâ ileri faz kapsamındadır.
>
> **Yön kuralı** (kod ve grameratik isim her zaman örtüşmez, bkz.
> `infra/migrations/003_lineage.sql`): her kenar `source -> target`
> VERİ/ETKİ AKIŞI yönünde saklanır — source bozulursa target etkilenir.
> `READS_FROM: dataset -> job`, `WRITES_TO: job -> dataset`,
> `DERIVED_FROM: origin_column -> derived_column`,
> `FEEDS: dataset/column -> external_asset`.

---

## 10. Root Cause Agent tasarımı

Agent'ın görevi anomali tespit etmek değil, mevcut sinyalleri ve
operasyonel kanıtları araştırarak en olası açıklamayı oluşturmaktır.
Başlangıçta tek agent ve sınırlandırılmış araç seti tercih edilir.

### 10.1 Agent araçları

| Araç | İzin | Döndürdüğü veri |
|---|---|---|
| get_incident | Read | Sinyaller, zaman aralığı, severity |
| query_lineage | Read | Upstream/downstream yollar |
| compare_profiles | Read | Baseline-current farkları |
| get_schema_diff | Read | Kolon ve tip değişiklikleri |
| search_logs | Read | Maskelenmiş log parçaları |
| get_deploy_changes | Read | Commit/deploy metadata |
| run_validation_sql | Sandbox | Sınırlı SELECT sonucu |
| generate_test | Write draft | dbt/SQL test taslağı |
| propose_patch | Write draft | Uygulanmamış patch |

### 10.2 Agent döngüsü

> Plan → Kanıt topla → Hipotez kur → Doğrula → Raporla

Agent en fazla belirlenen tool-call bütçesi kadar ilerler. Kanıt bulamazsa
'belirsiz' sonucu üretir; boşluğu tahminle doldurmaz. Üretim etkili her
işlem approval service tarafından kesilir.

### 10.3 Hipotez puanlama

Her hipotez; zaman uyumu, graph yakınlığı, sapmayı açıklama gücü,
deterministik doğrulama ve karşı kanıt kriterleriyle puanlanır. Nihai
güven skoru yalnızca modelin öznel olasılığı değildir.

> **Uygulama durumu (Faz 4 — çekirdek tamamlandı):** Agent, LLM-öncesi
> tamamen deterministik bir kural motoru olarak uygulandı
> (`rca.py::generate_hypotheses`) — dokümanın "Tasarım ilkesi"ne bilinçli
> bir uzantı: kanıt toplama VE hipotez puanlama LLM olmadan da eksiksiz
> çalışır; LLM yalnızca mevcutsa doğal dil ifadesini rafine eder
> (`llm.py`). Agent döngüsü (§10.2) `analyze.py::analyze_run` içinde:
> `get_incident`/`compare_profiles` → `pipeline.get_incident_context`,
> `get_schema_diff` → `pipeline.get_schema_diff` (profiles tablosundan
> türetilir, ayrı bir tablo gerekmez), `query_lineage` → Faz 3
> `lineage.downstream_impact`. `search_logs`, `get_deploy_changes`,
> `run_validation_sql` **uygulanmadı** — bu demo'da harici log/deploy
> kaynağı ve sandbox SQL çalıştırma altyapısı yok. `generate_test`/
> `propose_patch` (§10.1) `rca.py`'deki `recommended_actions` şablonlarıyla
> karşılanıyor (taslak metin, hiçbiri uygulanmıyor — §14.3).

---

## 11. RAG ve bağlam mühendisliği

RAG katmanı; geçmiş incident'lar, runbook'lar, veri sözleşmeleri, dönüşüm
SQL'leri ve deployment notlarını indeksler. Retrieval sırası metadata
filtresi, lexical arama, vector search ve graph genişletme olarak
tasarlanır.

### 11.1 Hibrit retrieval

| Aşama | Amaç | Örnek filtre |
|---|---|---|
| Metadata | Arama uzayını küçültmek | dataset, owner, tarih, environment |
| BM25 | Hata kodu ve kolon adı bulmak | exact error/identifier |
| Vector | Benzer incident ve açıklama | semantic similarity |
| Graph | Bağımlı varlıkları eklemek | 1-3 hop neighborhood |
| Rerank | En güçlü kanıtları seçmek | cross-encoder veya LLM |

### 11.2 Chunking stratejisi

- SQL: model veya CTE sınırına göre.
- Runbook: başlık ve prosedür adımına göre.
- Log: trace_id ve zaman penceresine göre.
- Incident: problem, kök neden, çözüm ve doğrulama alanlarına göre.

### 11.3 Kaynak zorunluluğu

Nihai rapordaki her önemli iddia `evidence_ids` alanı taşımalıdır. Kaynak
kimliği olmayan iddia sonuç ekranında 'doğrulanmamış çıkarım' etiketiyle
gösterilir.

> **Uygulama durumu:** Kasıtlı olarak ertelendi. Henüz gerçek bir incident
> geçmişi/runbook külliyatı yok — içeriği olmayan bir vector search
> altyapısı kurmak (pgvector, chunking, hibrit retrieval) saf iskelet
> olurdu. §11.3 "kaynak zorunluluğu" ilkesi yine de tam uygulandı: her
> hipotezin `evidence_ids`'i gerçek `signals` satırlarına doğrulanır
> (`rca.py`, `llm.py::_validate_and_merge`) — kaynaksız/hayali bir iddia
> asla `root_causes`'a girmez, `[doğrulanmamış çıkarım]` etiketiyle
> `uncertainties`'e taşınır. Gerçek RAG, incident geçmişi biriktikçe
> (Faz 6+) eklenmelidir.

---

## 12. Structured output ve prompt sözleşmesi

Agent çıktısı serbest metin yerine JSON Schema ile sınırlandırılır. UI
metni bu yapıdan üretilir.

### 12.1 Örnek çıktı

```json
{
  "incident_id": "inc_1042",
  "summary": "total_amount değerleri yaklaşık 100 kat arttı",
  "severity": "high",
  "root_causes": [
    {
      "hypothesis": "Kaynak birimi TL'den kuruşa çevrildi",
      "confidence": 0.96,
      "evidence_ids": ["profile_91", "schema_diff_14"],
      "counter_evidence": []
    }
  ],
  "affected_assets": ["daily_revenue", "finance_dashboard"],
  "recommended_actions": [
    {"type": "sql_patch", "requires_approval": true},
    {"type": "dbt_test", "requires_approval": false}
  ],
  "uncertainties": []
}
```

### 12.2 Sistem prompt ilkeleri

- Yalnızca araçlardan gelen kanıtları kullan.
- Kök neden ile korelasyonu birbirinden ayır.
- Eksik kanıtı açıkça `uncertainties` alanına yaz.
- Üretim değişikliğini doğrudan uygulama; yalnızca öner.
- Ham kişisel veriyi çıktı içine taşıma.
- Maksimum üç güçlü hipotez üret; düşük kaliteli adaylarla listeyi doldurma.

> **Uygulama durumu:** Yapısal şema (`rca.py::IncidentReport.to_dict`)
> §12.1'e birebir uyuyor (`incident_id` yerine `incidents.id` DB'de
> ayrıca tutuluyor). §12.2'nin tüm ilkeleri hem kural motorunda hem LLM
> doğrulama katmanında zorlanıyor: "yalnızca kanıt" ve "kanıtsız iddia
> yasak" → `_validate_and_merge`'in evidence_id kontrolü; "üretim
> değişikliğini uygulama" → `recommended_actions` her zaman taslak,
> LLM bunu değiştiremez; "maksimum üç hipotez" → `MAX_HYPOTHESES=3`
> hem kural motorunda hem LLM birleştirmesinde uygulanıyor.

---

## 13. API tasarımı

REST API, kullanıcı arayüzünü ve zamanlanmış işleri aynı domain
servislerine bağlar. Uzun süren analizler job olarak başlatılır.

| Method | Endpoint | Amaç |
|---|---|---|
| POST | /api/v1/sources | Veri kaynağı tanımlama |
| POST | /api/v1/profile-runs | Profiling işi başlatma |
| GET | /api/v1/profile-runs/{id} | Profiling durumu |
| GET | /api/v1/incidents | Filtreli incident listesi |
| GET | /api/v1/incidents/{id} | Incident, sinyal ve kanıt detayları |
| POST | /api/v1/incidents/{id}/analyze | RCA agent çalıştırma |
| GET | /api/v1/lineage/{assetId} | Upstream/downstream graph |
| POST | /api/v1/actions/{id}/approve | Öneriyi onaylama |
| POST | /api/v1/actions/{id}/reject | Öneriyi reddetme |

### 13.1 Incident oluşturma cevabı

Uzun analiz için API `202 Accepted` ve job kimliği döndürür. UI polling,
Server-Sent Events veya WebSocket ile ilerleme durumunu alabilir. MVP
için polling yeterlidir.

> **Uygulama durumu:** Bu demo ölçeğinde (yüzlerce-binlerce satır) hem
> pipeline run hem RCA analizi saniyeler içinde bittiği için `202
> Accepted` + polling yerine senkron `200 OK` tercih edildi (bkz.
> `apps/api/main.py` modül docstring'i). Gerçek ölçekte (§19.2) bu
> basitleştirme kaldırılmalı.

### 13.2 Idempotency

Profil run ve approval uçları `Idempotency-Key` kabul etmelidir. Aynı
event'in tekrar gelmesi duplicate incident veya iki kez uygulanan aksiyon
üretmemelidir.

> **Uygulama durumu:** Ayrı bir `Idempotency-Key` header'ı yok, ama
> incident üretimi doğal olarak idempotenttir — `incidents.run_id`
> UNIQUE kısıtı sayesinde aynı run'ı tekrar analiz etmek yeni bir
> incident değil, mevcut olanın güncellemesini üretir (`ON CONFLICT
> (run_id) DO UPDATE`, bkz. `pipeline.py::save_incident`).

> **Uygulama durumu (Faz 5 + Faz 7 — çekirdek tamamlandı):**
> `apps/api/main.py` FastAPI ile §13 tablosundaki tüm uçları uyguluyor:
> `/incidents`, `/incidents/{id}`, `/incidents/{id}/analyze`,
> `/lineage/{assetId}`, `/actions/{id}/approve|reject` birebir;
> `/profile-runs` yerine daha geniş kapsamlı `/pipeline-runs` (tam bir
> commerce pipeline run'ı — üç dataset birden). `/sources` (veri kaynağı
> tanımlama) uygulanmadı — MVP'nin tek "kaynağı" `examples/commerce-pipeline`,
> dinamik kaynak ekleme Faz 2+ connector genişlemesini gerektirir.
> `/actions/{id}/approve|reject` bir kararı kaydeder ama **hiçbir zaman
> otomatik uygulama yapmaz** — bkz. §14.3 uygulama notu.
> Dashboard (`apps/web/`) React/Next.js (§18.1) yerine bağımlılıksız
> statik HTML/CSS/vanilla JS ile yazıldı — MVP'de Airflow→manuel,
> Neo4j→adjacency table gibi diğer pragmatik ikamelerle aynı mantık
> (§18.1, §24): sıfır build adımı, `apps/api`'nin StaticFiles'ı üzerinden
> tek process'te servis edilir (`sentinel`'in kendisi gibi doğrudan
> çalıştırılabilir). Swagger UI (`/docs`) API'yi keşfetmek için ayrı bir
> bonus arayüz sağlıyor.

---

## 14. Güvenlik ve gizlilik

Sistem üretim verisine ve operasyon araçlarına bağlanabileceği için
güvenlik, sonradan eklenecek bir özellik değil mimari sınırdır.

### 14.1 Temel kontroller

| Risk | Kontrol |
|---|---|
| Yetkisiz veri okuma | Least privilege, read-only kullanıcı, dataset allowlist |
| Prompt injection | Retrieved içeriği talimat değil veri olarak işaretleme; tool policy |
| Tehlikeli SQL | SELECT-only parser, timeout, row limit, sandbox |
| PII sızıntısı | Ön maskeleme, örnekleme, alan bazlı redaction |
| Agent aşırı yetkisi | Ayrı read ve write tool setleri; approval gate |
| Credential sızıntısı | Secret manager; prompt/log içine anahtar yazmama |
| Yanlış otomatik düzeltme | Dry-run, test DB, rollback ve insan onayı |
| Audit eksikliği | Değiştirilemez trace ve approval kaydı |

### 14.2 Veri minimizasyonu

LLM'e tam tablo göndermek yerine profil özetleri, maskelenmiş örnekler ve
gerekli log parçaları gönderilir. Hassas kolonlar connector seviyesinde
engellenir. Embedding indeksine ham PII konulmaz.

### 14.3 İnsan onayı

`propose_patch` yalnızca taslak üretir. Uygulama aracı ayrı scope
gerektirir. Yüksek riskli işlemde agent run durur, kullanıcıya diff ve
etki raporu gösterilir; approve/reject kararı audit tablosuna yazılır.

> **Uygulama durumu (Faz 7 — tamamlandı):** `rca.py`'nin ürettiği
> `recommended_actions` (taslak — `propose_patch`/`generate_test`, §10.1)
> her `analyze_run` sonunda `actions` tablosuna stabil id'lerle idempotent
> olarak yazılır (`pipeline.py::sync_incident_actions` — aynı hipotez aynı
> aksiyonu ürettiği sürece verilmiş bir karar korunur). `sentinel actions
> approve/reject` ve `POST /api/v1/actions/{id}/approve|reject` (§13
> birebir) kararı `approvals` tablosuna audit kaydı olarak yazar
> (`pipeline.py::decide_action`). **Hiçbir zaman gerçek SQL/dbt
> çalıştırılmaz** — yalnızca bir durum değişikliği (`pending → approved
> | rejected`) kaydedilir; dry-run sandbox ve gerçek execution kasıtlı
> olarak kapsam dışıdır (§22 yol haritasında "v1.5 Repair sandbox ve
> otomatik doğrulama" olarak bir sonraki adım). Dashboard'da (`apps/web`)
> her aksiyon satırında durum rozeti + Onayla/Reddet butonları var;
> karar verilmiş bir aksiyon yeniden tetiklenemez (buton disabled).

---

## 15. Observability ve maliyet kontrolü

Uygulama telemetry'si ile agent telemetry'si aynı trace altında
ilişkilendirilmelidir. OpenTelemetry GenAI semantic conventions model
çağrısı, token, tool-call ve sonuçların ortak isimlerle kaydedilmesini
destekler.

### 15.1 İzlenecek metrikler

| Kategori | Metrikler |
|---|---|
| Pipeline | run süresi, gecikme, başarısızlık, freshness |
| Detection | sinyal sayısı, severity, false-positive, detection latency |
| Agent | tool-call sayısı, başarı, retry, trace süresi |
| Model | input/output token, model, cache hit, maliyet |
| Retrieval | top-k, rerank süresi, evidence coverage |
| Ürün | incident çözüm süresi, kabul edilen öneri oranı |

### 15.2 Bütçe sınırları

- Incident başına maksimum model çağrısı.
- Tool-call ve toplam token bütçesi.
- Benzer incident analizi için sonuç cache'i.
- Basit özetlerde küçük model; belirsiz RCA'da güçlü model.
- Bütçe aşımında kısmi kanıt raporu ve kontrollü durdurma.

> **Uygulama durumu:** Faz 2+ kapsamındadır; `packages/telemetry/` henüz boş.

---

## 16. Test ve değerlendirme stratejisi

Sistem üç ayrı katmanda test edilmelidir: veri tespiti, root-cause
reasoning ve operasyonel güvenlik.

### 16.1 Kontrollü hata kataloğu

| ID | Enjekte edilen hata | Beklenen sonuç |
|---|---|---|
| F01 | Kolon silme | Schema contract alarmı |
| F02 | String → integer tip değişimi | Breaking change |
| F03 | Null oranını %2 → %40 çıkarma | Completeness alarmı |
| F04 | Kayıtları iki kez yükleme | Volume + uniqueness alarmı |
| F05 | TL → kuruş ölçeği | Distribution ve range alarmı |
| F06 | Üç saat veri gecikmesi | Freshness alarmı |
| F07 | Yeni normal kategori | Drift; insan doğrulaması |
| F08 | Log içine prompt injection | Tool policy ihlali olmamalı |

> **Uygulama durumu:** F01-F06 `src/pipeline_sentinel/faults.py` içinde
> uygulanmış ve `evals/incidents/` altında altın test vakaları olarak
> doğrulanmıştır (bkz. repo kökü `README.md` → hızlı başlangıç). F07
> (insan onaylı drift) ve F08 (Root Cause Agent güvenlik testi) sırasıyla
> Faz 2 ve Faz 4 kapsamındadır.

### 16.2 Ölçüm metrikleri

- Detection precision, recall ve F1.
- Mean time to detect (MTTD).
- Root cause Top-1 ve Top-3 accuracy.
- Evidence precision: kullanılan kanıtın iddiayı destekleme oranı.
- Impact recall: etkilenen downstream varlıkları bulma oranı.
- Repair validation pass rate.
- Policy violation rate ve approval bypass sayısı.

> **Uygulama durumu (Faz 6):** `evals/graders/grader.py::compute_metrics`
> bu golden set (F01-F06) üzerinde `detection_recall`,
> `root_cause_top1_accuracy`, `evidence_precision` ve
> `mean_top1_confidence_when_correct`'i hesaplar ve
> `python -m evals.graders.grader` ile yazdırır — halihazırda hepsi
> %100. MTTD ve Top-3 accuracy (yalnızca 1 doğru hipotez ürettiğimiz bu
> senaryolarda anlamsız), repair validation pass rate (Faz 7'nin gerçek
> execution'ı olmadan ölçülemez) ve approval bypass sayısı henüz
> hesaplanmıyor.

### 16.3 Agent eval seti

Her test örneği incident girdisi, erişilebilir araçlar, beklenen kök
neden, kabul edilen alternatif açıklamalar, zorunlu kanıtlar ve yasak
aksiyonları içermelidir. Prompt veya model değişikliğinde eval seti CI
içinde yeniden çalıştırılır.

> **Uygulama durumu (Faz 6 — tamamlandı):** `evals/incidents/*.json`
> her F01-F06 için `expected_signal_types` (zorunlu kanıt tipleri) ve
> `expected_rule_id` + `min_confidence` (beklenen kök neden) taşır.
> Grader hem tespit motorunu (Faz 1-2) hem RCA'yı (Faz 4, `rca.py`)
> DB'siz, saf Python'da çalıştırır ve GitHub Actions'da her push/PR'da
> otomatik koşar (`.github/workflows/ci.yml`). "Kabul edilen alternatif
> açıklamalar" ve "yasak aksiyonlar" alanları henüz yok — yalnızca tek
> bir doğru `rule_id` bekleniyor; alternatif geçerli hipotezler devreye
> girdiğinde (ör. LLM rafinesiyle metin çeşitlendiğinde) eklenmelidir.

---

## 17. Uygulama planı

Fazlar, her aşamanın tek başına gösterilebilir bir çıktı üretmesi için
tasarlanmıştır.

| Faz | Süre | Teslimat |
|---|---|---|
| 1. Veri laboratuvarı | 1 hafta | PostgreSQL, sentetik veri, üç pipeline, hata enjektörü |
| 2. Profiling | 1 hafta | Profil tabloları, contract ve temel anomaly engine |
| 3. Lineage | 1 hafta | OpenLineage ingestion ve etki graph'ı |
| 4. Agent RCA | 1 hafta | Read-only araçlar, structured report, evidence zorunluluğu |
| 5. Dashboard | 1 hafta | Incident listesi, graph ve karşılaştırma ekranı |
| 6. Evaluation | 1 hafta | Hata kataloğu, metrikler ve CI regression testi |
| 7. Repair plan | 1 hafta | Test/patch taslağı ve approval workflow |

> **Uygulama durumu:** Faz 1, Faz 2'nin çekirdeği, Faz 3 (lineage graph,
> basitleştirilmiş), Faz 4'ün çekirdeği (kural tabanlı Root Cause Agent +
> opsiyonel LLM rafinesi; RAG hariç), Faz 5'in çekirdeği (REST API +
> statik dashboard), Faz 6 (RCA eval metrikleri + canlı Postgres CI) ve
> Faz 7'nin çekirdeği (approval workflow — dry-run sandbox/gerçek
> execution hariç) tamamlanmıştır — bkz. §9, §10-§14, §16, §20 uygulama
> notları ve repo kökü `README.md`. Kapsanmayan: gerçek OpenLineage
> ingestion, RAG/vector search, `/sources` API ucu, repair sandbox +
> otomatik execution.

### 17.1 İlk sprint backlog'u

- [x] Docker Compose ile PostgreSQL ve örnek servisleri ayağa kaldır.
- [x] Orders, Customers ve Payments dataset'lerini üret.
- [x] Pipeline run tablosu ve profil veri modelini oluştur.
- [x] Null, row count, distinct, min/max ve quantile profiler'ını yaz.
- [x] F01-F06 hata senaryolarını parametreli hale getir.
- [x] CLI üzerinden profiling ve anomaly raporu üret.

---

## 18. Repository yapısı

```
pipeline-sentinel/
├── apps/
│   ├── api/            # REST API
│   └── web/             # Dashboard
├── services/
│   ├── profiler/        # Veri profilleme
│   ├── detector/        # Kural + ML motoru
│   ├── lineage/          # OpenLineage ingestion
│   └── agent/            # RCA ve repair planner
├── packages/
│   ├── contracts/         # JSON Schema ve domain modelleri
│   ├── connectors/         # PostgreSQL, file, dbt
│   └── telemetry/           # OpenTelemetry yardımcıları
├── examples/
│   └── commerce-pipeline/    # Sentetik demo ortamı
├── evals/
│   ├── incidents/               # Golden test vakaları
│   └── graders/                  # Deterministik/LLM grader'lar
├── infra/
│   ├── docker-compose.yml
│   └── migrations/
└── docs/
```

Modüler sınırlar korunmalı fakat MVP'de mikroservis zorunlu
tutulmamalıdır. Erken aşamada dağıtık mimari, ürün probleminin önüne
geçebilir.

> **Uygulama durumu:** Bu repo yapısı birebir kuruludur. Faz 1'in çalışan
> kodu `src/pipeline_sentinel/` altında modüler monolith olarak yaşıyor
> (§4.2, §24); `services/*` ve `apps/*` altındaki `README.md` dosyaları
> hangi fazda hangi koda çıkacaklarını işaret eder.

### 18.1 Önerilen teknoloji seçenekleri

| Alan | MVP | Ölçek büyüdüğünde |
|---|---|---|
| API | FastAPI | FastAPI worker'lar veya alternatif backend |
| Profiling | Polars/Pandas | Spark/Flink |
| Queue | PostgreSQL job table | RabbitMQ/Kafka |
| Metadata | PostgreSQL | PostgreSQL + object storage |
| Graph | Adjacency tables | Neo4j |
| Vector | pgvector | Yönetilen vector store |
| Scheduler | GitHub Actions/manual | Airflow/Dagster |
| Telemetry | OpenTelemetry | Collector + Grafana/Tempo |

---

## 19. Deployment mimarisi

### 19.1 Ücretsiz portföy demosu

| Bileşen | Öneri | Sınır |
|---|---|---|
| Web + hafif API | Vercel Hobby | Kişisel/non-commercial ve kullanım limitleri |
| Database + pgvector | Supabase Free | 500 MB; inaktif proje duraklatılabilir |
| Graph | PostgreSQL adjacency veya AuraDB Free | SLA ve gelişmiş özellik yok |
| Scheduler | GitHub Actions/manual trigger | Sürekli worker değildir |
| LLM | Sınırlı ücretsiz kota veya BYOK | Kota değişebilir |

Ücretsiz demoda Airflow ve sürekli worker host edilmez. Profiling
kullanıcı isteği veya zamanlanmış kısa job ile çalışır. Tam mimari Docker
Compose ile yerelde gösterilir.

### 19.2 Üretim sürümü

- Container tabanlı uzun süreli profiler/detector worker'ları.
- Managed PostgreSQL, object storage ve yedekleme.
- OpenTelemetry Collector ve merkezi gözlemleme.
- Secret manager, private network ve RBAC.
- Queue tabanlı retry, dead-letter ve backpressure.
- Tenant izolasyonu ve müşteri bazlı encryption key.

---

## 20. CI/CD ve operasyon

| Aşama | Kontroller |
|---|---|
| Pull request | Lint, unit test, schema validation, secret scan |
| Build | Container build, SBOM, dependency scan |
| Evaluation | F01-F08 hata vakaları ve agent regression seti |
| Staging | Sentetik pipeline, dry-run repair, load test |
| Production | Manuel onay, migration, canary |
| Post-deploy | Anomaly metric, cost, latency ve rollback gözlemi |

### 20.1 Migration güvenliği

Database migration'ları ileri ve geri uyumluluk açısından test edilir.
Agent tarafından üretilen migration doğrudan çalıştırılmaz. Staging
verisi üzerinde dry-run ve contract testleri geçmeden onaya sunulmaz.

> **Uygulama durumu (Faz 6):** `.github/workflows/ci.yml` iki job
> çalıştırır — "Pull request" satırının basitleştirilmiş karşılığı: (1)
> `test`: unit testler + F01-F06 eval seti (DB'siz, saniyeler içinde);
> (2) `integration`: gerçek bir `postgres:16` service container'ına
> karşı `sentinel migrate` + tüm integration test paketi (orchestrator,
> lineage, RCA, API — 20 test). Lint/secret scan/SBOM/dependency scan,
> Build/Staging/Production/Post-deploy aşamaları henüz yok — bu bir
> kütüphane/CLI demo'su, henüz deploy edilen bir servis değil.

---

## 21. Risk kaydı ve önlemler

| Risk | Olasılık | Etki | Önlem |
|---|---|---|---|
| Çok fazla yanlış alarm | Yüksek | Yüksek | Gözlem modu, dataset bazlı eşik, feedback |
| Agent yanlış kök neden üretir | Orta | Yüksek | Evidence zorunluluğu, deterministik doğrulama |
| Maliyet hızla artar | Orta | Orta | Model routing, cache, tool/token bütçesi |
| PII modele gider | Orta | Yüksek | Connector seviyesinde redaction ve allowlist |
| Lineage eksik kalır | Yüksek | Orta | Confidence, manuel ilişki ekleme, dbt/OpenLineage |
| Gerçek değişim hata sanılır | Orta | Orta | İş sahibi onayı ve baseline versioning |
| Auto-fix zarar verir | Düşük | Çok yüksek | Draft-only MVP, sandbox, approval, rollback |
| Ücretsiz katman uyur | Yüksek | Düşük | Demo uyarısı, manuel wake-up, local compose |

### En kritik ürün riski

Kullanıcı güvenini en hızlı bozan durum yanlış pozitif alarm yağmurudur.
Bu nedenle MVP'nin başarısı kaç farklı model kullandığıyla değil, az
sayıdaki dataset üzerinde doğru ve açıklanabilir alarm üretmesiyle
ölçülmelidir.

---

## 22. Gelecek sürümler

| Sürüm | Yetenek |
|---|---|
| v1.1 | Slack/Teams incident bildirimi ve geri bildirim toplama |
| v1.2 | dbt manifest'ten kolon lineage çıkarımı |
| v1.3 | Kafka topic ve streaming freshness desteği |
| v1.4 | ML feature drift ve model performans korelasyonu |
| v1.5 | Repair sandbox ve otomatik doğrulama |
| v2.0 | Onaylı düşük riskli self-healing aksiyonları |

### 22.1 Ürünleştirme seçenekleri

- Açık kaynak core + ücretli managed platform.
- Data platform ekipleri için self-hosted enterprise sürüm.
- Belirli bir sektör veya veri stack'i için dikey çözüm.
- Mevcut observability ürünlerine agentic RCA eklentisi.

---

## 23. MVP kabul kriterleri

| # | Kabul kriteri |
|---|---|
| 1 | Üç örnek pipeline başarıyla çalışıyor ve lineage görüntüleniyor. |
| 2 | F01-F06 hatalarının en az beşi doğru biçimde tespit ediliyor. |
| 3 | Her alarm mevcut ve baseline profilini karşılaştırmalı gösteriyor. |
| 4 | RCA raporu en az bir doğrulanabilir evidence_id içeriyor. |
| 5 | Downstream etki listesi graph verisinden üretiliyor. |
| 6 | Agent üretim verisini değiştiren hiçbir araca sahip değil. |
| 7 | Model/tool çağrıları trace_id ile gözlemlenebiliyor. |
| 8 | Ücretsiz demo URL'si ve yerel Docker Compose kurulumu çalışıyor. |
| 9 | Test sonuçları precision, recall ve RCA accuracy ile raporlanıyor. |

### MVP tamamlandığında beklenen demo

Kullanıcı normal pipeline'ı çalıştırır, ardından 'TL → kuruş' hatasını
enjekte eder. Sistem sapmayı tespit eder, graph üzerinde kaynak ve
etkilenen varlıkları gösterir, agent kanıtlı kök neden raporu üretir ve
önerilen SQL/dbt testini insan onayına sunar.

> **Uygulama durumu:** Kriter 2 ve 3, ve kriter 9'un tespit kısmı (kriter
> 1, 4-8 lineage/RCA/dashboard fazlarını bekliyor) Faz 1 ile karşılanmıştır
> — `sentinel run --fault F05` bu senaryoyu birebir çalıştırır.

---

## 24. Mimari karar özeti

| Karar | Gerekçe |
|---|---|
| LLM anomaly detector olmayacak | Sayısal kalite kontrolleri daha ölçülebilir ve açıklanabilir. |
| İlk sürüm tek agent olacak | Multi-agent karmaşıklığı ve hata yüzeyi gereksiz. |
| MVP PostgreSQL kullanacak | Metadata, vector ve basit graph ihtiyacını tek servis karşılar. |
| Repair önce taslak olacak | Üretim riskini sınırlamak ve güven oluşturmak. |
| Evidence zorunlu olacak | Halüsinasyonu görünür ve ölçülebilir hale getirmek. |
| Baseline versioned olacak | Gerçek iş değişimini hata baseline'ına karıştırmamak. |
| Ücretsiz demo sade olacak | Sürekli worker ve Airflow maliyetini önlemek. |

### Son teknik değerlendirme

Pipeline Sentinel AI'ın farklılaştırıcı noktası, veri anomalisi yakalayan
mevcut yöntemlerin üzerine yalnızca sohbet arayüzü eklemek değildir.
Sistem; tespit, lineage, operasyonel kanıt, agent reasoning, güvenli onay
ve regression evaluation katmanlarını tek olay modeli altında birleştirir.

---

## 25. Kaynaklar

Aşağıdaki birincil dokümanlar tasarım kararlarının güncel standartlarla
uyumunu destekler:

- OpenLineage — Veri lineage standardı ve dataset/job/run modeli
- OpenLineage Object Model
- dbt — Model Contracts
- OpenAI — Agents SDK rehberi
- OpenAI Agents SDK — Human-in-the-loop
- OpenTelemetry — GenAI Observability
- Model Context Protocol — Giriş
- MCP Security Best Practices
- Vercel fiyatlandırma ve Hobby sınırları
- Supabase fiyatlandırma ve Free plan sınırları
- Neo4j AuraDB fiyatlandırması

### Not

Servis fiyatları ve ücretsiz katman limitleri zamanla değişebilir.
Uygulamaya başlamadan önce ilgili sağlayıcının güncel fiyatlandırma
sayfası tekrar kontrol edilmelidir.

---

*Pipeline Sentinel AI — Güvenilir veri sistemleri için ölçülebilir, kanıtlı
ve kontrollü yapay zeka. Sürüm 1.0 | Eylül 2026*
