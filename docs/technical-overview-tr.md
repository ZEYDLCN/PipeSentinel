# Pipeline Sentinel AI — Teknik Genel Bakış

Bu doküman, Pipeline Sentinel AI projesinin hangi problemi çözdüğünü, hangi
teknolojileri kullandığını ve uygulamanın uçtan uca nasıl çalıştığını mevcut
`main` branch'indeki gerçek kod davranışına göre açıklar.

## 1. Projenin amacı

Bir veri pipeline'ının teknik olarak başarılı tamamlanması, ürettiği verinin
doğru olduğu anlamına gelmez. Job hata vermeden çalışsa bile aşağıdaki sessiz
veri arızaları oluşabilir:

- Beklenen bir kolonun kaldırılması veya veri tipinin değiştirilmesi
- Null oranının aniden yükselmesi
- Aynı batch'in iki kez yüklenmesi
- Para gibi sayısal değerlerin yanlış birimle gönderilmesi
- Veri hacminin beklenmedik biçimde değişmesi
- Verinin SLA'dan geç gelmesi
- Kategorik alanlara beklenmeyen değerler girmesi

Pipeline Sentinel bu sorunları verinin kendisini inceleyerek yakalar. Tespit
ettiği sinyalleri lineage grafıyla ilişkilendirir, kanıta dayalı bir kök neden
hipotezi üretir ve uygulanmadan önce insan kararı bekleyen çözüm taslakları
oluşturur.

Projenin temel yaklaşımı şudur:

> Anomali kararını deterministik kurallar ve istatistiksel karşılaştırmalar
> verir. LLM zorunlu değildir; etkinleştirilirse yalnızca mevcut kanıtları daha
> anlaşılır bir kök neden metnine dönüştürür.

## 2. Sistem ne yapar?

Sistem şu yetenekleri tek bir olay modeli altında birleştirir:

1. `customers`, `orders` ve `payments` için sentetik commerce verisi üretir.
2. İstenirse F01–F06 kontrollü hata senaryolarından birini enjekte eder.
3. Üretilen batch'i PostgreSQL'e yükler.
4. Her kolonun kalite ve dağılım profilini çıkarır.
5. Veriyi data contract kurallarıyla kontrol eder.
6. Profili en son sağlıklı run'ın profiliyle karşılaştırır.
7. Schema, completeness, uniqueness, range, freshness, volume ve distribution
   sinyalleri üretir.
8. Dataset, job, kolon ve harici varlıklar arasındaki lineage grafını kurar.
9. Sinyalleri kanıt olarak kullanarak kök neden analizi yapar.
10. SQL patch, dbt testi veya alarm gibi aksiyon taslakları oluşturur.
11. Bu aksiyonların onay veya ret kararlarını audit kaydıyla saklar.
12. Sonuçları CLI, REST API ve web dashboard üzerinden sunar.

## 3. Uçtan uca mimari

```mermaid
flowchart LR
    U[CLI veya Dashboard] --> O[Orchestrator]
    O --> S[Sentetik veri üretici]
    S --> F{Fault seçildi mi?}
    F -->|Evet| FI[F01-F06 hata enjeksiyonu]
    F -->|Hayır| B[Sağlıklı batch]
    FI --> L[PostgreSQL commerce tabloları]
    B --> L
    L --> P[Profiler]
    P --> C[Contract kontrolleri]
    P --> D[Baseline karşılaştırması]
    C --> SG[Anomali sinyalleri]
    D --> SG
    SG --> DB[(Metadata PostgreSQL)]
    DB --> RCA[Root Cause Agent]
    LG[Lineage graph] --> RCA
    RCA --> I[Incident ve aksiyon taslakları]
    I --> A[İnsan onayı / ret]
    A --> DB
    DB --> API[FastAPI]
    API --> WEB[Statik web dashboard]
```

Uygulama bir **modüler monolit** olarak tasarlanmıştır. Domain mantığı
`src/pipeline_sentinel/` altında saf veya veritabanına bağlı Python modüllerine
ayrılır. FastAPI ve CLI aynı çekirdek modülleri kullanır; birbirinden farklı
iş kuralları uygulamaz.

## 4. Kullanılan teknolojiler

| Teknoloji | Kullanım amacı |
|---|---|
| Python 3.11+ | Uygulamanın ana çalışma dili |
| Pandas | DataFrame tabanlı profil çıkarma ve contract kontrolleri |
| NumPy | Sentetik veri dağılımları ve kontrollü hata üretimi |
| SciPy | İstatistiksel yöntemler için proje bağımlılığı; mevcut temel detector ağırlıklı olarak kendi z-score/oran hesaplarını kullanır |
| Faker | Gerçekçi sentetik müşteri e-postaları üretme |
| PostgreSQL 16 | Commerce verisi, metadata, profil, sinyal, lineage, incident ve audit kayıtları |
| SQLAlchemy 2 | PostgreSQL bağlantısı, transaction ve SQL çalıştırma katmanı |
| psycopg 3 | SQLAlchemy'nin PostgreSQL sürücüsü |
| FastAPI | REST API |
| Pydantic | API request/response şemaları |
| Uvicorn | ASGI uygulama sunucusu |
| Typer | `sentinel` CLI komutları |
| Rich | CLI tablo, panel ve renkli rapor çıktıları |
| Vanilla HTML/CSS/JavaScript | Build gerektirmeyen statik dashboard |
| Docker Compose | Yerel PostgreSQL servisini çalıştırma |
| Pytest | Birim ve entegrasyon testleri |
| GitHub Actions | Unit, eval ve canlı PostgreSQL entegrasyon CI'ı |
| Anthropic SDK, opsiyonel | Kanıtlı RCA metnini LLM ile rafine etme |

Node.js veya frontend build zinciri yoktur. Web arayüzü aynı origin'deki
`/api/v1` endpoint'lerini `fetch` ile çağırır ve FastAPI tarafından `/`
adresinde statik olarak sunulur.

## 5. Repository yapısı

```text
Pipeline Sentinel/
├── apps/
│   ├── api/                 # FastAPI endpoint'leri ve Pydantic şemaları
│   └── web/                 # Statik dashboard
├── docs/                    # Tasarım ve teknik dokümantasyon
├── evals/
│   ├── incidents/           # F01-F06 golden test vakaları
│   └── graders/             # Detection ve RCA değerlendirme kodu
├── infra/
│   ├── docker-compose.yml   # PostgreSQL servisi
│   └── migrations/          # 001-005 SQL migration'ları
├── packages/                # Gelecekte ayrıştırılabilecek paket sınırları
├── services/                # Gelecekte servisleşebilecek modül sınırları
├── src/pipeline_sentinel/   # Çalışan domain ve orkestrasyon kodu
├── tests/                   # Unit ve PostgreSQL integration testleri
├── pyproject.toml           # Paket, bağımlılık ve pytest ayarları
└── README.md                # Hızlı başlangıç
```

`services/` ve `packages/` altındaki klasörler bugün bağımsız çalışan
mikroservisler değildir. Gerçek kod modüler monolit içindedir; bu klasörler
ilerideki ayrıştırma sınırlarını belgeler.

## 6. Çekirdek modüller

### 6.1 `synthetic.py` — veri üretimi

Üç DataFrame oluşturur:

- `customers`: müşteri kimliği, e-posta, ülke, kayıt zamanı ve aktiflik
- `orders`: sipariş kimliği, müşteri, durum, tutar, para birimi ve zaman
- `payments`: ödeme kimliği, sipariş, tutar, yöntem ve ödeme zamanı

Sipariş tutarları yaklaşık `482.17` ortalama ve `131.44` standart sapma ile
normal dağılımdan üretilir ve negatif değerler engellenir. Aynı seed verilirse
çıktı büyük ölçüde tekrarlanabilir olur. Kimlik başlangıçları veritabanındaki
mevcut maksimum kimliklere göre ilerletilir.

### 6.2 `faults.py` — hata enjeksiyonu

Her fault saf bir DataFrame dönüşümüdür; doğrudan veritabanı işlemi yapmaz.
Fault yalnızca hedef dataset'i değiştirir.

| ID | Hedef | Enjekte edilen hata | Beklenen ana sinyal |
|---|---|---|---|
| F01 | `orders` | `customer_id` kolonunu kaldırır | `schema_missing_column` |
| F02 | `orders` | `status` değerlerini string'den integer koda çevirir | `schema_type_mismatch`, `semantic_drift` |
| F03 | `orders` | `total_amount` null oranını yaklaşık %40 yapar | `completeness` |
| F04 | `orders` | Satırları tekrar ekleyerek duplicate load üretir | `uniqueness`, `volume` |
| F05 | `orders` | `total_amount` değerlerini 100 ile çarpar | `range`, `distribution` |
| F06 | `orders` | `created_at` değerlerini üç saat geriye çeker | `freshness` |

### 6.3 `profiler.py` — veri profilleme

Her kolon için JSON uyumlu bir profil üretir:

- Satır sayısı
- Null oranı
- Distinct oranı
- Deterministik örnek hash'i
- Sayısal kolonlarda mean, population stddev, min, max, p50, p95 ve p99
- Tarih kolonlarında minimum ve maksimum zaman
- Kategorik kolonlarda en sık beş değer ve adetleri

Örnek hash ham veriyi saklamak yerine seçilmiş değerlerden SHA-256 üretir.
Bu, profil karşılaştırmasına yardımcı olurken kişisel verinin metadata'ya
kopyalanmasını azaltır.

### 6.4 `contracts.py` — deterministik veri kuralları

Contract iki bölümden oluşur:

- `expected_schema`: kolon adı ve beklenen temel tip
- `rules`: not-null, range, uniqueness, freshness, allowed-values ve
  referential-integrity kuralları

Varsayılan contract'lar Python sözlükleri olarak `DEFAULT_CONTRACTS` içinde
tanımlıdır. Veritabanında bir `contracts` tablosu bulunmasına rağmen mevcut
çalışma akışı bu varsayılan contract'ları tabloya yazıp oradan okumaz.

Contract ihlalleri bir `Violation` nesnesine dönüşür. Her ihlal; kural tipi,
kolon, açıklama, severity ve ölçülebilir evidence taşır.

### 6.5 `detector.py` — anomali tespiti

Detector iki sinyal kaynağını birleştirir:

1. **Contract sinyalleri:** Kesin schema veya veri kuralı ihlalleri
2. **Baseline sinyalleri:** Güncel profil ile önceki sağlıklı profil arasındaki
   istatistiksel değişimler

Mevcut baseline, 14 run'ın ortalaması değildir. `bootstrap --runs 14` sağlıklı
bir geçmiş oluşturur; aktif karşılaştırmada aynı job'a ait **en son başarılı,
fault içermeyen run** seçilir.

Başlıca baseline kontrolleri:

- Null oranı farkı varsayılan olarak `0.05` üzerinde ise completeness sinyali
- Sayısal ortalama baseline standart sapmasından en az `3` z-score uzaksa
  distribution sinyali
- Yeterli kardinaliteye sahip, identifier olmayan kolonlarda distinct oranı
  en az %50 değişirse cardinality drift
- Satır sayısı en az %30 değişirse volume sinyali

`*_id` ve `id` kolonlarının ortalaması doğal olarak sürekli değişebileceği
için bu kolonlarda dağılım ve distinct karşılaştırmaları atlanır.

Her sinyal `0–1` arasında bir skor alır. Incident risk skoru mevcut sürümde
contract varlığı ve en büyük sinyal magnitude'u kullanılarak hesaplanır:

```text
risk = (0.35 / 0.60) × contract_term
     + (0.25 / 0.60) × max_signal_score
```

Sonuç `1.0` ile sınırlandırılır. Severity eşikleri:

| Skor | Severity |
|---|---|
| `>= 0.85` | critical |
| `>= 0.65` | high |
| `>= 0.40` | medium |
| `< 0.40` | low |

### 6.6 `pipeline.py` — PostgreSQL kalıcılığı

Bu modül saf domain modülleriyle PostgreSQL arasındaki IO katmanıdır:

- Dataset, kolon ve job kaydı
- Job run başlatma ve bitirme
- DataFrame yükleme
- Profil ve sinyal kaydı
- Baseline profilini okuma
- Lineage düğümü ve kenarı çözümleme
- Incident context toplama
- Incident, agent run, action ve approval kaydı

Dataset, job, incident ve action kayıtlarının önemli bölümü unique constraint
ve `ON CONFLICT` kullanılarak idempotent hale getirilmiştir.

### 6.7 `orchestrator.py` — pipeline koordinasyonu

Bir `execute_pipeline_run` çağrısında aşağıdaki sıra izlenir:

```mermaid
sequenceDiagram
    participant Client as CLI/API
    participant O as Orchestrator
    participant G as Generator/Faults
    participant DB as PostgreSQL
    participant P as Profiler
    participant D as Detector
    participant L as Lineage

    Client->>O: execute_pipeline_run(fault_id, boyut, seed)
    O->>DB: Mevcut maksimum kimlikleri oku
    O->>G: customers/orders/payments üret
    opt fault_id verildi
        O->>G: Hedef DataFrame'e fault uygula
    end
    loop customers, orders, payments
        O->>DB: Dataset, kolon ve job kaydet
        O->>DB: job_run başlat
        O->>DB: DataFrame'i commerce tablosuna append et
        O->>P: Güncel kolon profillerini çıkar
        O->>DB: Profilleri kaydet ve baseline oku
        O->>D: Contract + baseline detection çalıştır
        O->>DB: Sinyalleri kaydet, run'ı success yap
    end
    O->>L: Commerce lineage kenarlarını senkronize et
    O-->>Client: Run ID'leri ve detection raporları
```

Her dataset'in ayrı `job_run` kaydı vardır. Üç dataset tek bir global database
transaction içinde çalışmaz; yardımcı fonksiyonlar kendi transaction'larını
açar. Bu nedenle üretim ölçeğinde hata yönetimi, retry ve bütün batch için
atomicity ayrıca tasarlanmalıdır.

### 6.8 `lineage.py` — etki grafı

Lineage kenarları daima veri/etki akışı yönünde saklanır:

```text
upstream source → downstream target
```

Kenar tipleri:

- `READS_FROM`: dataset → onu okuyan job
- `WRITES_TO`: job → ürettiği dataset
- `DERIVED_FROM`: kaynak kolon → türetilmiş kolon
- `FEEDS`: dataset/kolon → dashboard veya ML feature

Graf küçük olduğu için kenarlar PostgreSQL'den belleğe alınır ve breadth-first
search ile gezilir:

- `downstream_impact`: Bir düğüm bozulursa etkilenecek varlıklar
- `upstream_sources`: Bir düğümü etkileyebilecek kaynaklar

Varsayılan maksimum traversal derinliği 6 hop'tur. Commerce demosunda örneğin
`orders.total_amount`, `payments.amount`, `finance_dashboard` ve
`revenue_forecast_model` ile ilişkilidir.

### 6.9 `rca.py` — Root Cause Agent

RCA girdisi ham tablo satırları değil, aşağıdaki kanıt paketidir:

- Run bilgisi ve fault kimliği
- Kaydedilmiş anomaly sinyalleri
- Schema diff
- Lineage üzerinden hesaplanan etkilenen varlıklar

Kural motoru sinyal kombinasyonlarını hipotezlere dönüştürür:

| Sinyal veya kombinasyon | RCA kuralı |
|---|---|
| Eksik kolon | `schema_drop` |
| Tip uyumsuzluğu | `schema_type_change` |
| Range + distribution | `scale_error` |
| Completeness | `completeness_gap` |
| Volume + uniqueness | `duplicate_load` |
| Freshness | `freshness_sla_breach` |
| Açıklanamayan dağılım | `unexplained_distribution_shift` |

Her hipotez gerçek `signals.id` değerlerini `evidence_ids` olarak taşır. En
fazla üç güçlü hipotez rapora girer; minimum güven eşiği `0.4` değeridir. Eşik
altındaki veya ilk üç dışındaki adaylar tamamen kaybolmak yerine
`uncertainties` alanına yazılır.

En yüksek güvenli hipoteze göre yalnızca **taslak** aksiyonlar oluşturulur.
Örneğin scale error için ölçeği düzeltme SQL taslağı ve range dbt testi
önerilir.

### 6.10 `llm.py` — opsiyonel LLM rafinesi

`ANTHROPIC_API_KEY` yoksa sistem tamamen deterministik çalışır. Anahtar ve
`anthropic` ekstra paketi varsa LLM çağrısı yapılabilir.

LLM güvenlik sınırları:

- Girdi açık biçimde "veri, talimat değil" olarak işaretlenir.
- LLM yeni evidence üretemez.
- Dönen `evidence_ids`, veritabanındaki bilinen sinyal kimlikleriyle doğrulanır.
- Doğrulanabilir kanıtı olmayan hipotez root cause listesine alınmaz.
- Severity, affected assets ve recommended actions LLM tarafından değiştirilemez.
- Ağ, parse veya validation hatasında deterministik rapora geri dönülür.
- LLM hatası pipeline veya CLI çalışmasını durdurmaz.

### 6.11 `analyze.py` — RCA orkestrasyonu

RCA akışı aşağıdaki döngüyü uygular:

```text
Planla → Kanıt topla → Hipotez kur → Doğrula → Raporla
```

`analyze_run`:

1. Incident context'i PostgreSQL'den toplar.
2. En yüksek skorlu sinyalin düğümünden downstream etkiyi hesaplar.
3. Deterministik RCA raporu üretir.
4. Uygunsa LLM ile metni rafine eder.
5. Run başına tek incident olacak şekilde incident'ı upsert eder.
6. Taslak aksiyonları stabil kimliklerle senkronize eder.
7. Her analiz çağrısı için ayrı bir `agent_runs` audit kaydı oluşturur.

### 6.12 Approval workflow

Her önerilen aksiyon `pending`, `approved` veya `rejected` durumundadır.
Onay/ret işlemi:

1. `approvals` tablosuna actor, karar, not ve zaman yazar.
2. `actions.status` değerini günceller.
3. Gerçek SQL, dbt komutu veya veri değişikliği çalıştırmaz.

Aynı incident yeniden analiz edildiğinde aynı tip ve açıklamaya sahip action
yeniden oluşturulmaz; daha önce verilmiş karar korunur.

## 7. PostgreSQL veri modeli

Veri modeli dört mantıksal gruptur.

### Commerce tabloları

- `commerce.customers`
- `commerce.orders`
- `commerce.payments`

`orders` ve `payments` landing/staging davranışını göstermek için append-only
tasarlanmıştır. Bazı kalite kuralları bilinçli olarak DB constraint'i değildir;
örneğin duplicate kayıtların veritabanı tarafından reddedilmesi yerine Sentinel
tarafından uniqueness sinyali olarak yakalanması hedeflenir.

### Metadata ve detection

- `datasets`: İzlenen veri varlıkları
- `columns`: Dataset kolon kataloğu
- `jobs`: Pipeline işleri
- `job_runs`: Çalıştırma geçmişi
- `profiles`: Run ve kolon bazında profil JSON'u
- `signals`: Anomali sinyalleri ve evidence JSON'u
- `contracts`: Sözleşme saklamak için hazırlanmış tablo

### Lineage

- `lineage_edges`: Polimorfik source/target kenarları
- `external_assets`: Dashboard ve ML feature gibi terminal varlıklar

### Agent ve approval

- `incidents`: Run başına RCA raporu
- `agent_runs`: Her analiz çağrısının trace/audit kaydı
- `actions`: Kalıcı aksiyon taslakları
- `approvals`: İnsan kararlarının audit geçmişi

```mermaid
erDiagram
    DATASETS ||--o{ COLUMNS : contains
    DATASETS ||--o{ CONTRACTS : governed_by
    JOBS ||--o{ JOB_RUNS : executes
    JOB_RUNS ||--o{ PROFILES : produces
    COLUMNS ||--o{ PROFILES : described_by
    JOB_RUNS ||--o{ SIGNALS : emits
    DATASETS ||--o{ SIGNALS : has
    DATASETS ||--o{ INCIDENTS : has
    JOB_RUNS ||--o| INCIDENTS : analyzed_as
    INCIDENTS ||--o{ AGENT_RUNS : analyzed_by
    INCIDENTS ||--o{ ACTIONS : recommends
    ACTIONS ||--o{ APPROVALS : audited_by
```

Migration dosyaları `infra/migrations/001_init.sql` ile
`005_actions.sql` arasında dosya adına göre sırayla uygulanır. Ayrı bir Alembic
version tablosu yoktur; migration SQL'leri `IF NOT EXISTS` ve uyumlu `ALTER`
ifadeleriyle yeniden çalıştırılabilir olacak şekilde hazırlanmıştır.

## 8. API katmanı

FastAPI uygulaması `apps/api/main.py` içindedir. Endpoint'ler senkron çalışır;
uzun işleri kuyruğa bırakıp `202 Accepted` dönme modeli henüz uygulanmamıştır.

| Method | Endpoint | Amaç |
|---|---|---|
| GET | `/api/v1/health` | Process sağlık cevabı |
| GET | `/api/v1/datasets` | Dataset ve son run durumları |
| GET | `/api/v1/faults` | F01-F06 kataloğu |
| POST | `/api/v1/pipeline-runs` | Sağlıklı veya fault içeren run çalıştırma |
| POST | `/api/v1/bootstrap` | Sağlıklı baseline geçmişi üretme |
| GET | `/api/v1/pipeline-runs/{run_id}` | Run, sinyal ve severity detayı |
| POST | `/api/v1/pipeline-runs/{run_id}/analyze` | Run üzerinden ilk RCA analizi |
| GET | `/api/v1/incidents` | Incident listeleme ve filtreleme |
| GET | `/api/v1/incidents/{incident_id}` | Incident, evidence ve action detayı |
| POST | `/api/v1/incidents/{incident_id}/analyze` | Incident run'ını yeniden analiz etme |
| GET | `/api/v1/incidents/{incident_id}/actions` | Incident aksiyonları |
| GET | `/api/v1/lineage/{asset_id}` | Upstream ve downstream traversal |
| POST | `/api/v1/actions/{action_id}/approve` | Aksiyonu onaylama |
| POST | `/api/v1/actions/{action_id}/reject` | Aksiyonu reddetme |
| GET | `/api/v1/actions/{action_id}/approvals` | Aksiyon audit geçmişi |

Lineage `asset_id` örnekleri:

```text
orders
orders.total_amount
payments.amount
```

`/api/v1/health` yalnızca uygulama process'inin cevap verdiğini gösterir;
PostgreSQL'e ayrıca health query çalıştırmaz. Dataset gibi DB kullanan bir
endpoint'in başarılı cevabı uygulama + DB bağlantısını birlikte doğrular.

## 9. Dashboard

Dashboard `apps/web/` altında statik HTML, CSS ve JavaScript'ten oluşur.
FastAPI bu klasörü kök path'e mount eder.

Ana ekranlar:

- **Datasets:** Son run ve severity bilgileri; sağlıklı run, fault run ve
  bootstrap tetikleme
- **Incidents:** Filtrelenebilir incident listesi ve kanıtlı RCA detayı
- **Actions:** Incident içindeki çözüm taslaklarını onaylama veya reddetme
- **Lineage:** Dataset veya kolon için upstream/downstream sorgusu

Fault run dashboard'dan başlatıldığında frontend ilgili `orders` run'ı için
RCA endpoint'ini de tetikler. Onay/ret sonrasında butonlar kilitlenir ve durum
rozeti güncellenir.

## 10. CLI kullanımı

Paket kurulduğunda `sentinel` komutu oluşur.

```bash
sentinel migrate
sentinel bootstrap --runs 14
sentinel run
sentinel run --fault F05
sentinel faults
sentinel report --dataset orders
sentinel lineage graph
sentinel lineage impact --dataset orders --column total_amount
sentinel lineage upstream --dataset payments --column amount
sentinel analyze --dataset orders
sentinel actions list --incident <incident_id>
sentinel actions approve <action_id> --actor ZEYDLCN
sentinel actions reject <action_id> --actor ZEYDLCN --note "Gerekçe"
```

CLI, Windows konsollarında Türkçe ve Unicode karakterlerde çökmemesi için
stdout/stderr encoding'ini UTF-8 olarak yapılandırır.

## 11. Kurulum ve çalıştırma

### 11.1 Python paketini kurma

```bash
pip install -e ".[dev,api]"
```

LLM rafinesi isteniyorsa:

```bash
pip install -e ".[dev,api,llm]"
```

### 11.2 PostgreSQL'i başlatma

Varsayılan host portu `5432`, varsayılan image `postgres:16` değeridir:

```bash
docker compose -f infra/docker-compose.yml up -d
```

Port doluysa Compose değişkenleriyle başka port seçilebilir.

PowerShell örneği:

```powershell
$env:POSTGRES_PORT = "5434"
$env:POSTGRES_IMAGE = "postgres:16-alpine"
docker compose -f infra/docker-compose.yml up -d
$env:DATABASE_URL = "postgresql+psycopg://sentinel:sentinel@localhost:5434/pipeline_sentinel"
```

Bash örneği:

```bash
POSTGRES_PORT=5434 POSTGRES_IMAGE=postgres:16-alpine \
  docker compose -f infra/docker-compose.yml up -d
export DATABASE_URL=postgresql+psycopg://sentinel:sentinel@localhost:5434/pipeline_sentinel
```

Uygulama `DATABASE_URL` değerini process environment'tan okur. `.env.example`
bir şablondur; CLI `.env` dosyasını kendiliğinden yüklemez. Uvicorn için
`--env-file .env` kullanılabilir.

### 11.3 Şema ve demo verisi

```bash
sentinel migrate
sentinel bootstrap --runs 14
sentinel run --fault F05
sentinel analyze --dataset orders
```

### 11.4 API ve dashboard

```bash
python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8000
```

- Dashboard: `http://127.0.0.1:8000/`
- Swagger UI: `http://127.0.0.1:8000/docs`
- API base URL: `http://127.0.0.1:8000/api/v1`

## 12. Örnek F05 olay akışı

F05, `orders.total_amount` değerlerini 100 ile çarpar:

```mermaid
flowchart TD
    A[Normal total_amount yaklaşık 482 TRY] --> B[F05: değer x100]
    B --> C[Range kuralı ihlali]
    B --> D[Baseline ortalamasından büyük z-score sapması]
    C --> E[range sinyali]
    D --> F[distribution sinyali]
    E --> G[RCA scale_error kuralı]
    F --> G
    G --> H[Olası TL → kuruş ölçek hatası]
    H --> I[Lineage: payments.amount ve bağlı varlıklar]
    H --> J[SQL ölçek düzeltme taslağı]
    H --> K[dbt range testi önerisi]
    J --> L[İnsan onayı bekler]
    K --> L
```

Buradaki kritik nokta, RCA'nın yalnızca “değer yükseldi” dememesidir. Range ve
distribution sinyallerini birlikte kullanır, gerçek sinyal kimliklerini kanıt
olarak raporlar ve downstream varlıkları lineage grafından çıkarır.

## 13. Test ve değerlendirme

Pytest ayarlarında varsayılan komut canlı DB gerektiren testleri dışarıda
bırakır:

```bash
pytest -q
```

PostgreSQL integration testleri:

```bash
pytest -m integration -q
```

Golden fault değerlendirmesi:

```bash
python -m evals.graders.grader
```

Eval seti şu metrikleri hesaplar:

- Detection recall
- Root cause Top-1 accuracy
- Doğru Top-1 hipotezlerde ortalama confidence
- Evidence precision

Mevcut doğrulamada 71 unit test, 31 integration test ve F01-F06 için 6/6
golden senaryo başarıyla geçmiştir.

GitHub Actions iki ana job çalıştırır:

1. DB'siz unit testler ve F01-F06 eval seti
2. `postgres:16` service container üzerinde migration ve integration testleri

## 14. Güvenlik ve güven sınırları

Mevcut güvenli davranışlar:

- Anomali kararı LLM'e bırakılmaz.
- LLM'e ham tablo yerine özet kanıt gönderilir.
- LLM evidence ID'leri doğrulanır.
- Action'lar otomatik uygulanmaz.
- Onay ve retler actor bilgisiyle audit edilir.
- Migration ve DB erişimi tek `DATABASE_URL` üzerinden yönetilir.

Üretim öncesi tamamlanması gereken kontroller:

- API authentication ve role-based authorization yoktur.
- CORS şu anda tüm origin'lere açıktır.
- Dataset/column erişim allowlist'i uygulanmamıştır.
- Rate limit ve tenant izolasyonu yoktur.
- Health endpoint'i DB sağlığını kontrol etmez.
- Secret manager entegrasyonu yoktur.
- Otomatik repair execution, sandbox ve rollback yoktur.

Bu nedenle mevcut uygulama yerel demo/MVP olarak değerlendirilmelidir; doğrudan
internet erişimine veya üretim verisine açılmamalıdır.

## 15. Mevcut sınırlar ve teknik borçlar

- Tek veri kaynağı sentetik PostgreSQL commerce pipeline'ıdır.
- Gerçek OpenLineage event ingestion yoktur; commerce lineage kodla sabittir.
- Baseline seasonal veya çoklu-run istatistiği değildir; en son sağlıklı run'dır.
- EWMA, PSI, KS, change-point ve Isolation Forest henüz uygulanmamıştır.
- `contracts` tablosu çalışma zamanındaki contract kaynağı değildir.
- Gerçek log arama, deploy değişikliği ve validation SQL agent araçları yoktur.
- RAG/vector search yoktur.
- `/sources` ile dinamik connector tanımlama yoktur.
- API işlemleri senkrondur; queue/worker ve polling modeli yoktur.
- Bir commerce run'ındaki üç dataset tek transaction değildir.
- Pipeline ortasında exception oluştuğunda kapsamlı retry/compensation ve tüm
  run'ları `failed` yapma mekanizması sınırlıdır.
- Lineage grafı büyürse tüm kenarları belleğe alma yaklaşımı ölçeklenmez.
- Approval gerçek patch execution anlamına gelmez.
- OpenTelemetry entegrasyonu yerine `agent_runs` tablosunda hafif audit vardır.

## 16. Planlanan doğal gelişim yönleri

Mevcut mimariyi bozmadan aşağıdaki sırayla geliştirilebilir:

1. API authentication, RBAC ve dataset allowlist
2. Gerçek connector ve `/sources` yönetimi
3. dbt manifest veya OpenLineage event ingestion
4. Çoklu-run/seasonal baseline ve gelişmiş drift yöntemleri
5. Queue tabanlı async profiling ve RCA worker'ları
6. Log/deploy kanıt araçları ve kontrollü RAG
7. OpenTelemetry trace, metric ve maliyet gözlemlenebilirliği
8. Test veritabanında dry-run repair doğrulaması
9. Rollback planlı ve açık onaylı düşük riskli self-healing

## 17. Kısa özet

Pipeline Sentinel AI yalnızca bir dashboard veya LLM sohbet arayüzü değildir.
Projenin esas değeri şu katmanları tek akışta birleştirmesidir:

```text
Data contract
  + istatistiksel profil
  + anomaly signal
  + lineage impact
  + kanıtlı root cause analysis
  + kontrollü insan onayı
```

Bugünkü sürüm, bu yaklaşımın güvenli ve test edilebilir MVP çekirdeğidir. Veri
kalitesi kararları deterministiktir, açıklamalar evidence kimlikleriyle
izlenebilir ve hiçbir öneri otomatik olarak üretim verisini değiştirmez.
