# Gerçek kaynaklar ve doğrulanmış onarım — kullanım kılavuzu

Bu sürüm PostgreSQL kaynağından analiz, sürümlü sözleşme, kalite onaylı baseline,
dbt/OpenLineage bağımlılıkları, olay gruplama, iş etkisi, çözüm hafızası,
izole onarım doğrulaması, webhook kutusu ve CI veri karşılaştırmasını ekler.

Yeni dashboard: **http://localhost:8000/** (`/reliability.html` de desteklenir).
Mevcut sentetik demo `/demo` adresinde çalışmaya devam eder. Gerçek kaynak
analizleri `/api/v1/reliability` altında ayrı bir yaşam döngüsüne sahiptir.

Arayüz gri dış zemin, krem/sarı yüzeyler, koyu işlem kartı ve yuvarlak üst
menü kullanır. Özet sayaçları son 50 analiz ve son 100 iş üzerinden hesaplanır;
aktivite grafiği bu analizlerin son yedi güne dağılımıdır. Veri sağlığı göstergesi
incelenen analizlerde sinyal bulunmayanların oranıdır. Boş kaynakta örnek sayı
gösterilmez. Aynı renk teması veri laboratuvarında da kullanılır.

## Kurulum ve çalıştırma

PowerShell, proje kökünde:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev,api]"
$env:DATABASE_URL = "postgresql+psycopg://sentinel:sentinel@localhost:5432/pipeline_sentinel"
.\.venv\Scripts\sentinel.exe migrate
.\.venv\Scripts\python.exe -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8000
```

İkinci terminalde aynı `DATABASE_URL` ve kaynak bağlantılarıyla işleyiciyi başlatın:

```powershell
$env:SENTINEL_SOURCE_COMMERCE = "postgresql+psycopg://reader:password@localhost:5432/commerce"
.\.venv\Scripts\sentinel.exe reliability worker
```

`DATABASE_URL` metadata deposudur. `SENTINEL_SOURCE_*` yalnızca kaynak okuma
bağlantısıdır. Kaynak kullanıcısına seçilen tablolarda SELECT yetkisi verin.
Bağlantı URL'si API'ye veya metadata'ya yazılmaz; yalnızca ortam değişkeninin
adı kaydedilir. CLI yerel operatör aracıdır; API token politikasını kullanmaz.

`migrate`, mevcut SQL migration'larına ek olarak `sentinel_*` metadata tablolarını
idempotent oluşturur. Bu tablolar, eski demo `reset` işleminden bağımsızdır.
`metadata.create_all` bu ilk şemayı kurar; gelecekteki kolon değişiklikleri
için ayrıca sürümlü ALTER migration'ları gerekir.

## Pilot erişimi

```powershell
$env:SENTINEL_MODE = "pilot"
$operatorToken = .\.venv\Scripts\python.exe -c "import secrets; print(secrets.token_urlsafe(32))"
$readerToken = .\.venv\Scripts\python.exe -c "import secrets; print(secrets.token_urlsafe(32))"
$tokenMap = @{}
$tokenMap[$operatorToken] = @{actor="operator-1";role="operator"}
$tokenMap[$readerToken] = @{actor="reader-1";role="reader"}
$env:SENTINEL_API_TOKENS = ConvertTo-Json -InputObject $tokenMap -Compress
```

Bu ortam değişkenlerini API sürecini başlatmadan ayarlayın. Dashboard'daki
“Oturum aç” düğmesi token'ı yalnızca sayfa belleğinde tutar. API istekleri
`Authorization: Bearer <token>` kullanır. Okuyucu sadece GET erişimine sahiptir;
operatör yapılandırma, analiz, geri bildirim ve onay işlemlerini yapar.
Audit aktörü token kimliğinden gelir; istemcinin gönderdiği `actor` dikkate alınmaz.
Pilot modunda token yapılandırılmamışsa API kapalı kalır ve sentetik veri
üretme/hata enjeksiyonu uçları devre dışıdır. Health endpoint'i açıktır.

Varsayılan `development` modu kimliksiz yerel geliştirme içindir; API'yi
127.0.0.1 üzerinde başlatın. Uzak pilot yayını için HTTPS terminasyonu kullanın.

## İlk gerçek analiz

1. “Kaynaklar” ekranında ad, bağlantı değişkeni, şema ve tabloyu seçin.
2. YAML sözleşmesini tanımlayıp “Sözleşmeyi doğrula” düğmesine basın.
3. Sıralama kolonlarını, satır bütçesini ve istenirse partition aralığını seçin.
4. Kaydedin; “Analiz başlat” ile işi kuyruğa alın.
5. İşleyici çalışırken pending → running → succeeded/failed geçişlerini izleyin.
6. Temiz ilk analizi inceleyip “Baseline olarak kabul et” seçin.
7. Sonraki analizler yalnızca aynı yapılandırmada kabul edilmiş geçmişi kullanır.

Örnek dosyalar `examples/reliability/` altındadır. Örnekteki tablo şeması
`order_id, batch, total_amount` şeklindedir; mevcut sentetik commerce tablosuyla
birebir aynı değildir. Kendi tablonuza göre sözleşmeyi düzenleyin.

CLI karşılıkları:

```powershell
.\.venv\Scripts\sentinel.exe reliability validate-contract examples/reliability/contract.yml
.\.venv\Scripts\sentinel.exe reliability source-add orders examples/reliability/source.json
.\.venv\Scripts\sentinel.exe reliability analyze <source-id> <benzersiz-istek-anahtari>
.\.venv\Scripts\sentinel.exe reliability worker --once
.\.venv\Scripts\sentinel.exe reliability baseline-accept <observation-id>
```

POST `/sources/{id}/analyze` cevabı 202'dir ve `Idempotency-Key` başlığı
zorunludur. Aynı kaynak/anahtar aynı işi döndürür. Başarısız işler en fazla
üç denemeye kadar `/jobs/{id}/retry` ile tekrar alınabilir. Worker lease'i
sona eren işler sonraki worker taramasında başarısız olarak işaretlenir.

## Tarama ve baseline kapsamı

- Kaynak transaction'ı `REPEATABLE READ, READ ONLY` kullanır. Tablo isimleri
  quote edilir, partition değerleri bind parametreleridir. Serbest SQL kabul edilmez.
- Tam tablo/partition hacmi SQL `count(*)` ile hesaplanır. Kalite kontrolleri,
  en fazla seçilen satır bütçesindeki DataFrame üzerinde yapılır.
- İki kaynak sorgusunun her birinde 1–120 saniyelik statement timeout vardır.
  Satır bütçesi 1–100000 aralığındadır. Bu, satır bayt boyutunu sınırlamaz.
- Sıralanmış veya sıralanmamış ilk N kayıt örneği istatistiksel rastgele örnek
  değildir. Sonuçta tam/kısmi kapsam, yöntem, süre ve satır sayısı görünür.
- Boş kaynak veya sinyal içeren analiz baseline olamaz. Teknik başarı tek
  başına yeterli değildir. Temiz aday açık operatör kabulü bekler.
- Mevsimsel karşılaştırma UTC hafta günü/saat kohortunda en az üç kabul edilmiş
  analiz gerektirir; son 12 uygun profilin sayısal medyanını kullanır. Yetersiz
  geçmişte otomatik olarak farklı mevsimin referansına geçmez.
- Segmentler en fazla iki kolon ve 100 gruptur. Segment karşılaştırması seçilen
  kohortun en son kabul edilmiş segment profilini kullanır. Kaybolan segmentler
  ayrıca işaretlenir. Kısmi taramada segment sonucu da örnek kapsamındadır.
- `sensitive_columns` için kategorik örnekler/hash ve sinyal örnek değerleri
  metadata'dan çıkarılır. Hassas kolonlar segment anahtarı olamaz. Bu kaynaklarda
  snapshot saklama kapalı olmalıdır; kaynak okuması yine inceleme için gerçekleşir.

## dbt, OpenLineage ve iş etkisi

```powershell
.\.venv\Scripts\sentinel.exe reliability ingest-dbt commerce target/manifest.json --run-results target/run_results.json
.\.venv\Scripts\sentinel.exe reliability ingest-openlineage airflow event.json
```

Dashboard dosya yüklemeyi de destekler. dbt modeli, source ve exposure
bağımlılıkları içeri alınır; manifest güncellendiğinde kaldırılan kenarlar silinir.
Kod/config/schema/dependency hash değişiklikleri ve run sonuçları zaman çizelgesinde
görünür. Modelin `unique_id` değerini kaynakta `lineage_asset` olarak seçin.

OpenLineage RunEvent için namespace/name, runId, eventTime, eventType ve
inputs/outputs işlenir; düğümler `namespace::name`, işler `job::namespace::name`
biçimindedir. RunEvent'ler birikimli job bağlantıları sağlar; historical
kenarlar korunur. SQL'den veya job giriş/çıkışlarından kolon türetimi uydurulmaz.

Aynı/bağlantılı varlıkların bir saat içindeki sinyalli analizleri gruplandırılır.
Bu korelasyon, kanıtlanmış ortak neden iddiası değildir. “İş etkisi” ekranında
varlık sahibi, 1–5 kritiklik ve SLA girilir. Öncelik puanı severity, kritiklik
ve gözlenen freshness/SLA bileşenlerini açıklar. Parasal kayıp hesaplanmaz.

## Onarım ve olay hafızası

Snapshot saklama kaynak başına isteğe bağlıdır ve varsayılan kapalıdır.
Açıldığında taranan ham verinin bir kopyası metadata veritabanına yazılır;
veritabanı erişim ve saklama politikası buna göre belirlenmelidir. API ham
snapshot'ı dışarı vermez. Ayrı disk/VM izolasyonu yoktur: sandbox ayrı, süre
sınırlı Python sürecinde yalnızca izinli DataFrame şablonlarını çalıştırır.
Keyfi Python, SQL veya shell çalıştırılmaz; üretim bağlantısı kullanılmaz.

Desteklenen ilk onarım şablonları:

- `scale`: zorunlu batch/eşitlik filtresinde sayısal kolonu 0.01, 0.1, 10 veya
  100 ile çarpar. Değişen satır sınırı vardır; seçilmeyen kayıtlar kontrol grubudur.
- `deduplicate`: tam aynı kayıtları kaldırır. Aynı anahtarda farklı içerik
  varsa hangi kaydın doğru olduğu tahmin edilmez ve işlem reddedilir.

Önceki ihlaller azalmakla kalmamalı, sözleşme tamamen geçmeli ve kontrol grubu
korunmalıdır. Önce/sonra profil, satır sayısı, ihlaller ve içerik hash'leri
kaydedilir. Sonuç yalnızca snapshot sözleşmesini doğrular; işletme doğruluğu veya
downstream sonuçlar için garanti değildir. Yanlış onarımın sözleşmeyi de geçtiği
durumlar ek iş kuralları ve insan incelemesi gerektirir.

Onay güncel analiz/yapılandırmaya bağlıdır, yarışan kararlar reddedilir.
**Onay üretimde SQL çalıştırmaz.** Üretim execution, rollback/backfill otomasyonu
ve downstream pipeline'ı sandbox'ta çalıştırma bu teslimatın kapsamı dışındadır;
özellik planında da sonraki ayrı aşama olarak belirtilmiştir.

“Yeniden oynat”, kaydedilmiş sinyal kanıtları ve o zamanki downstream listesiyle
RCA'yı tekrar üretir. Kural kaynak dosyalarının hash'i değiştiyse eski sürüm
gerektirir. Bu işlem kaynağı yeniden okumaz, tam ETL replay'i değildir.
“Çözüldü” geri bildirimi olan benzer olaylar kaynak/sözleşme/sinyal eşleşmesiyle
listelenir. Sonradan başarısız olarak işaretlenen çözümler önerilmez.

## Bildirimler

Her sinyalli analiz için kalıcı outbox kaydı oluşturulur. Dashboard'da tüm
durumlar görünür. İsteğe bağlı webhook için worker ortamında
`SENTINEL_WEBHOOK_URL` (HTTPS) ve `SENTINEL_WEBHOOK_SECRET` (en az 24 karakter)
tanımlayıp `reliability worker --notifications` kullanın.

Payload ham satır veya örnek değer içermez. `X-Sentinel-Signature`, payload'ın
HMAC-SHA256 imzasıdır. `X-Sentinel-Event` sabit olay kimliğidir; alıcı bu kimlikle
tekrar teslimleri ayıklamalıdır. Teslim 10 saniye timeout, en fazla üç deneme ve
kesilmiş worker için lease kurtarma kullanır. Redirect takip edilmez. Varsayılan
worker dışarı bildirim göndermez. Bu geliştirme sırasında gerçek bildirim gönderilmedi.

## PR / CI kalite kapısı

```powershell
.\.venv\Scripts\sentinel.exe reliability ci-check baseline.csv candidate.csv `
  --contract examples/reliability/contract.yml --keys order_id `
  --max-change-ratio 0 --output reports/data-quality.json
```

Tekil/dolu anahtarlarla eklenen, silinen ve değişen kayıtlar, şema ve sayısal
toplamlar karşılaştırılır. Candidate sözleşmeyi geçmezse veya değişim bütçesi
aşılırsa komut exit 1 döndürür. Her CSV en fazla 20 MB / 100000 satırdır.
Bu sürüm karşılaştırılacak snapshot'ları kendisi dbt ile üretmez.

`.github/workflows/data-quality.yml` yeniden kullanılabilir workflow'dur.
Çağıran PR workflow'u base/candidate Git ref'lerini, snapshot_path,
contract_path, keys ve max_change_ratio gönderir. Dosyalar her iki ref'te
bulunmalıdır. Kapı, PR'ın kuralı gevşeterek geçmesini önlemek için **base ref'teki
sözleşmeyi ve aracı** kullanır. Bu özellik önce base branch'e alınmış olmalıdır.
Çıktı GitHub artifact'ı olur; workflow PR yorumu göndermez.

## Doğrulama ve ölçüm

```powershell
.\.venv\Scripts\python.exe -m pytest -q
# Yalnızca ayrı, boş test veritabanıyla: eski entegrasyon testleri demo şemasını sıfırlar.
$env:DATABASE_URL = "postgresql+psycopg://testuser:password@localhost:5432/sentinel_test"
.\.venv\Scripts\python.exe -m pytest -m integration -q
.\.venv\Scripts\python.exe -m evals.graders.grader
```

`GET /api/v1/reliability/metrics`, son 1000 analiz için tarama süresi, sinyalli
analiz, kullanıcı değerlendirmesi, çözüm ve onarım doğrulama sayılarını verir.
Etiketli veri olmadan accuracy/recall hesaplanmaz. F01–F06 eval başarısı yalnızca
bu altı sentetik senaryo için geçerlidir. Yeni testler gerçek kaynak, kalite
referansı, mevsimsellik, segment kaybı, audit/roller, timeout/tekrar deneme,
yanlış onarım ve güncelliğini yitirmiş onay durumlarını da kapsar.

## Protokol kaynakları

29 Eylül 2026 yerel doğrulaması: 98 birim testi, ayrı PostgreSQL 15 kümesinde
34 entegrasyon testi ve F01–F06 eval senaryolarının tamamı geçti. Headless Edge
ile kaynak oluşturma → sözleşme doğrulama → analiz → baseline kabulü → hata
tespiti → sandbox → onay akışı geçti; JavaScript hatası görülmedi. CI ayrıca
PostgreSQL 16 üzerinde entegrasyon testlerini çalıştıracak şekilde yapılandırılmıştır.

- [PostgreSQL READ ONLY transaction](https://www.postgresql.org/docs/16/sql-set-transaction.html)
- [dbt manifest](https://docs.getdbt.com/reference/artifacts/manifest-json)
- [dbt run_results](https://docs.getdbt.com/reference/artifacts/run-results-json)
- [OpenLineage run cycle](https://openlineage.io/docs/spec/run-cycle/)
