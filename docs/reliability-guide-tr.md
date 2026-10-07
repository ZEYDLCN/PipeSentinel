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

## Sürekli izleme: zamanlama, referans ve taslak sözleşme

**Zamanlama.** Kaynak yapılandırmasına `"schedule_minutes": 60` (5–10080 dakika)
ekleyin veya dashboard'daki "Otomatik analiz aralığı" alanını doldurun.
`reliability worker` 30 saniyede bir vadesi gelen kaynakları kuyruğa alır
(`--no-schedule` ile kapanır). Anahtar zaman dilimine bağlıdır; birden fazla worker
aynı dilim için tek iş üretir. Kaynak için bitmemiş bir iş varsa yenisi eklenmez,
yavaş kaynaklarda işler birikmez. Zamanlamayı değiştirmek geçmiş referansı sıfırlamaz.
Worker durursa analizler de durur; worker'ı bir servis olarak çalıştırın.

**Taslak sözleşme.** Elle YAML yazmak yerine bir CSV örneğinden başlayın:

```powershell
.\.venv\Scripts\sentinel.exe reliability suggest-contract ornek.csv --output contract.yml
```

Dashboard'da kaynak formunda CSV'yi seçip "Taslak öner" düğmesine basmak aynı şeyi
yapar. Çıktı tür, `not_null` (anahtar kolonlarda sıfır boşluk), anahtar benzeri
kolonlar için `uniqueness`, sayısal kolonlar için payı olan `range` ve az değerli
metin kolonları için `allowed_values` önerir. Zaman kolonları için SLA tahmin
edilmez, başlıktaki `# NOT:` satırlarında hatırlatılır. Eşikler örneğe göre
seçilir; kaydetmeden önce gözden geçirin. Örnek 100000 satırla sınırlıdır.

**Yeni sözleşme kuralları.**

```yaml
rules:
  - {type: row_count, min_rows: 1000}                              # kolon almaz
  - {type: column_compare, column: net, operator: "<=", other: gross}  # sayısal/datetime
```

`column_compare` boş değerleri atlar. Tipler uyuşmuyorsa ilişki yerine şema ihlali
raporlanır.

**Otomatik referans (isteğe bağlı).** `"auto_baseline": true` ile sinyalsiz bir analiz
otomatik olarak referans olur; yalnızca daha önce bir insan referansı kabul
etmişse (`baseline.state == ready`) çalışır ve denetim kaydına `auto-baseline`
aktörüyle yazılır. Büyüyen tablolarda eski hacimle kıyaslamayı önler, ama yavaş
kaymayı normalleştirebilir; hassas kaynaklarda kapalı tutun.

**Beklenen değişim.** Kampanya, yeni pazar veya fiyat değişimi gibi gerçek bir iş
değişimi sonrası alarm sürerse analiz detayından "Beklenen değişim olarak kabul et"
düğmesini kullanın (en az 10 karakterlik gerekçe zorunlu):

```powershell
.\.venv\Scripts\sentinel.exe reliability expected-change <observation-id> --note "Kasım kampanyası fiyatları yükseltti" --actor ayse
```

Yalnızca geçmişle karşılaştırmadan doğan sinyaller (dağılım, hacim, segment,
kardinalite) için ve kaynağın **en son** analizinde mümkündür. Sözleşme ihlali
içeren analiz referans olamaz; sözleşmeyi güncelleyin. Karar, gerekçesiyle denetim
kaydına ve geri bildirime yazılır.

**Tespit.** Dağılım kontrolü artık ortalama farkını standart hataya (Welch) göre de
sınar: |z| ≥ 6 ve baseline'a göre ≥ %10 kayma gerekir. Böylece büyük örneklemde
satır bazlı stddev'in gizlediği orta büyüklükteki kaymalar yakalanır, çok büyük
örneklemde önemsiz kaymalar alarm vermez. Örnek 30'dan küçükse devreye girmez.

**Değişiklik adayları.** Sinyalli bir analizde, son bilinen iyi referanstan beri
varlığı veya yukarı akışını etkileyen dbt tanım değişiklikleri ve başarısız
çalıştırmalar listelenir. İlk içe aktarma değişiklik sayılmaz. Bu liste zamansal
örtüşmedir, nedensellik kanıtı değildir; RCA raporunu ve `replay` sonucunu etkilemez.

**Olay yönetimi ve susturma.** Sinyalli bir analizde "Olay durumu" ile durumu (açık,
üstlenildi, çözüldü), sorumluyu ve notu kaydedin; liste "Durum" sütununda gösterir.
Aynı alarm tekrar ediyorsa "Alarmı sustur" ile belirli bir sinyali (tür + kolon) 24,
72 veya 168 saat susturun (gerekçe en az 5 karakter). Susturma yalnızca bildirimi
bastırır: analiz, sinyaller ve kanıtlar kaydedilir, analiz "susturulmuş alarm"
olarak işaretlenir. Analizdeki **her** sinyal susturulmuş değilse bildirim yine gider.
API: `POST /observations/{id}/triage`, `GET/POST /mutes`, `DELETE /mutes/{id}`.

**Metrik geçmişi.** Analiz detayında satır sayısı ile sayısal kolonların ortalama ve boş
oranı, aynı yapılandırmadaki son 60 analiz üzerinden çizilir (`GET /sources/{id}/history`).
Kırmızı nokta sinyalli analizi, halka açık olan analizi gösterir.

**Öğrenilmiş tazelik (SLA gerektirmez).** Kaynak yapılandırmasına
`"learned_freshness": ["created_at"]` ekleyin (sözleşmedeki `datetime` kolonları, en
fazla 10). En az 5 kabul edilmiş analizden "analiz anı − en yeni değer" gecikmesinin
medyanı ve MAD'ı öğrenilir; gecikme `max(medyan + 6σ, 2 × medyan, medyan + 15 dk)`
eşiğini aşarsa `staleness` sinyali üretilir. Geçmiş yoksa sinyal üretilmez. Kesin bir
SLA'nız varsa `freshness` kuralını kullanın.

**PR veri özeti.** `ci-check` komutuna `--markdown reports/data-quality.md` verirseniz
kolon bazında değişim, sayısal toplam farkları, ihlaller ve en fazla 5 örnek satır
okunabilir bir özet olarak yazılır; `data-quality.yml` bu özeti GitHub step summary'ye
ekler. Örnek satırlar CSV'lerinizdeki gerçek değerleri içerir; hassas veri taşıyan
dosyalar için raporu paylaşırken dikkatli olun.

## İzleme sağlığı, gelişmiş tespit ve kolon lineage

**İzleme sağlığı.** Worker her ~10 saniyede kalp atışı yazar. `GET /api/v1/reliability/monitoring`
ve `sentinel reliability status` şunu raporlar: worker canlı mı (`SENTINEL_WORKER_TTL_SECONDS`,
varsayılan 90 sn), kuyrukta bekleyen/çalışan iş, her kaynak için son başarılı analiz.
`schedule_minutes` tanımlı bir kaynak `2 × aralık + 10 dakika` içinde başarılı analiz üretmezse
"gecikmiş" sayılır (kaynak veritabanına ulaşılamadığı için analizler başarısız oluyorsa da). Durum:
`ok`, `degraded` (gecikmiş kaynak veya 15 dakikadan eski bekleyen iş), `down` (iş var ama canlı
worker yok). `status` komutu çıkış kodu verir (0/1/2; `--fail-on down` ile yalnızca `down`
başarısız sayılır), yani cron veya uptime kontrolüne bağlanabilir. Worker çökerse webhook da
gönderilemez; bu yüzden dışarıdan izleyin. Dashboard'da durum `ok` değilse üstte bir banner çıkar.

**Geçmişten öğrenen eşikler.** En az 8 kabul edilmiş analiz biriktiğinde hacim, kolon ortalaması
ve boş oranı için olağan aralık (medyan ve MAD) öğrenilir; |z| ≥ 6 ve pratik anlamlılık (ortalama
ve hacimde ≥ %5, boş oranında ≥ 2 puan) gerekir. Kararlı bir tabloda sabit %30 hacim eşiğinin
kaçırdığı ~%12'lik düşüşü yakalar. Kapatmak için `"adaptive_detection": false`. Mevsimsel
kaynaklarda yalnızca aynı hafta günü ve saatindeki analizler (en az 5) kullanılır.

**Çok değişkenli tespit (isteğe bağlı).** `"multivariate": true` (ve `retain_snapshot`) ile her
analiz, en yeni kabul edilmiş analizin veri kopyasına göre kolonların *birlikte* dağılımını
karşılaştırır. Mahalanobis mesafesi doğrusal ilişkilerin bozulmasını (ör. ücret ≈ 0,1 × tutar
artık geçerli değil; kolonların tek tek ortalaması aynı kalsa bile), Isolation Forest küresel
aykırılıkları (ör. yeni bir küme) yakalar. Isolation Forest için `pip install -e ".[ml]"`; yoksa
yalnızca Mahalanobis çalışır. Satırların ≥ %3'ü referansta alışılmadık ve p < 1e-6 ise
`multivariate_drift` sinyali üretilir. En az iki sayısal, kimlik olmayan kolon ve 200 satır gerekir.

**DuckDB connector.** `"type": "duckdb"`, `connection_env` bir `.duckdb` dosyasının yolunu içeren
ortam değişkeni (`SENTINEL_SOURCE_...`), `schema` genellikle `main`. Dosya `read_only=True` açılır,
tablo allowlist'te olmalıdır, sorgu `timeout_seconds` sonunda kesilir. `pip install -e ".[duckdb]"`.
Parquet/CSV dosyalarını izlemek için DuckDB içinde bir görünüm (view) tanımlayın. Dashboard'da
"Kaynak türü" alanından seçilir.

**Yeni connector eklemek.** `scan(config) -> ScanResult` sağlayan bir sınıf yazın,
`connectors/postgres.py::CONNECTOR_TYPES` listesine türü ekleyin ve `connectors/__init__.py::get_connector`'a
bağlayın. Salt okunur erişimi sunucu tarafında zorlayın (rol, oturum ayarı); Snowflake/BigQuery bu
repoda yoktur, çünkü hesapsız doğrulanamaz.

**Kolon lineage.** `ingest-dbt` komutu (veya dashboard'da dbt manifest yükleme), manifest'te
`compiled_code` varsa (`dbt compile/run` sonrası) her modelin çıktı kolonlarını yukarı akış
kolonlarına bağlar (join, alias, CTE ve belgelenmiş kolonlarla `select *` desteklenir). OpenLineage
olaylarında standart `columnLineage` facet'i okunur. `pip install -e ".[lineage]"` gerekir. Sinyalli
bir kolon için analiz detayında "Kolon etkisi" yukarı ve aşağı akış kolonlarını gösterir; bağımlılık
uydurulmaz, çözülemeyenler `column_stats`'ta sayılır. Dashboard'daki Bağımlılıklar sekmesi kolon
bağlantılarını listeler.

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

`SENTINEL_WEBHOOK_FORMAT=slack` Slack/Mattermost uyumlu `{"text": …}` gövdesi
gönderir (varsayılan `json`). Bu biçimde de imza başlığı eklenir; Slack onu
doğrulamaz, kendi webhook adresinizin gizliliğine güvenir.

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

7 Ekim 2026 yerel doğrulaması (sürekli izleme turları): 189 birim testi ve yalıtılmış
bir PostgreSQL 15 kümesinde 35 entegrasyon testi geçti; F01–F06 eval senaryolarının
tamamı geçti. Dashboard, gerçek tarayıcıda (Edge ve Chrome) Playwright ile uçtan uca
test edildi: CSV'den taslak sözleşme, zamanlayıcının kendiliğinden analizi, baseline
kabulü, dbt manifest'ten kolon bağlantıları, veri kayması ve "Kolon etkisi", olay durumu,
susturma, metrik grafikleri, beklenen değişim, DuckDB kaynağı ekleme, worker durunca "İzleme
durdu" banner'ı, pilot modunda rol denetimi ve dar ekran. Ayrıntılı gerekçe ve rakip karşılaştırması:
[`product-review-tr.md`](product-review-tr.md).

Tarayıcı testini kendiniz çalıştırmak için boş, ayrı bir PostgreSQL veritabanı gerekir
(test her koşuda `sentinel_*` tablolarını siler):

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[e2e]"
.\.venv\Scripts\python.exe tests/e2e/dashboard_flow.py --db-url postgresql+psycopg://kullanici@127.0.0.1:5432/sentinel_e2e --out reports/e2e --browser msedge
```

`--browser` değeri `msedge`, `chrome` veya `chromium` olabilir; ekran görüntüleri `--out`
klasörüne yazılır.

`sentinel migrate`, veri kopyalarını `sentinel_snapshots` tablosunda tutar. Önceki
sürümde gövdede saklanan kopyalar okunmaya devam eder.

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
