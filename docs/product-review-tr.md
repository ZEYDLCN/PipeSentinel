# PipeSentinel — Ürün incelemesi, rakip karşılaştırması ve kararlar

Tarih: 7 Ekim 2026. İncelenen taban: `76c8cf2` (üzerine bu turdaki değişiklikler eklendi).
Bu doküman bir product owner gözüyle yazıldı: önce ürünün bugünkü durumu, sonra
rakipler, ardından "daha iyi nasıl olur?" sorusuna verilen kararlar ve bu turda
uygulananlar.

## 1. Özet

- Ürünün çekirdeği sağlam: LLM'siz deterministik tespit, kanıt zorunluluğu,
  salt okunur kaynak erişimi, izole onarım doğrulaması, denetim kaydı.
- En büyük boşluk **"sürekli izleme"** idi: analiz yalnızca elle veya API'den
  tetikleniyordu, ilk sözleşmeyi elle yazmak gerekiyordu ve meşru bir iş değişimi
  sonrası alarm sonsuza kadar tekrarlanıyordu. Üçü de bu turda kapatıldı.
- Tespit kalitesinde sessiz bir zayıflık vardı: orta büyüklükte kaymalar
  (ör. %20) büyük örneklemde hiç yakalanmıyordu. Standart hata tabanlı bir test eklendi.
- Sonraki turlarda kolon seviyesi lineage, geçmişten öğrenen/çok değişkenli anomali, DuckDB
  connector'ı ve izleme sağlığı eklendi. Rakiplerin hâlâ gerisinde olduğumuz yerler: Snowflake/BigQuery
  gibi büyük depo connector'ları (hesapsız doğrulanamadı), ekip özellikleri ve barındırılan hizmet.

## 2. Uçtan uca mevcut durum

| Katman | Durum |
|---|---|
| Kaynak erişimi | Salt okunur PostgreSQL connector (`REPEATABLE READ, READ ONLY`, allowlist, satır/süre bütçesi) |
| Sözleşme | Sürümlü, katı doğrulamalı YAML; kural türleri: şema, not_null, range, uniqueness, freshness, allowed_values, referential_integrity (**+ bu turda** row_count, column_compare) |
| Tespit | Sözleşme ihlalleri + baseline karşılaştırması (z-skoru, hacim, kardinalite, segment, mevsimsel kohort) |
| Baseline | Yalnızca insan onaylı temiz analizler; yapılandırma değişince yeni baseline |
| Lineage / etki | dbt manifest + OpenLineage RunEvent, varlık sahibi/kritiklik/SLA ile iş önceliği |
| RCA | Kural tabanlı, kanıt kimlikli hipotezler; opsiyonel LLM rafinesi (eski demo yolunda) |
| Onarım | `scale` / `deduplicate` şablonları, izole süreçte, önce/sonra + kontrol grubu; onay üretimde SQL çalıştırmaz |
| Operasyon | Kalıcı iş kuyruğu (lease, idempotency, tekrar deneme), rol tabanlı token, HMAC imzalı webhook |
| CI | Base ref'teki sözleşmeyi kullanan PR veri farkı kapısı |

Doğrulama (bu oturumda çalıştırıldı): **189 birim testi** (başlangıçta 98) ve
**35 entegrasyon testi** (başlangıçta 34) geçti; entegrasyon testleri yalıtılmış,
geçici bir PostgreSQL 15 kümesinde, gerçek connector ile koştu. Dashboard için
**gerçek tarayıcıda uçtan uca test** (`tests/e2e/dashboard_flow.py`, Playwright;
Edge ve Chrome'da geçti). F01–F06 eval seti %100 (yalnızca bu altı sentetik
senaryo için geçerli).

## 3. Bulunan eksikler ve kararlar

| # | Eksik (kanıt) | Karar | Durum |
|---|---|---|---|
| 1 | Zamanlayıcı yok; worker yalnızca kuyruğu boşaltıyor (`reliability_cli.py`) | Kaynak başına `schedule_minutes`, slot bazlı idempotent kuyruğa alma | **Yapıldı** |
| 2 | Sinyalli analiz hiçbir koşulda baseline olamıyordu (`accept_baseline`) | "Beklenen değişim olarak kabul et": yalnızca baseline kaynaklı sinyaller, gerekçe zorunlu, denetim kaydı | **Yapıldı** |
| 3 | Dağılım testi satır bazlı stddev'e bölüyordu; %20 kayma N=10 000'de kaçıyordu | Welch standart hata testi (istatistiksel ve pratik anlamlılık birlikte) | **Yapıldı** |
| 4 | İlk sözleşmeyi elle yazmak gerekiyordu | CSV örneğinden taslak sözleşme (CLI, API, dashboard) | **Yapıldı** |
| 5 | Baseline tek bir geçmiş analizdi; büyüyen tabloda hacim alarmı kalıcılaşır | Opt-in `auto_baseline` (yalnızca insan onaylı referans varken, sinyalsiz analizler) | **Yapıldı** |
| 6 | Dbt/OpenLineage değişiklikleri yalnızca zaman çizelgesinde duruyordu | Yukarı akış "değişiklik adayları" (korelasyon etiketiyle) | **Yapıldı** |
| 7 | Kural yelpazesi dar (satır sayısı, sütunlar arası ilişki yok) | `row_count`, `column_compare` | **Yapıldı** |
| 8 | Webhook yalnızca ham JSON | `SENTINEL_WEBHOOK_FORMAT=slack` | **Yapıldı** |
| 9 | Her analiz, 12 MB'a varan veri kopyası taşıyan 500 geçmiş gövdesini okuyordu | Veri kopyası ayrı tabloya (`sentinel_snapshots`); eski kayıtlar için geri uyumlu | **Yapıldı** |
| 10 | Kaynak çeşidi: yalnızca PostgreSQL | DuckDB connector'ı (salt okunur dosya) eklendi; connector kaydı (`get_connector`) ile yenisi eklemek kolay. **Snowflake/BigQuery yapılmadı:** hesap ve kimlik bilgisi olmadan salt okunur erişimi ve davranışı doğrulayamam; doğrulanmamış bir connector göndermek istemedim | **Kısmen (3. tur)** |
| 11 | Kolon seviyesi lineage yok (Datafold, Metaplane, Elementary Cloud'da var) | dbt derlenmiş SQL'inden (sqlglot) ve OpenLineage `columnLineage` facet'inden kolon bağlantıları; sinyalli kolonun yukarı/aşağı akış kolonları | **Yapıldı (3. tur, en iyi çaba)** |
| 17 | Worker durursa analizler sessizce durur | Worker kalp atışı, `GET /monitoring`, `sentinel reliability status` (çıkış kodlu), dashboard banner'ı | **Yapıldı (3. tur)** |
| 18 | Sabit eşikler (%30 hacim) kararlı metriklerdeki küçük sapmaları kaçırır; ML tabanlı anomali yok | Geçmişten öğrenen eşikler (medyan + MAD); çok değişkenli ilişki dedektörü (Mahalanobis) ve isteğe bağlı Isolation Forest | **Yapıldı (3. tur)** |
| 12 | Metrik geçmişi grafiği yok (Monte Carlo, Metaplane, GX Cloud'da var) | Analiz detayında satır sayısı, ortalama ve boş oranı grafikleri | **Yapıldı (2. tur)** |
| 13 | Olay yaşam döngüsü yok: üstlenme, sorumlu, çözüldü (Monte Carlo, Elementary Cloud) | `open → acknowledged → resolved`, sorumlu ve not, denetim kaydı | **Yapıldı (2. tur)** |
| 14 | Alarm yorgunluğu: tekrarlayan alarm susturulamıyor (rakiplerin snooze'u) | Süreli susturma: bildirim gitmez, analiz ve kanıt kaydedilir | **Yapıldı (2. tur)** |
| 15 | Sıfır-konfigürasyon monitör yok (Monte Carlo, Metaplane, Bigeye) | Öğrenilmiş tazelik: SLA girilmeden olağan gecikmeden sapma | **Yapıldı (2. tur)** |
| 16 | PR özeti yok (Datafold PR yorumu) | Kolon bazında değişim, örnek satırlar, Markdown özeti (GitHub step summary) | **Yapıldı (2. tur)** |

## 4. Rakipler

Aşağıdaki bilgiler 7 Ekim 2026'da kamuya açık kaynaklardan derlendi; fiyat ve
özellikler değişebilir, karar vermeden önce tekrar kontrol edin.

| Ürün | Model | Öne çıkan | Bize göre |
|---|---|---|---|
| [Monte Carlo](https://www.montecarlodata.com/blog-monte-carlo-observability-agents) | Ticari SaaS | Nisan 2025'te "Monitoring Agent" (kural/eşik önerir) ve "Troubleshooting Agent" (kök neden araştırır) duyurdu; ajanlar salt okunur | Ürün olgunluğu ve kapsam çok üstün; kapalı, verinin dışarı çıkması gerekir |
| [Datafold](https://docs.datafold.com/data-diff) | Ticari SaaS | PR açıldığında veri farkı (satır, şema, anahtar, kolon), kolon seviyesi lineage, veritabanları arası diff | PR kapımız benzer fikir ama CSV snapshot'larıyla sınırlı; Datafold kolon lineage'ında ileride |
| [Soda](https://docs.soda.io/soda-v4/quickstart) | Açık çekirdek (Apache 2.0) + bulut | YAML veri sözleşmeleri, AI anomali tespiti; üçüncü taraf listelemeye göre bulut ücretsiz plan ve takım planı mevcut | Sözleşme modeli bize en yakın; onların kural yelpazesi ve connector çeşidi çok daha geniş |
| [Elementary](https://docs.elementary-data.com/cloud/cloud-vs-oss) | Açık çekirdek (dbt paketi + CLI) + bulut | dbt içinde anomali testleri, lineage, Slack uyarıları; bulutta ML, kolon lineage, olay yönetimi, AI ajanlar | dbt'ye bağlı; bizim PostgreSQL tablolarını dbt'siz izleyebilmemiz bir fark |
| [Great Expectations](https://greatexpectations.io/blog/introducing-gx-core-1-0) | Açık çekirdek + bulut | Çok sayıda Expectation; bulutta zamanlama, Slack/PagerDuty; ExpectAI veriden kural önerir (PostgreSQL, Databricks, Snowflake, Redshift) | `suggest-contract` ile aynı fikir; onlar bulut + LLM, biz çevrimdışı ve deterministik |
| [Metaplane](https://techcrunch.com/2025/04/23/datadog-acquires-ai-powered-observability-startup-metaplane) | Datadog tarafından satın alındı (Nisan 2025) | ML tabanlı izleme, kolon lineage | Pazar, büyük gözlemlenebilirlik platformlarına doğru birleşiyor |

**Savunulabilir fark:** küçük ekibin kendi ortamında çalıştırabildiği, LLM
zorunluluğu olmayan, kanıtı yeniden üretilebilir (`replay`) ve düzeltmenin
sonucunu veri kopyasında gösteren ürün. Rakiplerin ajanları öneri üretir; biz
önerinin etkisini sandbox'ta ölçüp insan onayına sunuyoruz.

**Dürüst eksik listesi:** büyük depo connector'ları (Snowflake, BigQuery, Databricks;
yalnızca PostgreSQL ve DuckDB var), ekip özellikleri (yorum, entegrasyonlar),
barındırılan hizmet güvencesi. Anomali tarafında rakiplerin eğitilmiş zaman serisi
modelleri yerine biz açıklanabilir robust istatistik, Mahalanobis ve isteğe bağlı
Isolation Forest kullanıyoruz; kolon lineage'ı en iyi çaba düzeyinde.

## 5. Product owner soruları ve kararlar

**S1. Pilot kullanıcı ilk hafta neden bırakır?**
(a) Hiçbir şey kendiliğinden çalışmıyor, (b) ilk sözleşmeyi yazmak zor,
(c) meşru bir değişimden sonra alarm susmuyor. → Kararlar #1, #4, #2.

**S2. Güveni en hızlı ne bozar?** Yanlış pozitif alarm yağmuru (tasarım dokümanı
§21). → Kararlar #3 (pratik anlamlılık eşiği ile) ve #2 (alarmı kapatma yolu). Her
yeni kural "kanıt kimliği olmadan hipotez yok" ilkesine bağlı kaldı.

**S3. Otomasyon ne kadar ileri gitmeli?** Referansı otomatik güncellemek
sürüklenmeyi (yavaş yavaş kayan veri) normalleştirebilir. → `auto_baseline`
varsayılan **kapalı**, yalnızca bir insan referansı onayladıktan sonra çalışır,
denetim kaydı `auto-baseline` aktörüyle tutulur. "Beklenen değişim" her zaman
insan kararıdır ve sözleşme ihlallerinde kullanılamaz (sözleşme güncellenmeli).

**S4. Nedensellik iddiası?** Zamansal örtüşme kanıt değildir. → Değişiklik
adayları ayrı alanda, "korelasyon, nedensellik değil" notuyla gösterilir; RCA
raporuna ve `replay` sonucuna karışmaz.

**S5. Yeni connector mı, derinlik mi?** Pilot geri bildirimi olmadan connector
eklemek tahmine dayalı iş. → Bu tur derinlik: sürekli izleme, onboarding, tespit kalitesi.

## 6. Bu turda eklenenler

| Özellik | Kullanım |
|---|---|
| Zamanlama | Kaynak yapılandırmasında `"schedule_minutes": 60` (5–10080); `sentinel reliability worker` 30 sn'de bir vadesi gelen kaynakları kuyruğa alır (`--no-schedule` ile kapanır). Bitmemiş iş varken yenisi eklenmez |
| Otomatik referans | `"auto_baseline": true`; insan onaylı referans + sinyalsiz analiz gerekir |
| Beklenen değişim | `sentinel reliability expected-change <id> --note "…"`, `POST /observations/{id}/expected-change`, dashboard'da analiz detayında düğme |
| Standart hata testi | Otomatik; her iki profilde de `row_count` varsa, \|z\|≥6 ve ≥%10 kayma gerekir. Eski satır bazlı test aynen duruyor |
| Taslak sözleşme | `sentinel reliability suggest-contract veri.csv --output contract.yml`, `POST /contracts/suggest`, kaynak formunda CSV seçip "Taslak öner" |
| Yeni kurallar | `{type: row_count, min_rows: 1000}`, `{type: column_compare, column: net, operator: "<=", other: gross}` |
| Değişiklik adayları | Analiz detayında "Olası değişiklik adayları" (dbt tanım değişimi, başarısız run) |
| Slack/Mattermost | `SENTINEL_WEBHOOK_FORMAT=slack` |
| Veri kopyası tablosu | `sentinel migrate` `sentinel_snapshots` tablosunu oluşturur |

### İkinci tur: rakip özellikleri

| Özellik | Rakipteki karşılığı | Kullanım |
|---|---|---|
| Olay yaşam döngüsü | Monte Carlo / Elementary Cloud olay yönetimi | Analiz detayında "Olay durumu": durum, sorumlu, not; `POST /observations/{id}/triage`; listede "Durum" sütunu |
| Süreli susturma | Alarm snooze/mute | "Alarmı sustur" (24/72/168 saat, gerekçe zorunlu); `POST/GET/DELETE /mutes`. Yalnızca bildirimi bastırır, analiz ve kanıtlar kaydedilir; her sinyal kapsanmıyorsa bildirim yine gider |
| Metrik geçmişi | Metaplane / Monte Carlo metrik grafikleri, GX Cloud geçmiş sonuçlar | Analiz detayında "Metrik geçmişi"; `GET /sources/{id}/history`; kırmızı nokta sinyalli analiz, halka bu analiz |
| Öğrenilmiş tazelik | Sıfır-konfigürasyon freshness monitörleri | `"learned_freshness": ["created_at"]`; en az 5 kabul edilmiş analizden olağan gecikme (medyan, MAD) öğrenilir; eşik `max(medyan+6σ, 2×medyan, medyan+15 dk)`. Sinyal türü `staleness` |
| PR veri özeti | Datafold PR veri farkı özeti | `sentinel reliability ci-check … --markdown reports/data-quality.md`; workflow özeti GitHub step summary'ye yazar. Tablo hücreleri kaçırılır (CSV içeriği işaretlemeyi bozamaz) |

### Üçüncü tur: kalan boşlukların denenmesi

| Özellik | Ne yapar | Kullanım |
|---|---|---|
| İzleme sağlığı | Worker her ~10 sn kalp atışı yazar; zamanlanmış bir kaynak `2 × aralık + 10 dk` içinde başarılı analiz üretmezse "gecikmiş", iş var ama canlı worker yoksa "down" | `GET /api/v1/reliability/monitoring`; `sentinel reliability status` (çıkış 0/1/2, `--fail-on`); dashboard'da banner. `SENTINEL_WORKER_TTL_SECONDS` ile eşik ayarlanır |
| Geçmişten öğrenen eşik | En az 8 kabul edilmiş analizden hacim, ortalama ve boş oranı için olağan aralık (medyan, MAD); \|z\| ≥ 6 ve pratik anlamlılık gerekir. Mevsimsel kaynakta aynı hafta günü/saat kohortu (en az 5) | Varsayılan açık; `"adaptive_detection": false` ile kapanır. Sinyal kanıtında `method: history` |
| Çok değişkenli tespit | Referans veri kopyasına göre kolonların *ortak* dağılımı: Mahalanobis (ilişki bozulması, bağımlılıksız) + Isolation Forest (küresel aykırılık, scikit-learn varsa) | `"multivariate": true` (`retain_snapshot` gerekir). Sinyal türü `multivariate_drift`, kanıtta iki dedektörün sonucu |
| DuckDB connector | `.duckdb` dosyasını `read_only=True` açar, allowlist, satır/süre bütçesi, zaman aşımında sorguyu keser | `"type": "duckdb"`, `connection_env` dosya yolunu içerir; `pip install -e ".[duckdb]"` |
| Kolon lineage | dbt `compiled_code`'dan kolon bağlantıları (join, alias, CTE, `select *` şemayla); OpenLineage `columnLineage` facet'i | `pip install -e ".[lineage]"`; manifest'i `ingest-dbt` ile alın; analiz detayında "Kolon etkisi" |

Dürüst bir bulgu: Isolation Forest tek başına, iki kolonun birlikte hareket etmesinin (ör. ücret ≈ 0,1 × tutar) bozulmasını **yakalayamadı**; bozulmuş satırları normalden bile az "alışılmadık" buldu (eksen paralel bölmeler ilişkiyi görmez). Bu yüzden ilişki bozulmaları için Mahalanobis eklendi ve Isolation Forest küresel aykırılıklar (ör. yeni bir küme) için tutuldu; iki yöntem birlikte çalışır.

Not: `RULE_VERSION` `reliability-2` oldu ve tespit kodu değiştiği için eski
analizlerin `replay`'i "kural sürümü değişmiş" uyarısı verir; bu tasarım gereğidir.
`schedule_minutes` ve `auto_baseline`, baseline karşılaştırılabilirlik özetine
(`config_hash`) dahil değildir; bunları değiştirmek geçmiş referansı sıfırlamaz.

## 7. Bilinen sınırlar ve riskler

- **Dashboard gerçek tarayıcıda denendi ama elle gezilmedi.** Otomatik uçtan uca
  test Edge ve Chrome'da geçti; yine de görsel cila, erişilebilirlik ve çok farklı
  ekran boyutları için bir kişinin göz atması gerekir. Test iki gerçek kusur
  yakaladı ve düzeltildi: oturum açtıktan sonra eski 401 hata mesajının ekranda
  kalması ve eksik favicon'dan gelen konsol hatası.
- **Kolon lineage en iyi çabadır.** Derlenmiş SQL gerekir (`dbt compile/run` sonrası manifest);
  dinamik SQL, bazı pencere/lateral yapılar ve manifest dışı tablolar çözülemeyebilir. Sayılar
  `column_stats`'ta (çözülemeyen kolon/tablo, atlanan kolon) görünür; bağımlılık uydurulmaz.
- **Geçmişten öğrenen eşik varsayılan açıktır** ve sabit %30 eşiğinden daha hassastır: kararlı bir
  tabloda ~%12 sapma alarm verebilir. Gürültülü bulursanız `"adaptive_detection": false`.
- **Mahalanobis doğrusal ilişkiyi, Isolation Forest küresel aykırılığı yakalar;** doğrusal olmayan,
  yerel ilişki bozulmaları kaçabilir. Referans, en yeni kabul edilmiş analizin veri kopyasıdır.
- **Worker çökerse webhook da gidemez.** Bu yüzden izleme durumu API ve `status` komutuyla dışarıdan
  (cron, uptime kontrolü) izlenmelidir; banner yalnızca dashboard'u açan birine görünür.
- **Snowflake/BigQuery yok.** Connector arayüzü hazır, ama hesapsız doğrulayamadığım bir
  salt okunur erişim kodunu göndermedim. Bir pilot hesabıyla eklenip doğrulanabilir.
- **Öğrenilmiş tazelik temkinli çalışır.** Tabloyu saatte bir tarayıp günde bir
  yüklenen bir tabloda ancak birkaç gün gecikme olunca alarm verir (eşik geçmişteki
  gecikme dağılımına bağlıdır). Kesin SLA için `freshness` kuralını kullanın.
- **Susturma süresi dolana kadar bildirim yok.** Susturulan alarm gerçek bir
  bozulmayı gizleyebilir; her susturma gerekçesiyle denetim kaydındadır, süresi
  en fazla 30 gündür ve analiz detayında "susturulmuş alarm" olarak görünür.
- **`auto_baseline` yavaş kaymayı gizleyebilir.** Her temiz analiz bir sonrakinin
  referansı olur. Hassas tablolarda mevsimsel modu veya elle onayı tercih edin.
- **Mevsimsel modda beklenen değişim yavaş yansır.** Referans, aynı kohortun son
  12 kabul edilmiş analizinin medyanıdır; eski analizler zamanla düşer.
- **Zamanlayıcı, worker'ın sürekli çalışmasını gerektirir.** Worker durursa
  kaynaklar sessizce analiz edilmez; "son başarılı analiz yaşı" uyarısı henüz yok.
- **Veri kopyası saklama süresi yok.** `retain_snapshot` açık kaynaklarda kopyalar
  birikir; saklama/temizleme politikası gelecek iş.
- **Sözleşme önerisi bir taslaktır.** Eşikler örnekten çıkar; SLA gibi iş
  bilgisi tahmin edilmez, not olarak önerilir.
- **Standart hata testi bağımsız satır varsayar.** Otokorelasyonlu veride
  (zaman serisi) fazla duyarlı olabilir; pratik anlamlılık eşiği (%10) bunu sınırlar.
- Docker Desktop bu makinede çalışmıyordu; entegrasyon testleri yerel PostgreSQL 15
  ikili dosyalarıyla başlatılan geçici bir kümede koştu (CI ayrıca PostgreSQL 16 kullanır).

## 8. Sonraki adımlar

1. Bir pilot kullanıcıyla zamanlama + taslak sözleşme akışını uçtan uca deneyin;
   ilk alarmın ne kadar sürede anlamlı olduğunu ölçün.
2. Worker sağlığı: "son başarılı analiz yaşı" uyarısı ve `/health` ayrıntısı.
3. Veri kopyası saklama süresi; metrik geçmişinde eşik/baseline bandı.
4. Gerçek eval: farklı seed/şiddetli kayma, çoklu hata ve mevsimsellik senaryoları
   (özellik planındaki başarı ölçümü bölümü).
5. Pilot hesabıyla Snowflake veya BigQuery connector'ı (salt okunur rol ve sorgu bütçesi doğrulanarak).
6. Gerçek eval: tespit kalitesini (yeni geçmiş/çok değişkenli dedektörler dahil) etiketli sentetik
   senaryolarla ölçüp yanlış pozitif oranını raporlamak.
