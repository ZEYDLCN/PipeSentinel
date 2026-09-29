# PipeSentinel — Özellik ve farklılaşma planı

Tarih: 29 Eylül 2026. İncelenen commit: `d822ac6`.
Durum: Aşağıdaki önceliklerin ilk çalışan sürümleri eklendi. Gerçek kaynak,
baseline, dbt/OpenLineage, olay yönetimi, sandbox, hafıza, webhook ve CI için
[kullanım kılavuzu](reliability-guide-tr.md) uygulanan kapsamı ve sınırlarını
açıklar. Bu dosyanın devamı ilk planı ve hedef kabul ölçütlerini korur;
üretimde otomatik execution ayrı sonraki aşamadır.

## Ürün odağı

Önerilen ilk hedef kitle: PostgreSQL ve dbt kullanan küçük/orta veri ekipleri.
Bu hedef, mevcut Python/PostgreSQL mimarisinden hareketle seçilmiş bir varsayımdır;
pilot görüşmeleriyle doğrulanmalıdır.

Ürün vaadi: **Veri arızasının nedenini kanıtlarıyla göster, önerilen düzeltmeyi
izole veride dene, sonuçlarını karşılaştır ve insan onayına sun.**

İlk pilot yalnızca okuma ve izole ortamda doğrulama yapar. Üretimde onarım
çalıştırma, doğrulama ve yetkilendirme tamamlandıktan sonraki ayrı bir aşamadır.

## Mevcut durum: koddan doğrulananlar

| Alan | Bugün | Plan için anlamı |
|---|---|---|
| Veri akışı | `orchestrator.py`, sentetik commerce batch'leri üretip yükler ve bu DataFrame'leri profiller | Genel bir kaynak bağlantısı ve tarama akışı gerekli |
| Kalite kontrolü | Contract kontrolleri, profil karşılaştırması, risk skoru | İlk pilot için kullanılabilecek çekirdek mevcut |
| Baseline | `pipeline.py:get_baseline_profile`, son başarılı ve fault işareti olmayan run'ı seçer | Teknik başarı kalite başarısı değildir; anomalili gerçek run referansı kirletebilir |
| Lineage | Grafik sorguları var; commerce ilişkileri kodla tanımlanıyor | Gerçek dbt/OpenLineage verisi alınmalı |
| RCA | Kural tabanlı hipotez, kanıt ve opsiyonel LLM sentezi | Değişiklik geçmişiyle desteklenebilir |
| Onarım | Aksiyon taslağı ve onay/ret kaydı | Onay, SQL çalıştırmaz; sandbox ve execution yok |
| Arayüz | Statik dashboard ve senkron API | Uzun analizler için arka plan işi ve durum ekranı gerekli |
| Değerlendirme | F01–F06 sentetik senaryolar | Gerçek dünyada doğruluk iddiası için kapsam yetersiz |

Kaynaklar: `src/pipeline_sentinel/{orchestrator,pipeline,detector,rca}.py`,
`apps/api/{main,deps}.py`, `evals/README.md`.

## Öncelikli özellikler

Eforlar tek geliştirici için kaba mühendislik tahminidir. Pilot geri bildirimi,
dağıtım ortamı ve veri hacmine göre değişir; satırlar doğrudan takvim taahhüdü değildir.

| Sıra | Özellik | Kullanıcıya fayda | Bağımlılık | Efor |
|---|---|---|---|---|
| P0 | Salt okunur PostgreSQL connector | Kendi tablo ve partition'larını izleme | Kaynak konfigürasyonu, sorgu bütçesi | 1–2 hafta |
| P0 | Kaliteye göre baseline seçimi | Bozuk veriyi normal kabul etmeme | Profil geçmişi, kalite durumu | 3–5 gün |
| P0 | YAML contract ve CLI doğrulaması | Kuralları Git üzerinden yönetme | Contract şeması ve sürümleme | 3–5 gün |
| P0 | Pilot işletim temeli | Güvenilir erişim ve uzun iş takibi | Kimlik, roller, job kaydı | 1–2 hafta |
| P1 | dbt artifact ingestion | Gerçek modelleri ve bağımlılıkları görme | Connector, varlık kimlikleri | 1 hafta |
| P1 | Zaman çizelgesi ve incident gruplama | Aynı arızaya ait alarmları birlikte inceleme | Lineage, sinyal zamanları | 1 hafta |
| P1 | Repair sandbox ve önce/sonra farkı | Önerilen düzeltmenin etkisini görme | Snapshot, contract, audit | 2–3 hafta |
| P1 | İş etkisi ve sahiplik | Önce kritik raporu etkileyen arızayı çözme | Lineage, varlık kataloğu | 3–5 gün |
| P2 | Mevsimsel ve segment bazlı tespit | Kampanya/hafta sonu değişimini arızadan ayırma | Yeterli temiz geçmiş | 1–2 hafta |
| P2 | Incident hafızası | Daha önce doğrulanan çözümü bulma | Sonuç ve çözüm kayıtları | 1 hafta |
| P2 | PR kalite kontrolü | Hatalı değişikliği yayınlanmadan yakalama | dbt ingestion, sandbox | 1–2 hafta |

### P0 kabul ölçütleri

- Connector, allowlist içindeki seçili tabloda profil üretir; kaynakta DDL/DML
  yapmaz. Metadata ayrı bağlantıda tutulur. Sırlar kayıtlara yazılmaz.
- Tarama kapsamı (tam/örnek/partition), satır sayısı, süre ve örnekleme yöntemi
  raporda görünür. Büyük tablolar bütçeyle taranır; örnekleme kesin sonuç gibi sunulmaz.
- İlk sürümde SQL tarafında temel agregasyonlar tercih edilir; tüm tabloyu
  Pandas belleğine alma zorunluluğu oluşturulmaz.
- Baseline yalnızca açıkça kabul edilmiş kaliteli run'lardan seçilir.
  Cold start, yetersiz geçmiş ve baseline sürümü arayüzde görünür.
- YAML sözleşmesindeki hata, veriye bağlanmadan satır/alan bilgisiyle açıklanır.
- Okuyucu ve operatör rolleri ayrılır; audit aktörü istemci metninden değil
  doğrulanmış kimlikten alınır. Demo hata enjeksiyonu pilot ortamında kapalıdır.
- Analiz arka planda yürür; pending/running/succeeded/failed durumları, zaman
  aşımı ve tekrar denemede idempotency vardır. İlk aşamada PostgreSQL job tablosu yeterlidir.

## Ayırt edici özellikler

### 1. Kanıtlı onarım: öner → dene → karşılaştır → onaya sun

Örnek: `orders.total_amount` 100 kat artınca sistem birim hatası hipotezi üretir.
Sadece bu bulguya dayanarak bütün tutarları bölmez. Etkilenen batch'i,
para birimini ve kaynak sözleşmesini inceler; kanıt yetersizse inceleme ister.
İzole snapshot'ta izinli düzeltmeyi dener; contract, satır sayısı, toplam tutar
ve downstream kontrollerinin önce/sonra sonuçlarını sunar.

Kabul: Üretim bağlantısı sandbox tarafından yazılamaz; işlem sınırı ve zaman
aşımı vardır; değişen satırlar ve değişmeyen kontrol grubu raporlanır. Yanlış
onarım fixture'ı doğrulamadan geçemez. Eksik downstream kapsamı açıkça belirtilir.

İlk sürümde yalnızca şablonlanmış, sınırlandırılmış aksiyonlar vardır. İlerideki
üretim execution'ı ayrıca sürüme bağlı onay, güncellik kontrolü, en az yetki,
idempotency, geri alma/backfill planı ve işlem sonrası doğrulama gerektirir.

### 2. Olayı yeniden oynatma ve değişiklik zaman çizelgesi

Incident'e profil/contract sürümü, dbt model değişimi, run ve hipotez kanıtları
bağlanır. Aynı snapshot ve sürümlerle yeniden analiz yapılabilir. Ham veri
saklanamıyorsa yalnızca profil düzeyinde tekrarın mümkün olduğu belirtilir.

Kabul: Aynı giriş ve kural sürümü aynı deterministik sonucu üretir. Zaman
yakınlığı kök neden kanıtı gibi sunulmaz; alternatif açıklamalar korunur.

### 3. İş etkisine göre önceliklendirme

“Kolon bozuk” bilgisinin yanında “finans dashboard'u etkilenebilir; sahibi
finans ekibi” gösterilir. Öncelik; severity, kritik varlıklar, SLA ve sahiplikle
açıklanır. Graf erişilebilirliği, doğrulanmış veri bozulmasından ayrı gösterilir.

Kabul: Her öncelik puanının bileşenleri görünür. Parasal etki ancak kullanıcı
tarafından tanımlanmış hesapla ve kapsamıyla verilir; tahmini kayıp uydurulmaz.

### 4. Tekrarlayan incident'lerden öğrenme

Benzer sinyal + şema + kaynak bağlamında geçmişte doğrulanmış çözümler önerilir.
İlk sürüm SQL/etiket aramasıyla başlar; yeterli kayıt birikmeden vector altyapısı
kurulmaz. Kullanıcı “doğru neden”, “yanlış alarm”, “çözüm başarısız” geri bildirimi verir.

Kabul: Öneri geçmiş incident'e bağlanır; eski ortamda işe yaraması yeni onarımı
otomatik onaylamaz. Hassas değerler maskeleme ve erişim kurallarına tabidir.

## Farklılaşma iddiasının sınırı

Lineage, RCA ve PR'da veri karşılaştırması tek başına benzersiz değildir:
[Monte Carlo](https://www.montecarlodata.com/blog-how-assurance-achieves-data-trust-at-scale-for-financial-services-with-data-observability-2/)
lineage ve RCA kullanımını,
[Datafold](https://www.datafold.com/dbt/)
dbt ile veri farkı ve downstream etki incelemesini anlatıyor.
Bu, sınırlı bir ürün taramasıdır; kapsamlı rekabet analizi değildir.

Önerilen farklılaşma hipotezi: küçük ekibin kendi ortamında çalıştırabildiği,
LLM zorunluluğu olmayan, kanıtları yeniden üretilebilen ve düzeltme sonucunu
gösteren uçtan uca deneyim. Bu paketin değeri ve rakiplere göre üstünlüğü
pilotlarda ölçülmelidir.

## Uygulama sırası

1. **Gerçek veri MVP'si:** PostgreSQL connector, YAML contract, kaliteli baseline,
   erişim kontrolü ve arka plan analizleri. Çıkış: sentetik üretici kullanılmadan
   harici test kaynağında uçtan uca incident oluşturma.
2. **Kanıt ve bağlam:** dbt artifact ingestion, zaman çizelgesi, incident gruplama,
   varlık sahipliği. Çıkış: aynı upstream arızanın ilişkili etkilerini tek ekranda açıklama.
3. **Onarım doğrulama:** İzole snapshot, sınırlı aksiyon, önce/sonra veri farkı,
   contract doğrulaması ve onay raporu. Çıkış: F05 benzeri doğru onarımı kabul,
   yanlış onarımı reddeden tekrarlanabilir demo.
4. **Pilotla iyileştirme:** Mevsimsellik, incident hafızası, bildirimler ve PR kalite
   kontrolü; öncelik gerçek kullanım verisine göre belirlenir.

Modüler monolit korunur. Yeni sınırlar: `connectors/`, `baseline.py`,
`ingestion/dbt.py`, `incident_groups.py`, `repair/`. Model önerileri:
`sources`, `contract_versions`, `baseline_versions`, `analysis_jobs`,
`change_events`, `repair_attempts`, `validation_results`. Mevcut `actions`
ve `approvals` kayıtları genişletilir; ayrı bir onay sistemi kurulmaz.

İlk teslimat paketi: connector arayüzü + PostgreSQL adaptörü + YAML doğrulama
+ baseline uygunluk durumu + dashboard'dan gerçek kaynakta analiz. Diğer
warehouse bağlantıları, Kafka, Kubernetes ve vector DB pilot sonrasına bırakılır.

## Başarı ölçümü

- F01–F06 regresyonları korunur; farklı seed, şema ve hata şiddetleri eklenir.
- Çoklu hata, mevsimsellik, gecikmeli partition, join fan-out, sessiz satır
  kaybı ve yetersiz kanıt senaryolarından ayrı değerlendirme seti oluşturulur.
- False-positive oranı, detection recall ve RCA Top-1 kapsamla birlikte raporlanır;
  altı sentetik senaryodaki başarı üretim doğruluğu olarak sunulmaz.
- Sandbox için yanlış onarım kabulü, doğru onarım reddi ve doğrulama kapsamı ölçülür.
- Pilotta tespit süresi, teşhis süresi, incident başına insan emeği, sorgu maliyeti
  ve kullanıcı tarafından doğrulanmış çözüm oranı izlenir.
- Sayısal ürün hedefleri ilk pilot ölçümünden sonra belirlenir.

## Yerel inceleme notu

İlk incelemede `git fetch origin` tamamlandı; `HEAD...origin/main`
karşılaştırması `0 / 0` idi. Sonraki uygulama turunda bu plandaki özelliklerin
ilk sürümleri eklendi; mevcut yerel çalışmalar korundu. Uygulanan kapsam,
kurulum ve bilinen sınırlar [kullanım kılavuzunda](reliability-guide-tr.md).
