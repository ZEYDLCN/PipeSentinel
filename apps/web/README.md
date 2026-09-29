# apps/web

Ana ekran `/` veya `/reliability.html`, sentetik veri laboratuvarı `/demo`
adresindedir. İki ekran `theme.css` içindeki gri, krem, sarı ve kömür rengi
tasarım dilini paylaşır. `reliability.css` ana ekranın responsive kart düzenini
tanımlar. Özetler gerçek API sonuçlarından hesaplanır; boş durumda örnek veri
gösterilmez. Ayrıntılı kurulum: `docs/reliability-guide-tr.md`.

Faz 5 + Faz 7 Dashboard (§13, §14.3) — bağımlılıksız statik HTML/CSS/vanilla JS.
Build adımı yok; `apps/api`'nin FastAPI uygulaması bu dizini kök path'e
(`/`) mount eder ve `fetch`'ler aynı origin'deki `/api/v1/...` uçlarını
çağırır.

Tasarım doküman §18.1'de "MVP" sütununda React/Next.js önerilir; burada
bilinçli bir ikame yapıldı (diğer MVP ikameleriyle aynı mantık — Airflow
→ manuel tetikleme, Neo4j → adjacency table): sıfır Node.js/build
toolchain bağımlılığı, tek process, anında çalışır. Ölçek/ekip
büyüdüğünde Next.js'e geçiş bu dizini değiştirir, `apps/api` değişmez
(API sözleşmesi zaten ayrık).

## Sayfalar

- **Datasets** — her dataset'in en son run'ı + severity; "Sağlıklı run
  çalıştır" / "Hata enjekte et" (F01-F06) / "Baseline oluştur" aksiyonları.
  Bir hata enjekte edildiğinde RCA agent'ı otomatik tetiklenir (§10.2).
- **Incidents** — filtrelenebilir incident listesi; bir satıra tıklamak
  kanıtlı kök neden raporunu (hipotezler, etkilenen varlıklar, önerilen
  aksiyonlar, belirsizlikler, ham sinyaller) açar. "Yeniden analiz et"
  RCA'yı aynı run üzerinde tekrar çalıştırır. Her önerilen aksiyonun
  durum rozeti (pending/approved/rejected) ve Onayla/Reddet butonları
  var (§14.3) — karar verilince buton kilitlenir; **hiçbir SQL/dbt
  otomatik çalıştırılmaz**, yalnızca karar kaydedilir.
- **Lineage** — bir dataset/kolon için downstream etki ve olası upstream
  kaynakları hop'lara göre gruplanmış biçimde gösterir (§9.3).

## Tasarım

Sabit sol sidebar navigasyon (marka + nav + API sağlık göstergesi), üstte
sayfa başlığı, KPI istatistik kartları (Datasets), severity-vurgulu
dataset/hipotez kartları, güven skoru için gradyan progress bar, lineage
sonuçları için numaralı/bağlantılı "hop" akışı ve sağ-alt toast bildirim
sistemi. Google Fonts'tan Inter (arayüz) + JetBrains Mono (kod/id'ler)
yükleniyor; ağ yoksa sistem fontlarına düşer. Açık/koyu tema tam destekli.

## Geliştirme

Dosyaları düzenleyip tarayıcıyı yenilemek yeterli — derleme/paketleme
yok. `app.js` yalnızca `fetch` ve DOM API'leri kullanır, harici bir
JS kütüphanesine bağımlı değildir; ikonlar satır içi SVG'dir (harici
ikon fontu/CDN yok).
