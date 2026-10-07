const locale = document.documentElement.lang === 'tr' ? 'tr' : 'en';

const content = {
  en: {
    menuOpen: 'Open menu',
    menuClose: 'Close menu',
    steps: [
      {title: 'Understand the source', copy: 'A read-only PostgreSQL connection starts a scoped analysis of the selected table and partition. The connection secret stays in an environment variable, outside metadata.'},
      {title: 'Learn normal behavior', copy: 'Column profiles record null rates, distinct counts, and numeric distributions. Only clean scans accepted by an operator can become baselines.'},
      {title: 'Detect the deviation', copy: 'YAML contract rules and baseline comparisons work together. Scope, risk score, and signal type are attached to the incident record.'},
      {title: 'Trace the impact', copy: 'dbt and OpenLineage dependencies reveal downstream assets. Ownership, criticality, and SLA details help explain priority.'},
      {title: 'Connect the evidence', copy: 'Rule-based RCA generates hypotheses from signals and lineage. When enabled, the LLM only summarizes existing evidence in clearer language.'},
      {title: 'Validate the decision', copy: 'An approved fix template is tested on an isolated snapshot. The before-and-after difference and contract result are reviewed; approval does not execute SQL in production.'}
    ],
    scenarios: {
      f05: {code: 'F05 / 06', type: 'DISTRIBUTION + RANGE', name: 'Lira → subunit scale error', description: 'orders.total_amount values rise to roughly 100 times the expected level.', detection: 'A contract range violation and baseline distribution shift appear together.', cause: 'The rules engine presents a possible unit / scale change as an evidence-backed hypothesis.', action: 'A limited scale template is tested on an isolated snapshot; the result goes to human review.'},
      f04: {code: 'F04 / 06', type: 'VOLUME + UNIQUENESS', name: 'The same batch loaded twice', description: 'Row volume increases and the contract uniqueness rule is violated.', detection: 'A baseline volume shift and uniqueness violation are grouped into one incident.', cause: 'The rules engine flags a possible duplicate-load or idempotency issue.', action: 'Exactly matching rows can be deduplicated on an isolated snapshot; conflicting rows with different content are rejected.'},
      f03: {code: 'F03 / 06', type: 'COMPLETENESS + BASELINE', name: 'Sudden spike in null values', description: 'The null rate in total_amount rises from roughly 2% to 40%.', detection: 'The contract completeness threshold and historical profile comparison expose the deviation.', cause: 'The rules engine suggests missing values in an upstream collection step as a possible cause.', action: 'The source and affected assets are reviewed; if evidence is insufficient, a person investigates further.'}
    }
  },
  tr: {
    menuOpen: 'Menüyü aç',
    menuClose: 'Menüyü kapat',
    steps: [
      {title: 'Kaynağı tanı', copy: 'Salt okunur PostgreSQL bağlantısı, seçili tablo ve partition için kapsamı belli bir analiz başlatır. Bağlantı sırrı metadata yerine ortam değişkeninde kalır.'},
      {title: 'Normali öğren', copy: 'Kolon profilleri null, distinct ve sayısal dağılımları kaydeder. Yalnızca temiz ve operatör tarafından kabul edilmiş analizler baseline olabilir.'},
      {title: 'Sapmayı yakala', copy: 'YAML sözleşme kuralları ile baseline karşılaştırması birlikte çalışır. Kapsam, risk puanı ve sinyal türü olay kaydına bağlanır.'},
      {title: 'Etkisini göster', copy: 'dbt ve OpenLineage bağımlılıkları downstream varlıkları görünür kılar. Sahiplik, kritiklik ve SLA bilgisi önceliği açıklamaya yardımcı olur.'},
      {title: 'Kanıtı birleştir', copy: 'Kural tabanlı RCA sinyaller ve lineage üzerinden hipotez üretir. LLM etkinse yalnızca mevcut kanıtları daha anlaşılır biçimde özetler.'},
      {title: 'Kararı güvenceye al', copy: 'İzinli onarım şablonu izole snapshot üzerinde denenir. Önce/sonra farkı ve sözleşme sonucu incelenir; onay üretimde SQL çalıştırmaz.'}
    ],
    scenarios: {
      f05: {code: 'F05 / 06', type: 'DAĞILIM + ARALIK', name: 'TL → kuruş ölçek hatası', description: 'orders.total_amount değerleri beklenen düzeyin yaklaşık 100 katına çıkar.', detection: 'Contract aralık ihlali ve baseline dağılım sapması birlikte görünür.', cause: 'Kural motoru, olası birim / ölçek değişimini kanıta bağlı hipotez olarak sunar.', action: 'Sınırlı scale şablonu izole snapshot üzerinde denenir; sonuç insan incelemesine sunulur.'},
      f04: {code: 'F04 / 06', type: 'HACİM + TEKİLLİK', name: 'Aynı batch iki kez yüklendi', description: 'Kayıt hacmi yükselir ve sözleşmedeki benzersizlik kuralı ihlal edilir.', detection: 'Baseline hacim sapması ile uniqueness ihlali aynı olayda birleşir.', cause: 'Kural motoru, olası duplicate load / idempotency sorununu işaret eder.', action: 'Birebir aynı satırlar izole snapshot üzerinde tekilleştirilebilir; içerikleri farklı çakışan kayıtlar reddedilir.'},
      f03: {code: 'F03 / 06', type: 'DOLULUK + BASELINE', name: 'Null oranında ani artış', description: 'total_amount alanında null oranı yaklaşık %2 düzeyinden %40 düzeyine yükselir.', detection: 'Contract doluluk eşiği ve geçmiş profil karşılaştırması sapmayı gösterir.', cause: 'Kural motoru, upstream veri toplama adımındaki eksik değerleri olası neden olarak sunar.', action: 'Kaynak ve etkilenen varlıklar incelenir; kanıt yetersizse insan araştırması istenir.'}
    }
  }
}[locale];

document.querySelectorAll('.process-step').forEach(button => button.addEventListener('click', () => {
  const index = Number(button.dataset.step);
  document.querySelectorAll('.process-step').forEach(item => {
    item.classList.toggle('active', item === button);
    item.setAttribute('aria-pressed', String(item === button));
  });
  document.getElementById('detail-number').textContent = `${String(index + 1).padStart(2, '0')} / 06`;
  document.getElementById('detail-title').textContent = content.steps[index].title;
  document.getElementById('detail-copy').textContent = content.steps[index].copy;
}));

document.querySelectorAll('[data-scenario]').forEach(button => button.addEventListener('click', () => {
  const item = content.scenarios[button.dataset.scenario];
  document.querySelectorAll('[data-scenario]').forEach(tab => tab.setAttribute('aria-selected', String(tab === button)));
  document.getElementById('scenario-panel').setAttribute('aria-labelledby', button.id);
  Object.entries({code: 'scenario-code', type: 'scenario-type', name: 'scenario-name', description: 'scenario-description', detection: 'scenario-detection', cause: 'scenario-cause', action: 'scenario-action'}).forEach(([key, id]) => {
    document.getElementById(id).textContent = item[key];
  });
}));

const menuButton = document.querySelector('.menu-toggle');
const menu = document.querySelector('.main-nav');
menuButton.addEventListener('click', () => {
  const open = menu.classList.toggle('open');
  menuButton.setAttribute('aria-expanded', String(open));
  menuButton.setAttribute('aria-label', open ? content.menuClose : content.menuOpen);
});
menu.querySelectorAll('a').forEach(link => link.addEventListener('click', () => {
  menu.classList.remove('open');
  menuButton.setAttribute('aria-expanded', 'false');
  menuButton.setAttribute('aria-label', content.menuOpen);
}));

document.getElementById('print-page').addEventListener('click', () => window.print());
document.getElementById('year').textContent = String(new Date().getFullYear());
