const steps=[
  {title:'Kaynağı tanı',copy:'Salt okunur PostgreSQL bağlantısı, seçili tablo ve partition için kapsamı belli bir analiz başlatır. Bağlantı sırrı metadata yerine ortam değişkeninde kalır.'},
  {title:'Normali öğren',copy:'Kolon profilleri null, distinct ve sayısal dağılımları kaydeder. Yalnızca temiz ve operatör tarafından kabul edilmiş analizler baseline olabilir.'},
  {title:'Sapmayı yakala',copy:'YAML sözleşme kuralları ile baseline karşılaştırması birlikte çalışır. Kapsam, risk puanı ve sinyal türü olay kaydına bağlanır.'},
  {title:'Etkisini göster',copy:'dbt ve OpenLineage bağımlılıkları downstream varlıkları görünür kılar. Sahiplik, kritiklik ve SLA bilgisi önceliği açıklamaya yardımcı olur.'},
  {title:'Kanıtı birleştir',copy:'Kural tabanlı RCA sinyaller ve lineage üzerinden hipotez üretir. LLM etkinse yalnızca mevcut kanıtları daha anlaşılır biçimde özetler.'},
  {title:'Kararı güvenceye al',copy:'İzinli onarım şablonu izole snapshot üzerinde denenir. Önce/sonra farkı ve sözleşme sonucu incelenir; onay üretimde SQL çalıştırmaz.'}
];
document.querySelectorAll('.process-step').forEach(button=>button.addEventListener('click',()=>{
  const index=Number(button.dataset.step);
  document.querySelectorAll('.process-step').forEach(item=>{item.classList.toggle('active',item===button);item.setAttribute('aria-pressed',String(item===button))});
  document.getElementById('detail-number').textContent=`${String(index+1).padStart(2,'0')} / 06`;
  document.getElementById('detail-title').textContent=steps[index].title;
  document.getElementById('detail-copy').textContent=steps[index].copy;
}));
const scenarios={
  f05:{code:'F05 / 06',type:'DAĞILIM + ARALIK',name:'TL → kuruş ölçek hatası',description:'orders.total_amount değerleri beklenen düzeyin yaklaşık 100 katına çıkar.',detection:'Contract aralık ihlali ve baseline dağılım sapması birlikte görünür.',cause:'Kural motoru, olası birim / ölçek değişimini kanıta bağlı hipotez olarak sunar.',action:'Sınırlı scale şablonu izole snapshot üzerinde denenir; sonuç insan incelemesine sunulur.'},
  f04:{code:'F04 / 06',type:'HACİM + TEKİLLİK',name:'Aynı batch iki kez yüklendi',description:'Kayıt hacmi yükselir ve sözleşmedeki benzersizlik kuralı ihlal edilir.',detection:'Baseline hacim sapması ile uniqueness ihlali aynı olayda birleşir.',cause:'Kural motoru, olası duplicate load / idempotency sorununu işaret eder.',action:'Birebir aynı satırlar izole snapshot üzerinde tekilleştirilebilir; içerikleri farklı çakışan kayıtlar reddedilir.'},
  f03:{code:'F03 / 06',type:'DOLULUK + BASELINE',name:'Null oranında ani artış',description:'total_amount alanında null oranı yaklaşık %2 düzeyinden %40 düzeyine yükselir.',detection:'Contract doluluk eşiği ve geçmiş profil karşılaştırması sapmayı gösterir.',cause:'Kural motoru, upstream veri toplama adımındaki eksik değerleri olası neden olarak sunar.',action:'Kaynak ve etkilenen varlıklar incelenir; kanıt yetersizse insan araştırması istenir.'}
};
document.querySelectorAll('[data-scenario]').forEach(button=>button.addEventListener('click',()=>{
  const item=scenarios[button.dataset.scenario];
  document.querySelectorAll('[data-scenario]').forEach(tab=>tab.setAttribute('aria-selected',String(tab===button)));
  document.getElementById('scenario-panel').setAttribute('aria-labelledby',button.id);
  Object.entries({code:'scenario-code',type:'scenario-type',name:'scenario-name',description:'scenario-description',detection:'scenario-detection',cause:'scenario-cause',action:'scenario-action'}).forEach(([key,id])=>document.getElementById(id).textContent=item[key]);
}));
const menuButton=document.querySelector('.menu-toggle');
const menu=document.querySelector('.main-nav');
menuButton.addEventListener('click',()=>{const open=menu.classList.toggle('open');menuButton.setAttribute('aria-expanded',String(open));menuButton.setAttribute('aria-label',open?'Menüyü kapat':'Menüyü aç')});
menu.querySelectorAll('a').forEach(link=>link.addEventListener('click',()=>{menu.classList.remove('open');menuButton.setAttribute('aria-expanded','false');menuButton.setAttribute('aria-label','Menüyü aç')}));
document.getElementById('print-page').addEventListener('click',()=>window.print());
document.getElementById('year').textContent=String(new Date().getFullYear());
