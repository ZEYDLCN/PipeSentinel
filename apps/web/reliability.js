"use strict";
let accessToken = "", selectedId = null, sessionRole = "reader", refreshing = false;
const root = "/api/v1/reliability", $ = id => document.getElementById(id);
const csv = value => value.split(",").map(v => v.trim()).filter(Boolean);
function node(tag, text, cls) { const n = document.createElement(tag); if (text !== undefined) n.textContent = text; if (cls) n.className = cls; return n; }
function message(text, error=false) { $("message").textContent=text; $("message").className=error?"error":""; }
async function api(path, method="GET", body, headers={}) {
  const res=await fetch(root+path,{method, headers:{"Content-Type":"application/json",...(accessToken?{Authorization:"Bearer "+accessToken}:{}),...headers},...(body!==undefined?{body:JSON.stringify(body)}:{})});
  const result=await res.json();
  if(!res.ok) throw new Error(typeof result.detail==="string"?result.detail:JSON.stringify(result.detail));
  return result;
}
async function act(fn) { try { await fn(); } catch(e) { message(e.message,true); } }
function button(text, fn, mutation=false) { const b=node("button",text,"btn secondary"); b.type="button"; b.disabled=mutation&&sessionRole!=="operator"; b.onclick=()=>act(async()=>{b.disabled=true;try{await fn();}finally{b.disabled=mutation&&sessionRole!=="operator";}});return b; }
function table(target, headings, rows) { target.replaceChildren(); if(!rows.length){target.append(node("p","Henüz kayıt yok."));return;}const t=node("table"),head=node("tr");headings.forEach(h=>head.append(node("th",h)));t.append(head);rows.forEach(values=>{const tr=node("tr");values.forEach(value=>{const td=node("td");if(value instanceof Node)td.append(value);else td.textContent=value??"—";tr.append(td);});t.append(tr);});target.append(t); }
function actions(...children){const d=node("div");d.append(...children);return d;}
function pretty(title,value){const d=node("details");d.append(node("summary",title),node("pre",JSON.stringify(value,null,2)));return d;}
function time(value){return new Date(value).toLocaleString("tr-TR");}
function tab(name){document.querySelectorAll("section[id^='tab-']").forEach(s=>s.hidden=s.id!=="tab-"+name);document.querySelectorAll("[data-tab]").forEach(b=>{b.className=b.dataset.tab===name?"btn":"btn secondary";if(b.dataset.tab===name)b.setAttribute("aria-current","page");else b.removeAttribute("aria-current");});}
document.querySelectorAll("[data-tab]").forEach(b=>b.onclick=()=>tab(b.dataset.tab));
$("login").onclick=()=>$("login-dialog").showModal();$("cancel-login").onclick=()=>$("login-dialog").close();
$("login-form").onsubmit=e=>{e.preventDefault();act(async()=>{accessToken=new FormData(e.target).get("token");await refresh();e.target.reset();$("login-dialog").close();});};

async function refresh(){
  if(refreshing)return;refreshing=true;
  try{
    const session=await api("/session");sessionRole=session.role;$("session-state").textContent=session.actor+" · "+session.role;
    const [sources,jobs,observations,graphs,policies,notifications]=await Promise.all(["sources","jobs","observations","lineage","policies","notifications"].map(p=>api("/"+p)));
    const names=Object.fromEntries(sources.map(s=>[s.id,s.name]));
    renderOverview(sources,jobs,observations,notifications,names);
    table($("sources"),["Kaynak","Tablo","Kapsam","İşlem"],sources.map(s=>[s.name,s.config.schema+"."+s.config.table,s.config.max_rows+" satır",actions(button("Analiz başlat",async()=>{await api("/sources/"+s.id+"/analyze","POST",undefined,{"Idempotency-Key":crypto.randomUUID()});message("Analiz kuyruğa alındı.");await refresh();tab("monitor");},true),button("Düzenle",()=>editSource(s)))]));
    table($("jobs"),["Kaynak","Durum","Deneme","Bilgi"],jobs.map(j=>[names[j.source_id],j.status,j.attempts,actions(node("span",j.error||time(j.created_at)),...(j.status==="failed"?[button("Tekrar dene",async()=>{await api("/jobs/"+j.id+"/retry","POST");await refresh();},true)]:[]))]));
    table($("observations"),["Kaynak / zaman","Sonuç","Referans","İş önceliği","İncele"],observations.map(o=>[names[o.source_id]+" · "+time(o.created_at),o.signal_count+" sinyal · "+o.severity+(o.coverage.complete?"":" · kısmi tarama"),o.quality+" / "+o.baseline_state,o.impact.priority,button("Kanıtları aç",()=>showObservation(o.id))]));
    table($("policies"),["Varlık","Sahibi","Kritiklik","SLA"],policies.map(p=>[p.asset,p.owner,p.criticality,p.sla_minutes+" dk"]));
    table($("notifications"),["Kaynak","Önem","Teslim","Deneme"],notifications.map(n=>[n.body.source,n.body.severity,n.status,n.attempts]));
    $("graphs").replaceChildren();for(const graph of graphs){const area=node("div");area.append(node("h3",graph.namespace),node("p",graph.body.coverage));for(const [a,b] of graph.body.edges){const edge=node("div");edge.append(node("span",a,"graph-node"),node("span"," → "),node("span",b,"graph-node"));area.append(edge);}$("graphs").append(area);}
    document.querySelectorAll("#source-form button,#ingest-form button,#policy-form button").forEach(b=>b.disabled=sessionRole!=="operator");
  }finally{refreshing=false;}
}
$("refresh").onclick=()=>act(refresh);
function editSource(s){const f=$("source-form");f.elements.name.value=s.name;for(const [key,value] of Object.entries(s.config)){const input=f.elements.namedItem(key);if(!input)continue;if(input.type==="checkbox")input.checked=value;else if(key==="contract")input.value=JSON.stringify(value,null,2);else input.value=Array.isArray(value)?value.join(","):value;}for(const part of ["column","start","end"])f.elements["partition_"+part].value=s.config.partition?.[part]??"";tab("sources");f.scrollIntoView({behavior:"smooth"});}
$("validate-contract").onclick=()=>act(async()=>{await api("/contracts/validate","POST",{yaml:$("source-form").elements.contract.value});message("Sözleşme geçerli.");});
$("source-form").onsubmit=e=>{e.preventDefault();act(async()=>{const f=new FormData(e.target),validated=await api("/contracts/validate","POST",{yaml:f.get("contract")});const config={contract:validated.contract};for(const key of ["connection_env","schema","table"])config[key]=f.get(key);config.allowlist=[config.schema+"."+config.table];for(const key of ["max_rows","timeout_seconds"])config[key]=Number(f.get(key));for(const key of ["order_by","segment_by","sensitive_columns"])config[key]=csv(f.get(key));for(const key of ["seasonal","retain_snapshot"])config[key]=f.has(key);if(f.get("lineage_asset"))config.lineage_asset=f.get("lineage_asset");if(f.get("partition_column")){const c=f.get("partition_column"),numeric=["integer","float"].includes(config.contract.expected_schema[c]);config.partition={column:c,start:numeric?Number(f.get("partition_start")):f.get("partition_start"),end:numeric?Number(f.get("partition_end")):f.get("partition_end")};}await api("/sources","POST",{name:f.get("name"),config});message("Kaynak kaydedildi.");await refresh();});};
async function fileJSON(file){if(file.size>20000000)throw new Error("Dosya en fazla 20 MB olabilir.");return JSON.parse(await file.text());}
$("ingest-form").onsubmit=e=>{e.preventDefault();act(async()=>{const f=new FormData(e.target),body={namespace:f.get("namespace"),artifact:await fileJSON(f.get("artifact"))};if(f.get("results").size)body.run_results=await fileJSON(f.get("results"));const result=await api("/ingest/"+f.get("kind"),"POST",body);message(result.nodes+" varlık ve "+result.edges+" bağlantı alındı.");await refresh();});};
$("policy-form").onsubmit=e=>{e.preventDefault();act(async()=>{const f=new FormData(e.target);await api("/policies","POST",{asset:f.get("asset"),owner:f.get("owner"),criticality:Number(f.get("criticality")),sla_minutes:Number(f.get("sla_minutes"))});message("İş etkisi tanımı kaydedildi; sonraki analizde kullanılacak.");await refresh();});};

async function showObservation(id){
  selectedId=id;const item=await api("/observations/"+id);if(selectedId!==id)return;const b=item.body,area=$("observation-detail");area.hidden=false;area.replaceChildren(node("h2",b.rca.summary),node("p",time(item.created_at)+" · "+b.severity+" · grup "+b.group_id));
  area.append(node("div",b.coverage.scanned_rows+" / "+b.coverage.total_rows+" satır incelendi · "+(b.coverage.complete?"tam kapsam":"kısmi kapsam"),"metric"));
  area.append(actions(button("RCA'yı yeniden oynat",async()=>{const r=await api("/observations/"+id+"/replay","POST");message(r.matches?"Kaydedilmiş kanıtlar aynı kök neden raporunu üretti.":"Rapor farklı.");},true),...(!b.signals.length&&item.quality!=="accepted"?[button("Baseline olarak kabul et",async()=>{await api("/observations/"+id+"/baseline","POST");await showObservation(id);await refresh();},true)]:[])));
  const signalTable=node("div");table(signalTable,["Sinyal","Kolon","Önem","Kanıt"],b.signals.map(s=>[s.type,s.column,s.severity,pretty("Ayrıntı",s.evidence)]));area.append(signalTable);
  for(const h of b.rca.root_causes)area.append(node("h3",h.hypothesis),node("p","Güven: "+h.confidence+" · Kanıt: "+h.evidence_ids.join(", ")));
  area.append(pretty("İş etkisi ve sahipler",b.impact),pretty("Referans ve kapsama bilgisi",{baseline:b.baseline,coverage:b.coverage}),pretty("Zaman çizelgesi",item.timeline),pretty("Benzer doğrulanmış çözümler",item.similar_incidents));
  area.append(node("h3","Geri bildirim"));const feedback=node("div"),verdict=node("select");for(const [v,t] of [["correct_cause","Kök neden doğru"],["false_positive","Yanlış alarm"],["repair_failed","Çözüm başarısız"],["resolved","Çözüldü"]]){const option=node("option",t);option.value=v;verdict.append(option);}const note=node("input");note.placeholder="Çözüm veya değerlendirme notu";note.maxLength=2000;feedback.append(verdict,note,button("Kaydet",async()=>{await api("/observations/"+id+"/feedback","POST",{verdict:verdict.value,note:note.value});message("Geri bildirim kaydedildi.");await showObservation(id);},true));area.append(feedback,pretty("Geri bildirim geçmişi",item.feedback));
  area.append(node("h3","Onarımı veri kopyasında dene"),node("p","Sonuç saklanan veri kopyasını ve sözleşmesini kapsar. Downstream çalıştırılmaz; onay üretime yazmaz."));
  if(b.snapshot_available)repairForm(area,id);else area.append(node("p","Bu analiz için veri kopyası saklanmamış. Kaynak ayarından açıp yeni analiz başlatabilirsiniz."));
  for(const attempt of item.repairs){const v=attempt.body.validation,box=node("div");box.append(node("h3",(v.passed?"Doğrulandı":"Doğrulanamadı")+" · "+attempt.status),node("p",v.changed_rows+" satır değişti · Önce "+v.before_violations.length+", sonra "+v.after_violations.length+" ihlal"));const comparison=node("div");table(comparison,["Kolon","Önce ortalama","Sonra ortalama"],Object.keys(v.before_profile).filter(c=>"mean" in v.before_profile[c]).map(c=>[c,v.before_profile[c].mean,v.after_profile[c]?.mean]));box.append(comparison,pretty("Kontrol sonuçları",v));if(["validated","failed"].includes(attempt.status)){for(const decision of (v.passed?["approve","reject"]:["reject"]))box.append(button(decision==="approve"?"Onayla":"Reddet",async()=>{await api("/repairs/"+attempt.id+"/"+decision,"POST");await showObservation(id);},true));}area.append(box);}
  area.scrollIntoView({behavior:"smooth",block:"start"});
}
function repairForm(area,id){const form=node("form"),grid=node("div",undefined,"form-grid"),inputs={};function field(key,label,value,type="text"){const wrap=node("label",label),input=node("input");input.value=value;input.type=type;inputs[key]=input;wrap.append(input);grid.append(wrap);}field("type","Şablon (scale / deduplicate)","scale");field("column","Hedef kolon","total_amount");field("factor","Çarpan (0.01 / 0.1 / 10 / 100)","0.01","number");inputs.factor.step="any";field("filterColumn","Batch filtre kolonu","batch");field("filterValue","Batch filtre değeri","bad");field("keys","Tekilleştirme anahtarları","order_id");field("max","En fazla değiştirilecek satır","1000","number");const submit=node("button","Dene ve doğrula","btn");submit.disabled=sessionRole!=="operator";form.append(grid,submit);form.onsubmit=e=>{e.preventDefault();act(async()=>{submit.disabled=true;try{const action={type:inputs.type.value,max_changed_rows:Number(inputs.max.value)};if(action.type==="scale")Object.assign(action,{column:inputs.column.value,factor:Number(inputs.factor.value),where:{[inputs.filterColumn.value]:inputs.filterValue.value}});else action.keys=csv(inputs.keys.value);await api("/observations/"+id+"/repairs","POST",{action});await showObservation(id);}finally{submit.disabled=sessionRole!=="operator";}});};area.append(form);}
function renderOverview(sources,jobs,observations,notifications,names) {
  const total = observations.length;
  const clean = observations.filter(o=>o.signal_count===0).length;
  const accepted = observations.filter(o=>o.quality==="accepted").length;
  const issues = total-clean;
  const percent = value=>total ? Math.round(value/total*100) : 0;
  $("metric-sources").textContent=sources.length;
  $("metric-analyses").textContent=total;
  $("metric-incidents").textContent=issues;
  $("clean-percent").textContent=total ? "%"+percent(clean-accepted) : "—";
  $("issue-percent").textContent=total ? "%"+percent(issues) : "—";
  $("baseline-count").textContent=total ? "%"+percent(accepted) : "—";
  $("clean-track").style.width=(total?(clean-accepted)/total*100:0)+"%";
  $("issue-track").style.width=(total?issues/total*100:0)+"%";
  const pattern=document.querySelector(".track-pattern");
  pattern.style.flex="none";pattern.style.minWidth="0";
  pattern.style.width=(total?accepted/total*100:100)+"%";
  $("health-value").textContent=total?"%"+percent(clean):"—";
  $("health-arc").style.strokeDashoffset=452.4*(1-(total?clean/total:0));
  const partial=observations.filter(o=>!o.coverage.complete).length;
  $("health-caption").textContent=total?clean+" / "+total+" temiz"+(partial?" · "+partial+" kısmi tarama":" · tam tarama"):"İlk analizinizi başlatın";
  $("notification-dot").hidden=!notifications.some(n=>n.status==="pending"||n.status==="failed");

  const days=Array.from({length:7},(_,i)=>{const date=new Date();date.setHours(0,0,0,0);date.setDate(date.getDate()-6+i);return date;});
  const dateKey=date=>[date.getFullYear(),date.getMonth(),date.getDate()].join("-");
  const counts=days.map(day=>observations.filter(o=>dateKey(new Date(o.created_at))===dateKey(day)).length);
  const max=Math.max(1,...counts);
  $("weekly-total").textContent=counts.reduce((a,b)=>a+b,0);
  $("activity-trend").textContent=counts[6]+" bugün";
  $("activity-range").textContent="Son 50 kayıt";
  $("activity-chart").replaceChildren();
  days.forEach((day,index)=>{
    const col=node("div",undefined,"chart-day"+(index===6?" today":"")+(counts[index]===0?" zero":""));
    const label=day.toLocaleDateString("tr-TR",{day:"numeric",month:"long"})+": "+counts[index]+" analiz";
    col.title=label;col.setAttribute("aria-label",label);col.tabIndex=0;
    const track=node("div",undefined,"chart-bar-track"),bar=node("span",undefined,"chart-bar");
    bar.style.height=Math.max(3,counts[index]/max*80)+"px";track.append(bar);
    col.append(track,node("span",undefined,"day-dot"),node("span",day.toLocaleDateString("tr-TR",{weekday:"short"}),"day-label"));
    $("activity-chart").append(col);
  });

  const done=jobs.filter(j=>j.status==="succeeded").length;
  $("queue-ratio").textContent=jobs.length?Math.round(done/jobs.length*100)+"%":"—";
  $("queue-done").textContent=done;
  $("queue-running").textContent=jobs.filter(j=>j.status==="running").length;
  $("queue-pending").textContent=jobs.filter(j=>j.status==="pending").length;
  $("queue-total").textContent=jobs.length+" iş";
  const statusNames={succeeded:"Tamamlandı",running:"Çalışıyor",pending:"Sırada",failed:"İnceleme gerekli"};
  $("queue-preview").replaceChildren();
  if(!jobs.length)$("queue-preview").append(node("p","İlk analiziniz burada görünecek. Kaynak bağlayarak başlayın."));
  jobs.slice(0,4).forEach(job=>{
    const row=node("div",undefined,"queue-item"),text=node("div",undefined,"queue-item-text");
    text.append(node("strong",names[job.source_id]||"Veri kaynağı"),node("small",statusNames[job.status]+" · "+new Date(job.created_at).toLocaleTimeString("tr-TR",{hour:"2-digit",minute:"2-digit"})));
    row.append(node("span",job.status==="succeeded"?"↗":"◷","queue-icon"),text,node("span",job.status==="succeeded"?"✓":job.status==="failed"?"!":"", "queue-state "+job.status));
    $("queue-preview").append(row);
  });

  $("source-preview").replaceChildren();
  if(!sources.length)$("source-preview").append(node("p","İlk veri kaynağınızı bağlayın. Analiz sonuçları burada bir araya gelsin."));
  sources.slice(0,3).forEach(source=>{
    const row=node("div",undefined,"source-preview-item"),text=node("div");
    text.append(node("strong",source.name),node("small",source.config.schema+"."+source.config.table));
    const latest=observations.find(o=>o.source_id===source.id);
    row.append(node("span","◫","source-icon"),text,node("span",latest?(latest.signal_count?"İncele":"Temiz"):"Hazır","source-state"));
    $("source-preview").append(row);
  });
  $("activity-timeline").replaceChildren();
  if(!observations.length)$("activity-timeline").append(node("p","Analizler tamamlandıkça zaman çizelgeniz oluşur."));
  observations.slice(0,3).forEach(observation=>{
    const row=node("div",undefined,"timeline-item"),event=node("span",undefined,"timeline-event"),text=node("span");
    text.append(node("strong",names[observation.source_id]||"Veri analizi"),node("small",observation.signal_count?observation.signal_count+" sinyal · İnceleme bekliyor":"Analiz tamamlandı · Sinyal bulunmadı"));
    event.append(text,node("span","↗"));
    const link=node("button",undefined,"timeline-content");link.type="button";link.append(event);link.onclick=()=>act(()=>showObservation(observation.id));
    link.setAttribute("aria-label",(names[observation.source_id]||"Analiz")+" kanıtlarını incele");
    row.append(node("span",new Date(observation.created_at).toLocaleTimeString("tr-TR",{hour:"2-digit",minute:"2-digit"}),"timeline-time"),link);
    $("activity-timeline").append(row);
  });
  $("timeline-date").textContent=observations.length?new Date(observations[0].created_at).toLocaleDateString("tr-TR",{day:"numeric",month:"long"}):"Son analizler";
}
$("today-label").textContent=new Date().toLocaleDateString("tr-TR",{day:"numeric",month:"long",year:"numeric"});
$("overview-add-source").onclick=()=>{tab("sources");$("source-form").scrollIntoView({behavior:"smooth"});};
$("overview-sources").onclick=()=>tab("sources");
for(const id of ["overview-analyses","overview-health"])$(id).onclick=()=>$("analyses-panel").scrollIntoView({behavior:"smooth"});
$("overview-jobs").onclick=()=>$("jobs-panel").scrollIntoView({behavior:"smooth"});
$("overview-refresh").onclick=()=>act(refresh);
act(refresh);setInterval(()=>{if(!document.hidden)act(refresh);},5000);
