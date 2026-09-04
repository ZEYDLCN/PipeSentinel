const API = "/api/v1";
let currentIncidentId = null;

const PAGE_META = {
  datasets: { title: "Datasets", subtitle: "Her dataset'in en son pipeline run'ı ve severity durumu." },
  incidents: { title: "Incidents", subtitle: "Root Cause Agent tarafından üretilen kanıtlı kök neden raporları (§10-§12)." },
  lineage: { title: "Lineage", subtitle: "Downstream etki ve olası upstream kaynakları keşfedin (§9)." },
};

// ---------------------------------------------------------------------------
// yardımcılar
// ---------------------------------------------------------------------------

async function api(path, options) {
  const res = await fetch(API + path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = body.detail || detail;
    } catch (_) {}
    throw new Error(`${res.status}: ${detail}`);
  }
  return res.json();
}

function el(tag, attrs, children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === "text") node.textContent = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const child of children || []) node.appendChild(child);
  return node;
}

function badge(severity) {
  return el("span", { class: `badge ${severity}`, text: severity });
}

function fmtTime(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  return d.toLocaleString("tr-TR", { dateStyle: "short", timeStyle: "medium" });
}

function empty(text) {
  return el("div", { class: "empty", text });
}

function skeletonRows(n) {
  const wrap = el("div", {});
  for (let i = 0; i < n; i++) wrap.appendChild(el("div", { class: "skeleton skeleton-row" }));
  return wrap;
}

// ---------------------------------------------------------------------------
// toasts
// ---------------------------------------------------------------------------

function showToast(message, type = "info", timeout = 4200) {
  const stack = document.getElementById("toast-stack");
  const toast = el("div", { class: `toast ${type}` }, [el("span", { class: "dot" }), el("span", { text: message })]);
  stack.appendChild(toast);
  setTimeout(() => {
    toast.style.transition = "opacity 0.2s ease";
    toast.style.opacity = "0";
    setTimeout(() => toast.remove(), 200);
  }, timeout);
}

// ---------------------------------------------------------------------------
// tab geçişi
// ---------------------------------------------------------------------------

document.querySelectorAll("nav.nav .nav-item").forEach((btn) => {
  btn.addEventListener("click", () => {
    const view = btn.dataset.view;
    document.querySelectorAll("nav.nav .nav-item").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".view").forEach((v) => v.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById(`view-${view}`).classList.add("active");
    document.getElementById("page-title").textContent = PAGE_META[view].title;
    document.getElementById("page-subtitle").textContent = PAGE_META[view].subtitle;
    if (view === "incidents") loadIncidents();
    if (view === "datasets") loadDatasets();
  });
});

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------

const SEVERITY_ORDER = ["critical", "high", "medium", "low"];

async function loadFaultOptions() {
  const faults = await api("/faults");
  const select = document.getElementById("fault-select");
  select.innerHTML = "";
  for (const f of faults) {
    select.appendChild(el("option", { value: f.fault_id, text: `${f.fault_id} — ${f.label}` }));
  }
}

function renderStatRow(datasets) {
  const total = datasets.length;
  const withRuns = datasets.filter((d) => d.latest_run_id);
  const critical = withRuns.filter((d) => d.latest_severity === "critical").length;
  const healthy = withRuns.filter((d) => d.latest_severity === "low" || !d.latest_severity).length;
  const faulted = withRuns.filter((d) => d.latest_fault_id).length;

  const row = document.getElementById("stat-row");
  row.innerHTML = "";
  const cards = [
    { label: "Dataset", value: total, cls: "" },
    { label: "Kritik severity", value: critical, cls: critical > 0 ? "critical" : "low" },
    { label: "Sağlıklı", value: healthy, cls: "low" },
    { label: "Enjekte edilmiş fault", value: faulted, cls: faulted > 0 ? "critical" : "" },
  ];
  for (const c of cards) {
    row.appendChild(
      el("div", { class: "stat-card" }, [
        el("div", { class: "stat-label", text: c.label }),
        el("div", { class: `stat-value ${c.cls}`, text: String(c.value) }),
      ])
    );
  }
}

async function loadDatasets() {
  const container = document.getElementById("datasets-table");
  container.innerHTML = "";
  container.appendChild(skeletonRows(3));
  try {
    const datasets = await api("/datasets");
    renderStatRow(datasets);

    if (!datasets.length) {
      container.innerHTML = "";
      container.appendChild(empty("Henüz dataset yok. 'Sağlıklı run çalıştır' ile başlayın."));
      return;
    }

    const grid = el(
      "div",
      { class: "dataset-grid" },
      datasets.map((d) => {
        const sevClass = d.latest_severity ? `sev-${d.latest_severity}` : "sev-none";
        return el("div", { class: `dataset-card ${sevClass}` }, [
          el("div", { class: "ds-top" }, [
            el("div", { class: "ds-name", text: `${d.namespace}.${d.name}` }),
            d.latest_severity ? badge(d.latest_severity) : el("span", { class: "badge low", text: "no data" }),
          ]),
          el("div", { class: "ds-meta" }, [
            el("div", {}, [el("span", { class: "k", text: "run  " }), el("span", { class: "mono", text: d.latest_run_id ? d.latest_run_id.slice(0, 8) : "-" })]),
            el("div", {}, [el("span", { class: "k", text: "fault  " }), el("span", { class: "mono", text: d.latest_fault_id || "-" })]),
            el("div", {}, [el("span", { class: "k", text: "zaman  " }), el("span", {}, [document.createTextNode(fmtTime(d.latest_run_at))])]),
          ]),
        ]);
      })
    );
    container.innerHTML = "";
    container.appendChild(grid);

    const filterSelect = document.getElementById("incident-dataset-filter");
    const existing = new Set(Array.from(filterSelect.options).map((o) => o.value));
    for (const d of datasets) {
      if (!existing.has(d.name)) filterSelect.appendChild(el("option", { value: d.name, text: d.name }));
    }
  } catch (e) {
    container.innerHTML = "";
    container.appendChild(empty(`Hata: ${e.message}`));
  }
}

document.getElementById("btn-run-healthy").addEventListener("click", async (ev) => {
  const btn = ev.currentTarget;
  btn.disabled = true;
  try {
    await api("/pipeline-runs", { method: "POST", body: JSON.stringify({}) });
    showToast("Sağlıklı run tamamlandı.", "success");
    loadDatasets();
  } catch (e) {
    showToast(`Hata: ${e.message}`, "error");
  } finally {
    btn.disabled = false;
  }
});

document.getElementById("btn-run-fault").addEventListener("click", async (ev) => {
  const faultId = document.getElementById("fault-select").value;
  const btn = ev.currentTarget;
  btn.disabled = true;
  try {
    const outcome = await api("/pipeline-runs", { method: "POST", body: JSON.stringify({ fault_id: faultId }) });
    // Faulty dataset'in run'ı için RCA agent'ı otomatik tetikle (§10.2) —
    // kullanıcı elle "analyze" çağırmak zorunda kalmasın.
    const faultedDataset = Object.keys(outcome.reports).find(
      (ds) => outcome.reports[ds].signal_count > 0
    );
    if (faultedDataset) {
      const runId = outcome.run_ids[faultedDataset];
      await api(`/pipeline-runs/${runId}/analyze`, { method: "POST" });
      showToast(`${faultId} enjekte edildi ve analiz edildi.`, "success");
    } else {
      showToast(`${faultId} enjekte edildi ancak sinyal üretmedi.`, "info");
    }
    loadDatasets();
  } catch (e) {
    showToast(`Hata: ${e.message}`, "error");
  } finally {
    btn.disabled = false;
  }
});

document.getElementById("btn-bootstrap").addEventListener("click", async (ev) => {
  const btn = ev.currentTarget;
  btn.disabled = true;
  showToast("Baseline oluşturuluyor (14 run)…", "info", 2500);
  try {
    await api("/bootstrap", { method: "POST", body: JSON.stringify({ runs: 14 }) });
    showToast("Baseline hazır.", "success");
    loadDatasets();
  } catch (e) {
    showToast(`Hata: ${e.message}`, "error");
  } finally {
    btn.disabled = false;
  }
});

// ---------------------------------------------------------------------------
// Incidents
// ---------------------------------------------------------------------------

async function loadIncidents() {
  const container = document.getElementById("incidents-table");
  container.innerHTML = "";
  container.appendChild(skeletonRows(3));
  const dataset = document.getElementById("incident-dataset-filter").value;
  const severity = document.getElementById("incident-severity-filter").value;
  const params = new URLSearchParams();
  if (dataset) params.set("dataset", dataset);
  if (severity) params.set("severity", severity);

  try {
    const incidents = await api(`/incidents?${params.toString()}`);
    updateIncidentBadge(incidents);

    if (!incidents.length) {
      container.innerHTML = "";
      container.appendChild(empty("Incident yok. Datasets sekmesinden bir hata enjekte edip analiz edin."));
      return;
    }
    const table = el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { text: "Severity" }),
          el("th", { text: "Dataset" }),
          el("th", { text: "Özet" }),
          el("th", { text: "Zaman" }),
        ]),
      ]),
      el(
        "tbody",
        {},
        incidents.map((inc) =>
          el(
            "tr",
            { class: "clickable", onclick: () => showIncidentDetail(inc.id) },
            [
              el("td", {}, [badge(inc.severity)]),
              el("td", { class: "mono", text: inc.dataset }),
              el("td", { class: "incident-row-summary", text: inc.summary }),
              el("td", { text: fmtTime(inc.created_at) }),
            ]
          )
        )
      ),
    ]);
    container.innerHTML = "";
    container.appendChild(table);
  } catch (e) {
    container.innerHTML = "";
    container.appendChild(empty(`Hata: ${e.message}`));
  }
}

function updateIncidentBadge(incidents) {
  const openCount = incidents.filter((i) => i.status === "open").length;
  const badgeEl = document.getElementById("nav-incident-count");
  if (openCount > 0) {
    badgeEl.textContent = String(openCount);
    badgeEl.hidden = false;
  } else {
    badgeEl.hidden = true;
  }
}

document.getElementById("btn-refresh-incidents").addEventListener("click", loadIncidents);
document.getElementById("incident-dataset-filter").addEventListener("change", loadIncidents);
document.getElementById("incident-severity-filter").addEventListener("change", loadIncidents);

async function showIncidentDetail(id) {
  currentIncidentId = id;
  const panel = document.getElementById("incident-detail");
  panel.style.display = "block";
  try {
    const inc = await api(`/incidents/${id}`);
    renderIncidentDetail(inc);
    panel.scrollIntoView({ behavior: "smooth", block: "nearest" });
  } catch (e) {
    document.getElementById("detail-summary").textContent = `Hata: ${e.message}`;
  }
}

function renderIncidentDetail(inc) {
  document.getElementById("detail-summary").textContent = inc.summary;
  document.getElementById("detail-meta").innerHTML = "";
  document.getElementById("detail-meta").appendChild(
    el("span", { style: "display:flex;align-items:center;gap:8px;flex-wrap:wrap" }, [
      badge(inc.severity),
      el("span", { class: "mono", text: `dataset=${inc.dataset}` }),
      el("span", { class: "mono", text: `run=${inc.run_id.slice(0, 8)}` }),
      el("span", { text: fmtTime(inc.created_at) }),
    ])
  );

  const hyp = document.getElementById("detail-hypotheses");
  hyp.innerHTML = "";
  if (!inc.root_causes.length) {
    hyp.appendChild(empty("Güçlü bir hipotez üretilemedi."));
  } else {
    for (const h of inc.root_causes) {
      const pct = Math.round(h.confidence * 100);
      hyp.appendChild(
        el("div", { class: "hypothesis-card" }, [
          el("div", { class: "hypothesis-top" }, [
            el("div", { class: "hypothesis-text", text: h.hypothesis }),
            el("div", { class: "confidence" }, [
              el("div", { class: "confidence-track" }, [el("div", { class: "confidence-fill", style: `width:${pct}%` })]),
              el("div", { class: "confidence-label", text: `${pct}%` }),
            ]),
          ]),
          el("div", { class: "hypothesis-evidence", text: `kanıt: ${h.evidence_ids.map((x) => x.slice(0, 8)).join(", ")}` }),
        ])
      );
    }
  }

  const affected = document.getElementById("detail-affected");
  affected.innerHTML = "";
  if (!inc.affected_assets.length) affected.appendChild(empty("Etkilenen varlık bulunamadı."));
  for (const a of inc.affected_assets) affected.appendChild(el("span", { class: "chip mono", text: a }));

  renderActions(inc.recommended_actions);

  const uncertainties = document.getElementById("detail-uncertainties");
  uncertainties.innerHTML = "";
  for (const u of inc.uncertainties) uncertainties.appendChild(el("li", { text: u }));
  if (!inc.uncertainties.length) uncertainties.appendChild(el("li", { text: "Yok." }));

  const signals = document.getElementById("detail-signals");
  signals.innerHTML = "";
  signals.appendChild(
    el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { text: "Tip" }),
          el("th", { text: "Kolon" }),
          el("th", { text: "Skor" }),
          el("th", { text: "Kaynak" }),
        ]),
      ]),
      el(
        "tbody",
        {},
        inc.signals.map((s) =>
          el("tr", {}, [
            el("td", { class: "mono", text: s.type }),
            el("td", { class: "mono", text: s.column || "-" }),
            el("td", { class: "mono", text: s.score.toFixed(2) }),
            el("td", { text: s.source }),
          ])
        )
      ),
    ])
  );
}

// ---------------------------------------------------------------------------
// Actions / approval workflow (§14.3) — hiçbir aksiyon burada otomatik
// uygulanmaz, yalnızca approve/reject kararı kaydedilir.
// ---------------------------------------------------------------------------

function renderActions(actions) {
  const container = document.getElementById("detail-actions");
  container.innerHTML = "";
  if (!actions.length) {
    container.appendChild(empty("Aksiyon önerisi yok."));
    return;
  }
  container.appendChild(
    el("table", {}, [
      el("thead", {}, [
        el("tr", {}, [
          el("th", { text: "Tip" }),
          el("th", { text: "Açıklama" }),
          el("th", { text: "Onay?" }),
          el("th", { text: "Durum" }),
          el("th", { text: "" }),
        ]),
      ]),
      el(
        "tbody",
        {},
        actions.map((a) =>
          el("tr", {}, [
            el("td", { class: "mono", text: a.type }),
            el("td", { text: a.description }),
            el("td", { text: a.requires_approval ? "Evet" : "Hayır" }),
            el("td", {}, [badge(a.status)]),
            el("td", {}, [
              el(
                "div",
                { class: "action-buttons" },
                [
                  el("button", {
                    class: "btn tiny approve",
                    ...(a.status !== "pending" ? { disabled: "true" } : {}),
                    onclick: () => decideAction(a.id, "approve"),
                    text: "✓ Onayla",
                  }),
                  el("button", {
                    class: "btn tiny reject",
                    ...(a.status !== "pending" ? { disabled: "true" } : {}),
                    onclick: () => decideAction(a.id, "reject"),
                    text: "✗ Reddet",
                  }),
                ]
              ),
            ]),
          ])
        )
      ),
    ])
  );
}

async function decideAction(actionId, decision) {
  const actor = prompt(`${decision === "approve" ? "Onaylayan" : "Reddeden"} kişi:`, "dashboard-user");
  if (!actor) return;
  try {
    await api(`/actions/${actionId}/${decision}`, {
      method: "POST",
      body: JSON.stringify({ actor }),
    });
    showToast(decision === "approve" ? "Aksiyon onaylandı." : "Aksiyon reddedildi.", "success");
    if (currentIncidentId) await showIncidentDetail(currentIncidentId);
  } catch (e) {
    showToast(`Hata: ${e.message}`, "error");
  }
}

document.getElementById("btn-reanalyze").addEventListener("click", async (ev) => {
  if (!currentIncidentId) return;
  const btn = ev.currentTarget;
  btn.disabled = true;
  const originalHTML = btn.innerHTML;
  btn.textContent = "Analiz ediliyor…";
  try {
    await api(`/incidents/${currentIncidentId}/analyze`, { method: "POST" });
    await showIncidentDetail(currentIncidentId);
    showToast("Yeniden analiz tamamlandı.", "success");
    loadIncidents();
  } catch (e) {
    showToast(`Analiz hatası: ${e.message}`, "error");
  } finally {
    btn.disabled = false;
    btn.innerHTML = originalHTML;
  }
});

// ---------------------------------------------------------------------------
// Lineage
// ---------------------------------------------------------------------------

function renderHits(hits) {
  if (!hits.length) return empty("Sonuç yok.");
  const byHop = {};
  for (const h of hits) {
    (byHop[h.hop] = byHop[h.hop] || []).push(h);
  }
  const flow = el("div", { class: "hop-flow" });
  for (const hop of Object.keys(byHop).sort((a, b) => a - b)) {
    flow.appendChild(
      el("div", { class: "hop-group" }, [
        el("div", { class: "hop-index", text: hop }),
        el(
          "div",
          { class: "hop-nodes" },
          byHop[hop].map((h) => el("span", { class: `badge node-${h.node_type}`, text: `${h.name} · ${h.via_edge_type}` }))
        ),
      ])
    );
  }
  return flow;
}

async function lineageSearch() {
  const asset = document.getElementById("lineage-input").value.trim();
  const container = document.getElementById("lineage-result");
  if (!asset) return;
  container.innerHTML = "";
  container.appendChild(skeletonRows(2));
  try {
    const result = await api(`/lineage/${encodeURIComponent(asset)}`);
    container.innerHTML = "";
    const cols = el("div", { class: "lineage-cols" }, [
      el("div", {}, [
        el("div", { class: "section-title", text: `Downstream etki (${result.downstream.length})` }),
        renderHits(result.downstream),
      ]),
      el("div", {}, [
        el("div", { class: "section-title", text: `Olası upstream kaynaklar (${result.upstream.length})` }),
        renderHits(result.upstream),
      ]),
    ]);
    container.appendChild(cols);
  } catch (e) {
    container.innerHTML = "";
    container.appendChild(empty(`Hata: ${e.message}`));
  }
}

document.getElementById("btn-lineage-search").addEventListener("click", lineageSearch);
document.getElementById("lineage-input").addEventListener("keydown", (e) => {
  if (e.key === "Enter") lineageSearch();
});

// ---------------------------------------------------------------------------
// init
// ---------------------------------------------------------------------------

async function checkApiHealth() {
  const dot = document.querySelector(".status-pill .dot");
  const text = document.getElementById("api-status-text");
  try {
    await api("/health");
  } catch (_) {
    dot.style.background = "var(--critical)";
    dot.style.boxShadow = "0 0 0 3px var(--critical-bg)";
    text.textContent = "API bağlantısı yok";
  }
}

loadDatasets();
loadFaultOptions();
checkApiHealth();
// Sidebar'daki açık incident sayısını başlangıçta da göster.
api("/incidents").then(updateIncidentBadge).catch(() => {});
