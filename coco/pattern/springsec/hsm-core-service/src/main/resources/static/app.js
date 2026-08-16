const API = "/api/sensec/hsm/v1";
let apps = [];
let currentApp = null;

const $ = (id) => document.getElementById(id);

async function loadApps() {
  const res = await fetch(`${API}/demo/apps`);
  const data = await res.json();
  apps = data.apps;
  const select = $("appSelect");
  select.innerHTML = apps.map(a => `<option value="${a.app_id}">${a.app_id}</option>`).join("");
  select.addEventListener("change", () => selectApp(select.value));
  selectApp(apps[0].app_id);
  populateGrantSelectors();
  refreshGrants();
  populateRevealSelector();
  refreshConsumerAccounts();
}

function selectApp(appId) {
  currentApp = apps.find(a => a.app_id === appId);
  const allScopes = ["encrypt", "decrypt", "rotate", "grant"];
  $("scopesRow").innerHTML = allScopes.map(s => {
    const granted = currentApp.scopes.includes(s);
    return `<span class="scope-chip ${granted ? "" : "denied"}">${s}</span>`;
  }).join("");
  $("rotateBtn").disabled = !currentApp.scopes.includes("rotate");
}

function authHeaders(forApp = null) {
  const app = forApp || currentApp;
  return {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${app.token}`,
    "X-App-ID": app.app_id,
  };
}

// Grant management always acts as the app holding the 'grant' scope —
// this mirrors a real deployment where only a privileged admin app
// can call these endpoints, regardless of who is "logged in" elsewhere.
function adminApp() {
  return apps.find(a => a.scopes.includes("grant"));
}

function showResult(el, obj, isError = false) {
  el.className = "result" + (isError ? " error" : "");
  el.textContent = typeof obj === "string" ? obj : JSON.stringify(obj, null, 2);
}

const FIELD_EXPLAINERS = {
  ciphertext: "Opaque token — store as a single VARCHAR/TEXT column; pass it back to /decrypt as-is; never decode client-side",
  edek_id: "Reference to the wrapped data key, stored server-side — never the key itself",
  owner_app_id: "Bound into the AES-GCM tag as AAD; decrypt fails if this doesn't match",
  iv_b64: "Random per call — same plaintext never produces the same ciphertext twice",
  ciphertext_b64: "The encrypted data",
  tag_b64: "GCM auth tag — proves ciphertext and owner_app_id weren't tampered with",
  kek_version: "Which HSM master key version wrapped this record",
  algorithm: "Cipher used — persisted per-record so future algorithm migrations stay decryptable",
  encoding: "utf8 vs base64 — tells the caller how to interpret plaintext on the way back out",
  plaintext: "The recovered original data",
  decrypted_as: "The app that made this decrypt call — may differ from owner_app_id if a grant exists",
  cache: "Redis DEK Cache result — HIT skips Azure Key Vault unwrap; MISS unwraps and caches the DEK for 60 s",
  reused: "true = this call reused the current DEK for dek_name below instead of minting a fresh one — Latest EDEK Records won't grow on reuse",
  status: "Response envelope: always \"success\" here — errors use a different {detail} shape and never reach this panel",
  code: "Machine-readable outcome code, stable across API versions even if the human-readable message wording changes",
  message: "Human-readable summary of what happened, safe to show directly to an end user",
  correlation_id: "Same ID as the X-Correlation-Id response header — grep the service log for this to see every step this request took",
};

function showFieldBreakdown(el, fields, isError = false) {
  el.className = "result" + (isError ? " error" : "");
  if (isError) {
    el.textContent = typeof fields === "string" ? fields : JSON.stringify(fields, null, 2);
    return;
  }
  el.innerHTML = Object.entries(fields).map(([key, value]) => {
    const explainer = FIELD_EXPLAINERS[key];
    return `<div class="field-row">
        <span class="field-label">${key}</span>
        <span class="field-value">${value}${explainer ? `<span class="field-explainer">${explainer}</span>` : ""}</span>
      </div>`;
  }).join("");
}

async function encrypt() {
  const plaintext = $("plaintext").value;
  const data_classification = $("dataClassification").value || null;
  const dek_name = $("dekName").value.trim() || null;
  const btn = $("encryptBtn");
  btn.disabled = true;
  try {
    const res = await fetch(`${API}/encrypt`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ plaintext, data_classification, dek_name, end_user_id: $("encryptEndUserId").value || null, context: { source: "demo-ui" } }),
    });
    const data = await res.json();
    if (!res.ok) { showResult($("encryptResult"), data, true); return; }
    showFieldBreakdown($("encryptResult"), data);
    $("ciphertextToken").value = data.ciphertext;
  } catch (e) {
    showResult($("encryptResult"), String(e), true);
  } finally {
    btn.disabled = false;
    refreshAuditLog();
    refreshEdekRecords();
  }
}

function _edekIdFromToken(token) {
  try {
    const b64 = token.slice(3).replace(/-/g, "+").replace(/_/g, "/");
    const bin = atob(b64);
    const hex = Array.from(bin.slice(1, 17))
      .map(c => c.charCodeAt(0).toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
  } catch { return null; }
}

async function decrypt() {
  const btn = $("decryptBtn");
  btn.disabled = true;
  const token = $("ciphertextToken").value.trim();
  const edekId = token ? _edekIdFromToken(token) : null;
  const isHit = edekId ? _demoCacheLookup(edekId) : false;
  try {
    const body = {
      ciphertext: token,
      end_user_id: $("decryptEndUserId").value || null,
    };
    const res = await fetch(`${API}/decrypt`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (!res.ok) { showResult($("decryptResult"), data, true); return; }
    showFieldBreakdown($("decryptResult"), {
      plaintext: data.plaintext,
      owner_app_id: data.owner_app_id,
      decrypted_as: currentApp.app_id,
      cache: isHit ? "HIT (KV unwrap skipped)" : "MISS (KV unwrapped, now cached 60s)",
    });
  } catch (e) {
    showResult($("decryptResult"), String(e), true);
  } finally {
    btn.disabled = false;
    refreshAuditLog();
  }
}

async function rotate() {
  const btn = $("rotateBtn");
  btn.disabled = true;
  try {
    const res = await fetch(`${API}/admin/rotate-kek`, {
      method: "POST",
      headers: authHeaders(),
    });
    const data = await res.json();
    if (!res.ok) { showResult($("rotateResult"), data, true); return; }
    showResult($("rotateResult"), data);
  } catch (e) {
    showResult($("rotateResult"), String(e), true);
  } finally {
    btn.disabled = !currentApp.scopes.includes("rotate");
    refreshAuditLog();
    refreshHsmState();
    refreshEdekRecords();
  }
}

async function refreshHsmState() {
  const res = await fetch(`${API}/demo/hsm-state`);
  const data = await res.json();
  const tbody = document.querySelector("#hsmTable tbody");
  tbody.innerHTML = (data.versions || []).map(v => `<tr>
      <td>${v.version}</td>
      <td>${v.key_length_bits} bits</td>
      <td>${v.created_at ? new Date(v.created_at).toLocaleTimeString() : "-"}</td>
      <td>${v.is_current ? "✓ current" : ""}</td>
    </tr>`).join("");
}

async function refreshEdekRecords() {
  const res = await fetch(`${API}/demo/edek-records?limit=15`);
  const data = await res.json();
  const tbody = document.querySelector("#recordsTable tbody");
  tbody.innerHTML = data.records.map(r => `<tr>
      <td>${r.edek_id.slice(0, 8)}…</td>
      <td>${r.app_id}</td>
      <td>${r.kek_version}</td>
      <td>${r.algorithm}</td>
      <td>${r.encoding}</td>
      <td>${r.data_classification || "-"}</td>
      <td>${r.dek_name || "-"}</td>
      <td>${r.rotation_status}</td>
      <td><code>${r.edek_blob_preview}</code></td>
      <td>${r.created_at ? new Date(r.created_at).toLocaleTimeString() : "-"}</td>
    </tr>`).join("");
}

function populateRevealSelector() {
  $("revealAs").innerHTML = apps.map(a => `<option value="${a.app_id}">${a.app_id}</option>`).join("");
}

async function createConsumerAccount() {
  const btn = $("createAccountBtn");
  btn.disabled = true;
  try {
    const res = await fetch(`${API}/demo/consumer/accounts`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        customer_name: $("custName").value,
        email: $("custEmail").value,
        account_number: $("custAccountNumber").value,
      }),
    });
    const data = await res.json();
    showResult($("createAccountResult"), data, !res.ok);
    if (res.ok) {
      $("custName").value = "";
      $("custEmail").value = "";
      $("custAccountNumber").value = "";
    }
    await refreshConsumerAccounts();
    await refreshEdekRecords();
    await refreshAuditLog();
  } catch (e) {
    showResult($("createAccountResult"), String(e), true);
  } finally {
    btn.disabled = false;
  }
}

async function refreshConsumerAccounts() {
  const res = await fetch(`${API}/demo/consumer/accounts`);
  const data = await res.json();
  const tbody = document.querySelector("#consumerTable tbody");
  tbody.innerHTML = data.accounts.map(a => `<tr data-id="${a.id}">
      <td>${a.id}</td>
      <td>${a.customer_name}</td>
      <td>${a.email}</td>
      <td><code class="account-cell token-cell">${a.ciphertext}</code></td>
      <td>${a.dek_name || "-"}</td>
      <td>${a.created_at ? new Date(a.created_at).toLocaleString() : "-"}</td>
      <td><button class="reveal-btn" data-id="${a.id}">Reveal</button></td>
    </tr>`).join("");
  tbody.querySelectorAll(".reveal-btn").forEach(btn => {
    btn.addEventListener("click", () => revealConsumerAccount(btn.dataset.id));
  });
}

async function revealConsumerAccount(id) {
  const row = document.querySelector(`#consumerTable tr[data-id="${id}"]`);
  const cell = row.querySelector(".account-cell");
  const reveal_as = $("revealAs").value;
  try {
    const res = await fetch(`${API}/demo/consumer/accounts/${id}/reveal`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reveal_as, end_user_id: $("revealEndUserId").value || null }),
    });
    const data = await res.json();
    if (!res.ok) {
      cell.textContent = `denied (${reveal_as}): ${data.detail}`;
      cell.classList.add("reveal-denied");
      return;
    }
    cell.textContent = `${data.account_number}  (revealed as ${reveal_as})`;
    cell.classList.remove("reveal-denied");
    cell.classList.add("reveal-shown");
  } catch (e) {
    cell.textContent = String(e);
  }
}

function populateGrantSelectors() {
  const opts = apps.map(a => `<option value="${a.app_id}">${a.app_id}</option>`).join("");
  $("grantGrantee").innerHTML = opts;
  $("grantOwner").innerHTML = opts;
}

async function refreshGrants() {
  const res = await fetch(`${API}/admin/grants`, { headers: authHeaders(adminApp()) });
  const data = await res.json();
  const tbody = document.querySelector("#grantsTable tbody");
  if (!res.ok) { tbody.innerHTML = ""; return; }
  tbody.innerHTML = data.grants.map(g => `<tr>
      <td>${g.grantee_app_id}</td>
      <td>${g.owner_app_id}</td>
      <td>${g.created_at ? new Date(g.created_at).toLocaleString() : "-"}</td>
      <td><button class="revoke-btn" data-grantee="${g.grantee_app_id}" data-owner="${g.owner_app_id}">Revoke</button></td>
    </tr>`).join("");
  tbody.querySelectorAll(".revoke-btn").forEach(btn => {
    btn.addEventListener("click", () => removeGrant(btn.dataset.grantee, btn.dataset.owner));
  });
}

async function addGrant() {
  const grantee_app_id = $("grantGrantee").value;
  const owner_app_id = $("grantOwner").value;
  const btn = $("addGrantBtn");
  btn.disabled = true;
  try {
    const res = await fetch(`${API}/admin/grants`, {
      method: "POST",
      headers: authHeaders(adminApp()),
      body: JSON.stringify({ grantee_app_id, owner_app_id }),
    });
    const data = await res.json();
    showResult($("grantResult"), data, !res.ok);
    await refreshGrants();
  } catch (e) {
    showResult($("grantResult"), String(e), true);
  } finally {
    btn.disabled = false;
  }
}

async function removeGrant(grantee_app_id, owner_app_id) {
  await fetch(`${API}/admin/grants`, {
    method: "DELETE",
    headers: authHeaders(adminApp()),
    body: JSON.stringify({ grantee_app_id, owner_app_id }),
  });
  await refreshGrants();
}

async function refreshAuditLog() {
  const res = await fetch(`${API}/demo/audit-log?limit=30`);
  const data = await res.json();
  const tbody = document.querySelector("#auditTable tbody");
  tbody.innerHTML = data.events.map(ev => {
    const time = new Date(ev._epoch * 1000 || Date.now()).toLocaleTimeString();
    const statusClass = ev.status === "success" ? "status-success" : "status-failure";
    const detail = ev.edek_id ? `edek=${ev.edek_id.slice(0, 8)}…` : (ev.reason || ev.new_kek_version || "");
    return `<tr>
      <td>${time}</td>
      <td>${ev.event_type}</td>
      <td>${ev.app_id || "-"}</td>
      <td class="${statusClass}">${ev.status || "-"}</td>
      <td>${detail}</td>
    </tr>`;
  }).join("");
}

// ── Redis DEK Cache simulation ────────────────────────────────────────────────
// In demo mode NullDEKCache is active server-side, so we simulate cache state
// client-side: first decrypt of an edek_id is a MISS; repeats within 60s are HITs.
const _demoCache = new Map(); // edek_id → { cachedAt: Date, timer: id }
let _cacheHits = 0;
let _cacheMisses = 0;

function _demoCacheLookup(edekId) {
  const entry = _demoCache.get(edekId);
  if (entry) {
    _cacheHits++;
    refreshCachePanel(edekId, "HIT");
    return true;
  }
  _cacheMisses++;
  const timer = setTimeout(() => {
    _demoCache.delete(edekId);
    refreshCachePanel(null, null);
  }, 60000);
  _demoCache.set(edekId, { cachedAt: new Date(), timer });
  refreshCachePanel(edekId, "MISS");
  return false;
}

function refreshCachePanel(lastEdekId, lastResult) {
  $("cacheHits").textContent   = _cacheHits;
  $("cacheMisses").textContent = _cacheMisses;
  $("cacheSize").textContent   = _demoCache.size;
  const tbody = document.querySelector("#cacheTable tbody");
  if (_demoCache.size === 0) {
    tbody.innerHTML = '<tr><td colspan="4" style="color:#555b7a;font-style:italic;">Cache empty — decrypt something to populate</td></tr>';
    return;
  }
  const now = Date.now();
  tbody.innerHTML = [..._demoCache.entries()].map(([id, e]) => {
    const ageMs  = now - e.cachedAt.getTime();
    const ttlSec = Math.max(0, Math.ceil((60000 - ageMs) / 1000));
    const isLast = id === lastEdekId;
    return `<tr>
      <td><code>${id.slice(0,8)}…</code></td>
      <td>${e.cachedAt.toLocaleTimeString()}</td>
      <td>${ttlSec}s</td>
      <td style="color:${isLast && lastResult === 'HIT' ? '#10b981' : isLast && lastResult === 'MISS' ? '#f87171' : '#8b92b8'}">${isLast ? lastResult || '—' : '—'}</td>
    </tr>`;
  }).join("");
}

function showTab(name) {
  const views = { demo: $("demoView"), diagram: $("diagramView"), sequence: $("sequenceView"), status: $("statusView") };
  const tabs  = { demo: $("tabDemo"),  diagram: $("tabDiagram"),  sequence: $("tabSequence"),  status: $("tabStatus")  };
  for (const key of Object.keys(views)) {
    const isActive = key === name;
    views[key].hidden = !isActive;
    tabs[key].classList.toggle("active", isActive);
    tabs[key].setAttribute("aria-selected", String(isActive));
  }
  if (location.hash !== `#${name}`) history.replaceState(null, "", `#${name}`);
}

$("tabDemo").addEventListener("click",     () => showTab("demo"));
$("tabDiagram").addEventListener("click",  () => showTab("diagram"));
$("tabSequence").addEventListener("click", () => showTab("sequence"));
$("tabStatus").addEventListener("click",   () => showTab("status"));
const _hash = location.hash.replace("#", "");
showTab(["demo","diagram","sequence","status"].includes(_hash) ? _hash : "demo");

// ── Development status tab ────────────────────────────────────────────────────
// Backed by the DB (DevStatusController), not a bundled static file, so edits
// made here survive a restart.
const STATUS_LABELS = { N: "Not started", P: "In progress", C: "Completed" };

function escapeAttr(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

async function loadDevStatus() {
  const res = await fetch(`${API}/demo/dev-status`);
  const data = await res.json();

  const latest = data.items.reduce((max, r) => (r.updated_at && (!max || r.updated_at > max)) ? r.updated_at : max, null);
  $("statusUpdated").textContent = latest
    ? `${data.items.length} tracked items · last updated ${new Date(latest).toLocaleString()}`
    : `${data.items.length} tracked items`;

  const groups = new Map();
  for (const row of data.items) {
    if (!groups.has(row.category)) groups.set(row.category, []);
    groups.get(row.category).push(row);
  }

  $("statusCategoryList").innerHTML = [...groups.keys()].map(c => `<option value="${escapeAttr(c)}"></option>`).join("");

  $("statusGroups").innerHTML = [...groups.entries()].map(([category, rows]) => `
    <h3 style="margin:1.2rem 0 0.4rem;font-size:0.9rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.03em;">${category}</h3>
    <table class="status-table">
      <thead><tr><th style="width:4.5rem;">Status</th><th>Item</th><th>Notes</th><th style="width:9.5rem;"></th></tr></thead>
      <tbody>
        ${rows.map(r => `<tr data-id="${r.id}" data-category="${escapeAttr(category)}">
          <td>
            <select class="status-select" title="${STATUS_LABELS[r.status] || r.status}">
              ${["N", "P", "C"].map(s => `<option value="${s}" ${r.status === s ? "selected" : ""}>${s}</option>`).join("")}
            </select>
          </td>
          <td><input class="status-item-input" type="text" value="${escapeAttr(r.item)}" /></td>
          <td><input class="status-notes-input" type="text" value="${escapeAttr(r.notes || "")}" /></td>
          <td class="status-actions">
            <button class="status-save-btn">Save</button>
            <button class="status-delete-btn">Delete</button>
          </td>
        </tr>`).join("")}
      </tbody>
    </table>
  `).join("");
}

$("statusGroups").addEventListener("click", async (e) => {
  const row = e.target.closest("tr[data-id]");
  if (!row) return;
  const id = row.dataset.id;

  if (e.target.classList.contains("status-save-btn")) {
    const body = {
      category: row.dataset.category,
      item: row.querySelector(".status-item-input").value,
      status: row.querySelector(".status-select").value,
      notes: row.querySelector(".status-notes-input").value,
    };
    await fetch(`${API}/demo/dev-status/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    loadDevStatus();
  }

  if (e.target.classList.contains("status-delete-btn")) {
    if (e.target.dataset.confirm !== "1") {
      e.target.dataset.confirm = "1";
      e.target.textContent = "Confirm?";
      setTimeout(() => {
        e.target.dataset.confirm = "";
        e.target.textContent = "Delete";
      }, 3000);
      return;
    }
    await fetch(`${API}/demo/dev-status/${id}`, { method: "DELETE" });
    loadDevStatus();
  }
});

$("statusAddBtn").addEventListener("click", async () => {
  const category = $("statusNewCategory").value.trim();
  const item = $("statusNewItem").value.trim();
  const status = $("statusNewStatus").value;
  const notes = $("statusNewNotes").value.trim();
  if (!category || !item) {
    alert("Category and item are required.");
    return;
  }
  await fetch(`${API}/demo/dev-status`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ category, item, status, notes }),
  });
  $("statusNewCategory").value = "";
  $("statusNewItem").value = "";
  $("statusNewNotes").value = "";
  $("statusNewStatus").value = "N";
  loadDevStatus();
});

loadDevStatus();

$("encryptBtn").addEventListener("click", encrypt);
$("decryptBtn").addEventListener("click", decrypt);
$("rotateBtn").addEventListener("click", rotate);
$("addGrantBtn").addEventListener("click", addGrant);
$("createAccountBtn").addEventListener("click", createConsumerAccount);
$("clearCacheBtn").addEventListener("click", () => {
  for (const e of _demoCache.values()) clearTimeout(e.timer);
  _demoCache.clear();
  refreshCachePanel(null, null);
});

loadApps();
refreshAuditLog();
refreshHsmState();
refreshEdekRecords();
setInterval(refreshAuditLog, 3000);
setInterval(refreshHsmState, 5000);
setInterval(refreshEdekRecords, 5000);
