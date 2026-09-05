import { useState, useEffect, useCallback } from 'react';

// ── This service is fully independent of this app's backend ──────────────────
// Istio routes /api/sensec/hsm/v1/* directly to the HSM encryption service's own
// pod. No Express proxy here — see vite.config.js for the local-dev-only proxy.
const HSM_BASE = '/api/sensec/hsm/v1';

// ── Flows tab renderer: Mermaid vs. the original hand-coded SVG ──────────────
// Flip this one constant to instantly roll back to the hand-coded SVG
// SequenceDiagram/OverviewSequenceDiagram components below (kept in place,
// unmodified) if the Mermaid rendering doesn't hold up under validation —
// no git revert needed. See docs/adr/0014-sensec-hsm-demo.md's 2026-07-21
// amendment for the tradeoffs (loss of per-arrow DENY/ALLOW color; gain of
// far less brittle, non-coordinate-based diagram source).
const USE_MERMAID_FLOWS = true;

const ALL_SCOPES = ['encrypt', 'decrypt', 'rotate', 'grant'];

// Field explainers shared by both the Encrypt and Decrypt breakdown panels —
// matches the master source's FIELD_EXPLAINERS in hsm_project/app/static/app.js.
const FIELD_EXPLAINERS = {
  ciphertext: 'Opaque token — store as a single VARCHAR/TEXT column; pass it back to /decrypt as-is; never decode client-side',
  edek_id:      'Reference to the wrapped data key, stored server-side — never the key itself',
  owner_app_id: "Bound into the AES-GCM tag as AAD; decrypt fails if this doesn't match",
  kek_version:  'Which HSM master key version wrapped this record',
  algorithm:    'Cipher used — persisted per-record so future algorithm migrations stay decryptable',
  encoding:     'utf8 vs base64 — tells the caller how to interpret plaintext on the way back out',
  plaintext:    'The recovered original data',
  decrypted_as: 'The app that made this decrypt call — may differ from owner_app_id if a grant exists',
  cache:        'Redis DEK Cache result — HIT skips Azure Key Vault unwrap; MISS unwraps and caches the DEK for 60s',
  reused:       'true = this call reused the current DEK for dek_name below instead of minting a fresh one — Latest EDEK Records won\'t grow on reuse',
  status:       'Response envelope: always "success" here — errors use a different {detail} shape and never reach this panel',
  code:         'Machine-readable outcome code, stable across API versions even if the human-readable message wording changes',
  message:      'Human-readable summary of what happened, safe to show directly to an end user',
  correlation_id: 'Same ID as the X-Correlation-Id response header — grep the service log for this to see every step this request took',
};

// ciphertext leads — it's the only field a caller actually needs to
// store; the rest is a breakdown of what the token bundles internally.
const ENCRYPT_FIELD_ORDER = ['ciphertext', 'edek_id', 'owner_app_id', 'kek_version', 'algorithm', 'encoding', 'reused', 'status', 'code', 'message', 'correlation_id'];

// Decodes the edek_id back out of a ciphertext token, purely for this demo's
// client-side "simulated cache" panel — ports hsm_project's own
// _edekIdFromToken() so the simulated hit/miss keying matches the real
// token format (3-char version prefix + base64url; bytes 1-16 are the UUID).
function edekIdFromToken(token) {
  try {
    const b64 = token.slice(3).replace(/-/g, '+').replace(/_/g, '/');
    const bin = atob(b64);
    const hex = Array.from(bin.slice(1, 17)).map((c) => c.charCodeAt(0).toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  } catch { return null; }
}

// ── Fetch helper — attaches Authorization + X-App-ID when an app is given ────
async function callApi(path, { method = 'GET', body, app, extraHeaders } = {}) {
  const headers = { 'Content-Type': 'application/json', ...extraHeaders };
  if (app) {
    headers.Authorization = `Bearer ${app.token}`;
    headers['X-App-ID'] = app.app_id;
  }
  let data = null;
  let networkError = null;
  try {
    const res = await fetch(`${HSM_BASE}${path}`, {
      method,
      headers,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    try { data = await res.json(); } catch { /* e.g. 204 No Content */ }
    return { ok: res.ok, status: res.status, data };
  } catch (err) {
    networkError = err;
  }
  return { ok: false, status: 0, data: null, networkError };
}

function errMessage({ status, data, networkError }, fallback) {
  if (networkError) return 'Network error — is the HSM service reachable?';
  return `${status}: ${data?.detail || data?.error || fallback}`;
}

function truncate(str, n) {
  if (!str) return '';
  return str.length > n ? `${str.slice(0, n)}…` : str;
}

function fmtTime(value) {
  if (!value) return '—';
  // _epoch is seconds; created_at/ISO strings parse directly
  const d = typeof value === 'number' ? new Date(value * 1000) : new Date(value);
  return Number.isNaN(d.getTime()) ? String(value) : d.toLocaleTimeString();
}

// ── Scope chips ────────────────────────────────────────────────────────────────
function ScopeChips({ scopes }) {
  const granted = new Set(scopes || []);
  return (
    <div style={s.chipRow}>
      {ALL_SCOPES.map((scope) => (
        <span key={scope} style={{ ...s.chip, ...(granted.has(scope) ? s.chipGranted : s.chipDenied) }}>
          {scope}
        </span>
      ))}
    </div>
  );
}

// ── Labeled-rows renderer for encrypt/decrypt responses ───────────────────────
function LabeledRows({ rows }) {
  return (
    <div style={s.rowsBox}>
      {rows.map(({ label, value, explainer }) => (
        <div key={label} style={s.row}>
          <div style={s.rowHead}>
            <span style={s.rowLabel}>{label}</span>
            {explainer && <span style={s.rowExplainer}>{explainer}</span>}
          </div>
          <div style={s.rowValue}>{value}</div>
        </div>
      ))}
    </div>
  );
}

function Panel({ title, sub, children }) {
  return (
    <section style={s.panel}>
      <div style={s.panelHead}>
        <h3 style={s.panelTitle}>{title}</h3>
        {sub && <p style={s.panelSub}>{sub}</p>}
      </div>
      {children}
    </section>
  );
}

// ── Architecture diagram tab ───────────────────────────────────────────────────
function ArchitectureDiagram() {
  return (
    <div style={s.diagramWrap}>
      <svg viewBox="0 0 1320 1330" xmlns="http://www.w3.org/2000/svg" role="img" style={s.diagramSvg}>
        <title>HSM Core Service Architecture — replicated from hsm_bouncy/java/hsm-core-service/src/main/resources/static/index.html</title>
        <desc>Centralized encryption service using Azure Key Vault HSM with DEK/KEK envelope encryption pattern, plus the Tier 3 Bulk PoC (POST /dek/issue and /dek/unwrap on CORE SERVICE itself, paired with the separate hsm-bulk-client), dek_name reuse, and BULK File's ciphertext-format interoperability with CORE SERVICE's own /decrypt, guarded by CoreBulkFileInteropTest. Multiple client apps consult PlainID/PBAC (an external shared policy service) before ever calling the HSM service; the HSM service's own Auth Middleware independently validates the JWT, App-ID, grant, and scope on every call, and the Core Service may optionally also call PlainID for fine-grained PBAC. Azure KV Secrets (cek-alpha, cek-beta, current_key pointer) and Azure Key Vault Managed HSM (the KEK) are two distinct resources — Service SPN reads both; a separate Rotation SPN is the only identity that writes new CEK slot bytes and flips current_key, via its own CEK Rotation Svc (a separate K8s deployable, dashed border). The Redis DEK Cache uses versioned keys ({'{'}slot{'}'}:{'{'}kv_version{'}'}:{'{'}edek_id{'}'}) so cache hits skip the Managed HSM unwrap. The EDEK Store (schema hsm_crypto) and the Access Store (schema hsm_access — app_registrations, the coarse app_grants table, and the fine-grained per-dek_name app_dek_grants table) are two distinct PostgreSQL schemas. Auditor SPN sits entirely outside the Azure subscription boundary, reading Azure KV Secrets, the EDEK Store, and the Access Store directly with read-only access — it never routes through the Core Service. The Tier 3 Bulk PoC's /dek/issue and /dek/unwrap endpoints live on CORE SERVICE itself (merged from the formerly-separate hsm-bulk-service codebase) — helm/hsm-bulk-service now just deploys the identical CORE SERVICE image as a 2nd, independently-scaled release for bulk-traffic isolation, not a separate codebase. hsm-bulk-client is an external batch job (shared by hsm-spark-adapter via the same hsm-crypto-client library) that reuses one DEK per dek_name across many rows instead of minting a fresh one per row.</desc>

        <rect width="1320" height="1330" fill="#0f1117" />

        {/* ── AZURE SUBSCRIPTION BOUNDARY — everything below/left of this
            dashed rect is inside the HSM Service's own Azure subscription;
            Auditor SPN (far right) is deliberately outside it. ── */}
        <rect x="5" y="165" width="895" height="1155" rx="10" fill="none" stroke="#374151" strokeWidth="1.5" strokeDasharray="8,4" />
        <rect x="5" y="157" width="160" height="16" rx="3" fill="#0f1117" />
        <text x="12" y="169" fill="#4b5563" fontSize="9" fontFamily="monospace" letterSpacing="1">AZURE SUBSCRIPTION</text>

        {/* ── MULTIPLE CLIENTS ── */}
        <rect x="15" y="10" width="168" height="152" rx="8" fill="#1a1d27" stroke="#3b82f6" strokeWidth="1.5" />
        <text x="99" y="28" textAnchor="middle" fill="#8b92b8" fontSize="10" letterSpacing="1" fontFamily="monospace">MULTIPLE CLIENTS</text>
        <rect x="28" y="36" width="142" height="26" rx="5" fill="#22263a" stroke="#3b82f6" strokeWidth="1" />
        <text x="99" y="53" textAnchor="middle" fill="#3b82f6" fontFamily="monospace" fontSize="10">Client App 1 · SPN</text>
        <rect x="28" y="68" width="142" height="26" rx="5" fill="#22263a" stroke="#3b82f6" strokeWidth="1" />
        <text x="99" y="85" textAnchor="middle" fill="#3b82f6" fontFamily="monospace" fontSize="10">Client App 2 · SPN</text>
        <rect x="28" y="100" width="142" height="26" rx="5" fill="#22263a" stroke="#3b82f6" strokeWidth="1" />
        <text x="99" y="117" textAnchor="middle" fill="#3b82f6" fontFamily="monospace" fontSize="10">Client App N · SPN</text>
        <text x="99" y="140" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Bearer JWT + user context</text>
        <text x="99" y="153" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">user info for SIEM</text>

        {/* ── PlainID / PBAC — external shared policy service ── */}
        <rect x="200" y="10" width="180" height="152" rx="8" fill="#1a1d27" stroke="#eab308" strokeWidth="2" />
        <text x="290" y="27" textAnchor="middle" fill="#eab308" fontSize="10" letterSpacing="1" fontFamily="monospace">PlainID / PBAC</text>
        <text x="290" y="40" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">External shared policy service</text>
        <rect x="213" y="48" width="154" height="22" rx="4" fill="#22263a" />
        <text x="290" y="63" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Evaluate policy per identity</text>
        <rect x="213" y="76" width="70" height="22" rx="4" fill="#1e3a2f" stroke="#10b981" strokeWidth="1" />
        <text x="248" y="91" textAnchor="middle" fill="#10b981" fontSize="9" fontFamily="monospace">ALLOW</text>
        <rect x="289" y="76" width="70" height="22" rx="4" fill="#3a1a1a" stroke="#ef4444" strokeWidth="1" />
        <text x="324" y="91" textAnchor="middle" fill="#ef4444" fontSize="9" fontFamily="monospace">DENY</text>
        <text x="290" y="116" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">Clients call PlainID before</text>
        <text x="290" y="128" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">calling the HSM service</text>
        <text x="290" y="143" textAnchor="middle" fill="#eab308" fontSize="8" fontFamily="monospace">HSM service may also call</text>
        <text x="290" y="155" textAnchor="middle" fill="#eab308" fontSize="8" fontFamily="monospace">PlainID for fine-grained PBAC</text>

        <line x1="183" y1="83" x2="200" y2="83" stroke="#3b82f6" strokeWidth="1.5" markerEnd="url(#arr-blue)" />
        <path d="M 99,162 L 99,215 L 230,215" fill="none" stroke="#3b82f6" strokeWidth="1.5" strokeDasharray="4,3" markerEnd="url(#arr-blue)" />
        <text x="160" y="209" textAnchor="middle" fill="#3b82f6" fontSize="8" fontFamily="monospace">Bearer JWT + end_user_id · client calls after PBAC ALLOW</text>
        <line x1="380" y1="89" x2="438" y2="89" stroke="#ef4444" strokeWidth="1.5" strokeDasharray="4,3" markerEnd="url(#arr-red)" />
        <rect x="440" y="78" width="68" height="22" rx="4" fill="#3a1a1a" stroke="#ef4444" strokeWidth="1" />
        <text x="474" y="93" textAnchor="middle" fill="#ef4444" fontSize="9" fontFamily="monospace">BLOCKED</text>
        <path d="M 440,220 L 395,220 L 395,100 L 380,100" fill="none" stroke="#eab308" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-yellow)" />
        <text x="415" y="168" fill="#eab308" fontSize="7" fontFamily="monospace" transform="rotate(-90 415 168)">optional PBAC</text>

        {/* ── AUTH MIDDLEWARE ── */}
        <rect x="230" y="220" width="160" height="140" rx="8" fill="#1a1d27" stroke="#f59e0b" strokeWidth="1.5" />
        <text x="310" y="240" textAnchor="middle" fill="#f59e0b" fontSize="10" letterSpacing="1" fontFamily="monospace">AUTH MIDDLEWARE</text>
        <rect x="245" y="250" width="130" height="24" rx="4" fill="#22263a" />
        <text x="310" y="266" textAnchor="middle" fill="#cdd2f0" fontSize="10" fontFamily="monospace">JWT Validation</text>
        <rect x="245" y="280" width="130" height="24" rx="4" fill="#22263a" />
        <text x="310" y="296" textAnchor="middle" fill="#cdd2f0" fontSize="10" fontFamily="monospace">App-ID + Grant Check</text>
        <rect x="245" y="310" width="130" height="24" rx="4" fill="#22263a" />
        <text x="310" y="326" textAnchor="middle" fill="#cdd2f0" fontSize="10" fontFamily="monospace">Scope Enforcement</text>
        <rect x="245" y="338" width="130" height="16" rx="4" fill="#78350f" />
        <text x="310" y="350" textAnchor="middle" fill="#fbbf24" fontSize="9" fontFamily="monospace">Audit Log → SIEM</text>
        <line x1="390" y1="290" x2="440" y2="290" stroke="#f59e0b" strokeWidth="1.5" markerEnd="url(#arr-amber)" />

        {/* ── CORE SERVICE ── */}
        <rect x="440" y="180" width="200" height="300" rx="8" fill="#1a1d27" stroke="#a78bfa" strokeWidth="2" />
        <text x="540" y="202" textAnchor="middle" fill="#a78bfa" fontSize="10" letterSpacing="1" fontFamily="monospace">CORE SERVICE</text>
        <text x="540" y="216" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Spring Boot · /api/sensec/hsm/v1</text>
        <rect x="455" y="225" width="170" height="50" rx="5" fill="#22263a" stroke="#10b981" strokeWidth="1" />
        <text x="540" y="243" textAnchor="middle" fill="#10b981" fontSize="10" fontFamily="monospace">POST /encrypt · /batch</text>
        <text x="540" y="258" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Gen DEK → AES-256-GCM</text>
        <text x="540" y="270" textAnchor="middle" fill="#10b981" fontSize="9" fontFamily="monospace">or reuse via dek_name</text>
        <rect x="455" y="285" width="170" height="58" rx="5" fill="#22263a" stroke="#f87171" strokeWidth="1" />
        <text x="540" y="301" textAnchor="middle" fill="#f87171" fontSize="10" fontFamily="monospace">POST /decrypt · /batch</text>
        <text x="540" y="315" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">grant check → cache lookup</text>
        <text x="540" y="328" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">→ unwrap MISS → AES decrypt</text>
        <text x="540" y="339" textAnchor="middle" fill="#555b7a" fontSize="7" fontFamily="monospace">Redis: {'{'}slot{'}'}:{'{'}kv_ver{'}'}:{'{'}edek_id{'}'}</text>
        <rect x="455" y="353" width="170" height="38" rx="5" fill="#22263a" stroke="#fb923c" strokeWidth="1" />
        <text x="540" y="371" textAnchor="middle" fill="#fb923c" fontSize="10" fontFamily="monospace">/admin/rotate-kek · grants</text>
        <text x="540" y="384" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Re-wrap EDEKs · +dek-grants</text>
        <rect x="455" y="401" width="170" height="30" rx="5" fill="#22263a" stroke="#64748b" strokeWidth="1" />
        <text x="540" y="420" textAnchor="middle" fill="#94a3b8" fontSize="10" fontFamily="monospace">GET /health · /apps/status</text>
        <rect x="455" y="440" width="170" height="14" rx="3" fill="#0a1f1e" stroke="#14b8a6" strokeWidth="1" />
        <text x="540" y="451" textAnchor="middle" fill="#14b8a6" fontSize="7" fontFamily="monospace">poll current_key · 30s · Service SPN read</text>
        <rect x="455" y="460" width="170" height="14" rx="3" fill="#1e3a2f" stroke="#10b981" strokeWidth="1" />
        <text x="540" y="471" textAnchor="middle" fill="#10b981" fontSize="7" fontFamily="monospace">FIPS 140-2 Level 3 · AES-256-GCM</text>

        {/* ── AZURE KV SECRETS (regular vault.azure.net — Secrets API) ── */}
        <rect x="690" y="180" width="190" height="64" rx="4" fill="#0a1f1e" stroke="#14b8a6" strokeWidth="1.5" />
        <text x="785" y="197" textAnchor="middle" fill="#14b8a6" fontSize="9" letterSpacing="1" fontFamily="monospace">AZURE KV SECRETS</text>
        <text x="785" y="210" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">vault.azure.net · Secrets API</text>
        <text x="785" y="222" textAnchor="middle" fill="#14b8a6" fontSize="7" fontFamily="monospace">cek-alpha · cek-beta (CEK bytes)</text>
        <text x="785" y="234" textAnchor="middle" fill="#eab308" fontSize="7" fontFamily="monospace">current_key → "alpha" | "beta"</text>
        <text x="785" y="240" textAnchor="middle" fill="#555b7a" fontSize="6" fontFamily="monospace">Service SPN: read · Rotation SPN: write</text>
        <line x1="640" y1="210" x2="690" y2="210" stroke="#14b8a6" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-teal)" />
        <text x="665" y="205" textAnchor="middle" fill="#14b8a6" fontSize="7" fontFamily="monospace">read · 30s poll</text>

        <line x1="640" y1="299" x2="690" y2="299" stroke="#14b8a6" strokeWidth="1.5" markerEnd="url(#arr-teal)" />
        <text x="665" y="293" textAnchor="middle" fill="#14b8a6" fontSize="7" fontFamily="monospace">cache GET/SET</text>
        <path d="M 640,380 L 678,380 L 678,415 L 690,415" fill="none" stroke="#a78bfa" strokeWidth="1.5" markerEnd="url(#arr-purple)" />
        <text x="674" y="393" textAnchor="end" fill="#a78bfa" fontSize="8" fontFamily="monospace">Service SPN</text>
        <text x="674" y="404" textAnchor="end" fill="#a78bfa" fontSize="7" fontFamily="monospace">wrap/unwrap MISS</text>

        {/* ── REDIS DEK CACHE ── */}
        <rect x="690" y="250" width="190" height="120" rx="8" fill="#1a1d27" stroke="#14b8a6" strokeWidth="2" />
        <text x="785" y="270" textAnchor="middle" fill="#14b8a6" fontSize="10" letterSpacing="1" fontFamily="monospace">REDIS DEK CACHE</text>
        <text x="785" y="283" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Azure Cache for Redis · TLS</text>
        <rect x="705" y="290" width="160" height="22" rx="4" fill="#22263a" />
        <text x="785" y="305" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">key: {'{'}slot{'}'}:{'{'}kv_version{'}'}:{'{'}edek_id{'}'}</text>
        <rect x="705" y="318" width="160" height="22" rx="4" fill="#22263a" />
        <text x="785" y="333" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">value: CEK-encrypted DEK</text>
        <rect x="705" y="346" width="160" height="18" rx="3" fill="#1e3a2f" stroke="#14b8a6" strokeWidth="1" />
        <text x="785" y="359" textAnchor="middle" fill="#14b8a6" fontSize="7" fontFamily="monospace">TTL 60s · versioned · no flush needed</text>

        <path d="M 880,303 L 940,303 L 940,453 L 880,453" fill="none" stroke="#14b8a6" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-teal)" />
        <text x="951" y="378" fill="#14b8a6" fontSize="8" fontFamily="monospace" transform="rotate(-90 951 378)">cache MISS → unwrapKey</text>

        {/* ── AZURE KEY VAULT (Managed HSM) ── */}
        <rect x="690" y="376" width="190" height="200" rx="8" fill="#1a1d27" stroke="#e879f9" strokeWidth="2" />
        <text x="785" y="398" textAnchor="middle" fill="#e879f9" fontSize="10" letterSpacing="1" fontFamily="monospace">AZURE KEY VAULT</text>
        <text x="785" y="412" textAnchor="middle" fill="#e879f9" fontSize="9" fontFamily="monospace">Managed HSM · FIPS 140-2 L3</text>
        <rect x="705" y="422" width="160" height="30" rx="4" fill="#2d1b47" stroke="#e879f9" strokeWidth="1" />
        <text x="785" y="437" textAnchor="middle" fill="#e879f9" fontSize="10" fontFamily="monospace">KEK (RSA-HSM 4096)</text>
        <text x="785" y="449" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">never leaves HSM boundary</text>
        <rect x="705" y="461" width="160" height="24" rx="4" fill="#22263a" />
        <text x="785" y="477" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Key Versioning</text>
        <rect x="705" y="493" width="160" height="24" rx="4" fill="#22263a" />
        <text x="785" y="509" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Auto-rotation Policy</text>
        <rect x="705" y="525" width="160" height="24" rx="4" fill="#22263a" />
        <text x="785" y="541" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Role-based Access (RBAC)</text>
        <rect x="705" y="557" width="160" height="14" rx="3" fill="#22263a" />
        <text x="785" y="568" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">Managed Identity (no secrets)</text>

        {/* ── EDEK STORE ── */}
        <rect x="440" y="510" width="200" height="120" rx="8" fill="#1a1d27" stroke="#38bdf8" strokeWidth="1.5" />
        <text x="540" y="530" textAnchor="middle" fill="#38bdf8" fontSize="10" letterSpacing="1" fontFamily="monospace">EDEK STORE</text>
        <text x="540" y="543" textAnchor="middle" fill="#38bdf8" fontSize="7" fontFamily="monospace">schema: hsm_crypto · PostgreSQL</text>
        <rect x="455" y="550" width="170" height="22" rx="4" fill="#22263a" />
        <text x="540" y="565" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">edek_id · blob · owner app_id</text>
        <rect x="455" y="578" width="170" height="22" rx="4" fill="#22263a" />
        <text x="540" y="593" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">algorithm · encoding · class.</text>
        <rect x="455" y="606" width="170" height="14" rx="3" fill="#22263a" />
        <text x="540" y="617" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">encrypted at rest · TDE</text>
        <line x1="540" y1="470" x2="540" y2="510" stroke="#38bdf8" strokeWidth="1.5" markerEnd="url(#arr-cyan)" />

        {/* ── CEK ROTATION SERVICE — separate K8s deployable, own Rotation
            SPN; dashed border signals "not part of the Encryption Service
            or Grants + Rotation" the way it does everywhere else in this
            diagram. ── */}
        <rect x="440" y="650" width="200" height="116" rx="8" fill="#1a1d27" stroke="#eab308" strokeWidth="1.5" strokeDasharray="5,3" />
        <text x="540" y="669" textAnchor="middle" fill="#eab308" fontSize="10" letterSpacing="1" fontFamily="monospace">CEK ROTATION SVC</text>
        <text x="540" y="682" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">Separate K8S service · HSM Service SPN</text>
        <rect x="455" y="689" width="170" height="20" rx="4" fill="#22263a" stroke="#eab308" strokeWidth="1" />
        <text x="540" y="703" textAnchor="middle" fill="#eab308" fontSize="8" fontFamily="monospace">every 4h · immediate on recovery</text>
        <rect x="455" y="715" width="170" height="20" rx="4" fill="#22263a" />
        <text x="540" y="729" textAnchor="middle" fill="#cdd2f0" fontSize="8" fontFamily="monospace">1. gen new 32-byte CEK → write slot</text>
        <rect x="455" y="741" width="170" height="20" rx="4" fill="#22263a" />
        <text x="540" y="755" textAnchor="middle" fill="#cdd2f0" fontSize="8" fontFamily="monospace">2. update current_key pointer</text>

        {/* Rotation SPN write path — mirrors master's own route exactly:
            straight up the gap between ACCESS STORE (right edge x=880) and
            the Auditor panel (left edge x=960), clearing ACCESS STORE's
            taller bottom edge (y=702). */}
        <path d="M 640,700 L 640,750 L 888,750 L 888,212 L 880,212" fill="none" stroke="#eab308" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-yellow)" />
        <text x="764" y="746" textAnchor="middle" fill="#eab308" fontSize="7" fontFamily="monospace">write slot FIRST · then current_key</text>

        {/* ── ACCESS STORE (hsm_access schema) — replaces the earlier
            "Grants + Rotation" box; master now documents the actual
            underlying tables, split into coarse (app_grants) and
            fine-grained per-dek_name (app_dek_grants) scopes. ── */}
        <rect x="690" y="582" width="190" height="154" rx="8" fill="#1a1d27" stroke="#fb923c" strokeWidth="1.5" />
        <text x="785" y="600" textAnchor="middle" fill="#fb923c" fontSize="10" letterSpacing="1" fontFamily="monospace">ACCESS STORE</text>
        <text x="785" y="613" textAnchor="middle" fill="#fb923c" fontSize="7" fontFamily="monospace">schema: hsm_access · Managed Access team</text>
        <rect x="705" y="619" width="160" height="22" rx="4" fill="#22263a" />
        <text x="785" y="632" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">app_registrations</text>
        <text x="785" y="643" textAnchor="middle" fill="#555b7a" fontSize="7" fontFamily="monospace">app_id · allowed_scopes · active · ts</text>
        <rect x="705" y="651" width="160" height="22" rx="4" fill="#22263a" />
        <text x="785" y="664" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">app_grants</text>
        <text x="785" y="675" textAnchor="middle" fill="#555b7a" fontSize="7" fontFamily="monospace">coarse · grantee→owner·scope · ts</text>
        <rect x="705" y="683" width="160" height="22" rx="4" fill="#22263a" />
        <text x="785" y="696" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">app_dek_grants</text>
        <text x="785" y="707" textAnchor="middle" fill="#555b7a" fontSize="7" fontFamily="monospace">fine-grained · +dek_name · scope</text>
        <rect x="705" y="715" width="160" height="14" rx="3" fill="#22263a" />
        <text x="785" y="726" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">HSM SPN: grant check (both scopes)</text>

        {/* ── ENCRYPT / DECRYPT PAYLOAD FLOW ── */}
        <rect x="20" y="650" width="400" height="352" rx="8" fill="#1a1d27" stroke="#2d3148" strokeWidth="1.5" />
        <text x="220" y="670" textAnchor="middle" fill="#8b92b8" fontSize="10" letterSpacing="1" fontFamily="monospace">ENCRYPT / DECRYPT PAYLOAD FLOW</text>
        <text x="36" y="690" fill="#10b981" fontSize="10" fontFamily="monospace">Encrypt:</text>
        <rect x="36" y="698" width="365" height="60" rx="4" fill="#22263a" />
        <text x="50" y="712" fill="#555b7a" fontSize="9" fontFamily="monospace">Request:  {'{'} plaintext, encoding, data_classification, end_user_id {'}'}</text>
        <text x="50" y="726" fill="#10b981" fontSize="9" fontFamily="monospace">Generate:  DEK = random_bytes(32)  (or reuse via dek_name)</text>
        <text x="50" y="740" fill="#555b7a" fontSize="9" fontFamily="monospace">Cipher  =  AES-256-GCM(DEK, IV, plaintext, AAD=owner_app_id)</text>
        <text x="50" y="752" fill="#555b7a" fontSize="9" fontFamily="monospace">EDEK    =  KEK.wrap(DEK)  →  stored in EDEK Store</text>
        <text x="36" y="772" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Response: {'{'} ciphertext {'}'}  ← only field client needs to store</text>
        <text x="36" y="792" fill="#f87171" fontSize="10" fontFamily="monospace">Decrypt:</text>
        <rect x="36" y="800" width="365" height="54" rx="4" fill="#22263a" />
        <text x="50" y="814" fill="#555b7a" fontSize="9" fontFamily="monospace">Request:   {'{'} ciphertext, end_user_id {'}'}</text>
        <text x="50" y="828" fill="#555b7a" fontSize="9" fontFamily="monospace">grant check → Redis HIT (skip KV) / MISS → KV unwrap → AES-GCM</text>
        <text x="50" y="842" fill="#555b7a" fontSize="9" fontFamily="monospace">Response:  {'{'} plaintext {'}'}  ← only field client needs</text>

        <rect x="36" y="856" width="365" height="68" rx="4" fill="#0a1f1e" stroke="#14b8a6" strokeWidth="1" />
        <text x="50" y="870" fill="#14b8a6" fontSize="9" fontFamily="monospace">CEK Rotation (alpha/beta · no restart):</text>
        <text x="50" y="883" fill="#555b7a" fontSize="8" fontFamily="monospace">Rotation SVC: gen bytes → write inactive slot → update current_key</text>
        <text x="50" y="895" fill="#555b7a" fontSize="8" fontFamily="monospace">Pods poll 30s: detect slot or kv_version change → rotate(cek, slot, ver)</text>
        <text x="50" y="907" fill="#555b7a" fontSize="8" fontFamily="monospace">Redis key: {'{'}slot{'}'}:{'{'}kv_ver{'}'}:{'{'}edek_id{'}'} · prev slot readable via fallback</text>
        <text x="50" y="919" fill="#555b7a" fontSize="8" fontFamily="monospace">Old {'{'}slot{'}'}:{'{'}old_ver{'}'}:* entries expire via 60s TTL — no flush needed</text>

        <rect x="36" y="932" width="365" height="60" rx="4" fill="#1a0a2e" stroke="#a78bfa" strokeWidth="1" />
        <text x="50" y="946" fill="#a78bfa" fontSize="9" fontFamily="monospace">Batch (encrypt/decrypt/batch):</text>
        <text x="50" y="959" fill="#555b7a" fontSize="8" fontFamily="monospace">Same per-item crypto path, looped — no new EDEK-writing path</text>
        <text x="50" y="971" fill="#555b7a" fontSize="8" fontFamily="monospace">N items/call, keyed by caller's own id · cap = batch-max-items (100)</text>
        <text x="50" y="983" fill="#555b7a" fontSize="8" fontFamily="monospace">Always 200 · per-item status success/error (bad batch itself → 422)</text>

        {/* ── AUDITOR SPN — OUTSIDE the Azure subscription boundary ── */}
        <rect x="960" y="10" width="340" height="940" rx="10" fill="#100404" stroke="#f87171" strokeWidth="1" strokeDasharray="6,3" />
        <text x="1130" y="30" textAnchor="middle" fill="#f87171" fontSize="9" fontFamily="monospace" letterSpacing="1">OUTSIDE AZURE SUBSCRIPTION</text>

        <rect x="975" y="42" width="310" height="260" rx="8" fill="#1a1d27" stroke="#f87171" strokeWidth="1.5" />
        <text x="1130" y="62" textAnchor="middle" fill="#f87171" fontSize="10" letterSpacing="1" fontFamily="monospace">AUDITOR SPN</text>
        <text x="1130" y="76" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Audit / Scanning Service</text>
        <text x="1130" y="90" textAnchor="middle" fill="#f87171" fontSize="8" fontFamily="monospace">Not part of the encryption solution</text>
        <rect x="990" y="98" width="280" height="30" rx="4" fill="#22263a" stroke="#f87171" strokeWidth="1" />
        <text x="1130" y="114" textAnchor="middle" fill="#f87171" fontSize="9" fontFamily="monospace">Azure KV: list / get keys (read-only)</text>
        <text x="1130" y="128" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">no wrap · no unwrap RBAC</text>
        <rect x="990" y="136" width="280" height="24" rx="4" fill="#22263a" />
        <text x="1130" y="148" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">hsm_crypto: read edek_records</text>
        <text x="1130" y="160" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">hsm_access: read grants + registrations</text>
        <rect x="990" y="168" width="280" height="24" rx="4" fill="#22263a" />
        <text x="1130" y="184" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Bypasses HSM service entirely</text>
        <rect x="990" y="200" width="280" height="20" rx="3" fill="#3a1a1a" stroke="#ef4444" strokeWidth="1" />
        <text x="1130" y="214" textAnchor="middle" fill="#ef4444" fontSize="8" fontFamily="monospace">Never routes through /decrypt endpoint</text>
        <rect x="990" y="228" width="280" height="30" rx="3" fill="#22263a" />
        <text x="1130" y="244" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">SIEM / Splunk audited separately</text>
        <text x="1130" y="258" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">Redis cache NOT accessible</text>

        <line x1="975" y1="200" x2="900" y2="453" stroke="#f87171" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-red)" />
        <text x="948" y="320" fill="#f87171" fontSize="7" fontFamily="monospace" transform="rotate(-70 948 320)">read-only access across boundary</text>
        <line x1="975" y1="220" x2="642" y2="568" stroke="#f87171" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-red)" />
        <text x="820" y="375" textAnchor="middle" fill="#f87171" fontSize="8" fontFamily="monospace">read-only · hsm_crypto</text>
        <line x1="975" y1="240" x2="882" y2="568" stroke="#f87171" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-red)" />
        <text x="940" y="430" textAnchor="middle" fill="#f87171" fontSize="8" fontFamily="monospace">read-only · hsm_access</text>

        {/* ── TIER 3 BULK PoC (dek_name reuse) — added later round ──────── */}
        <rect x="15" y="1032" width="230" height="16" rx="3" fill="#0f1117" />
        <text x="22" y="1044" fill="#4b5563" fontSize="9" fontFamily="monospace" letterSpacing="1">TIER 3 BULK PoC · dek_name REUSE</text>

        {/* SVC: /dek/issue + /dek/unwrap are endpoints on CORE SERVICE itself (merged from the
            formerly-separate hsm-bulk-service codebase); helm/hsm-bulk-service now deploys the
            identical CORE SERVICE image as a 2nd, independently-scaled release for bulk-traffic isolation */}
        <rect x="440" y="1055" width="250" height="175" rx="8" fill="#1a1d27" stroke="#a78bfa" strokeWidth="2" />
        <text x="565" y="1075" textAnchor="middle" fill="#a78bfa" fontSize="10" letterSpacing="1" fontFamily="monospace">CORE SERVICE (2nd release)</text>
        <text x="565" y="1088" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">same image · Helm release "hsm-bulk-service"</text>
        <rect x="455" y="1096" width="220" height="22" rx="4" fill="#22263a" stroke="#a78bfa" strokeWidth="1" />
        <text x="565" y="1111" textAnchor="middle" fill="#a78bfa" fontSize="9" fontFamily="monospace">POST /dek/issue · /dek/unwrap</text>
        <rect x="455" y="1122" width="220" height="34" rx="4" fill="#2d1b47" stroke="#e879f9" strokeWidth="1" />
        <text x="565" y="1134" textAnchor="middle" fill="#e879f9" fontSize="8" fontFamily="monospace">not a separate codebase —</text>
        <text x="565" y="1146" textAnchor="middle" fill="#e879f9" fontSize="8" fontFamily="monospace">deployment-level isolation only</text>
        <rect x="455" y="1160" width="220" height="22" rx="4" fill="#22263a" />
        <text x="565" y="1175" textAnchor="middle" fill="#38bdf8" fontSize="8" fontFamily="monospace">writes SAME EDEK STORE above</text>
        <rect x="455" y="1186" width="220" height="38" rx="4" fill="#0a1f1e" stroke="#10b981" strokeWidth="1" />
        <text x="565" y="1198" textAnchor="middle" fill="#10b981" fontSize="8" fontFamily="monospace">dek_name: global ownership (1st</text>
        <text x="565" y="1210" textAnchor="middle" fill="#10b981" fontSize="8" fontFamily="monospace">encrypt wins) + grant check (V14)</text>
        <text x="565" y="1221" textAnchor="middle" fill="#555b7a" fontSize="7" fontFamily="monospace">age-rotated · default 30d</text>

        {/* Routed arrow: this release → EDEK STORE, via the empty corridor right of the subscription boundary */}
        <path d="M 690,1075 L 920,1075 L 920,590 L 640,590" fill="none" stroke="#38bdf8" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-cyan)" />
        <text x="931" y="833" fill="#38bdf8" fontSize="7" fontFamily="monospace" transform="rotate(-90 931 833)">same edek_records table · same process as /decrypt</text>

        {/* CLNT: external caller, outside the subscription (same convention as MULTIPLE CLIENTS above) */}
        <rect x="940" y="1055" width="340" height="175" rx="8" fill="#1a1d27" stroke="#3b82f6" strokeWidth="1.5" />
        <text x="1110" y="1075" textAnchor="middle" fill="#3b82f6" fontSize="10" letterSpacing="1" fontFamily="monospace">hsm-bulk-client (CLNT)</text>
        <text x="1110" y="1088" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">batch job on hsm-crypto-client — shared by hsm-spark-adapter</text>
        <rect x="955" y="1096" width="310" height="30" rx="4" fill="#22263a" stroke="#3b82f6" strokeWidth="1" />
        <text x="1110" y="1109" textAnchor="middle" fill="#3b82f6" fontSize="8" fontFamily="monospace">RSA-OAEP-256 unwrap → AES-256-GCM locally</text>
        <text x="1110" y="1121" textAnchor="middle" fill="#555b7a" fontSize="7" fontFamily="monospace">plaintext never leaves CLNT's host; real KEK never seen</text>
        <rect x="955" y="1130" width="310" height="30" rx="4" fill="#0a1f1e" stroke="#10b981" strokeWidth="1" />
        <text x="1110" y="1143" textAnchor="middle" fill="#10b981" fontSize="8" fontFamily="monospace">dek-name on a column: ONE /dek/issue for</text>
        <text x="1110" y="1155" textAnchor="middle" fill="#10b981" fontSize="8" fontFamily="monospace">the whole job run, not once per row</text>
        <rect x="955" y="1164" width="310" height="30" rx="4" fill="#22263a" />
        <text x="1110" y="1177" textAnchor="middle" fill="#94a3b8" fontSize="8" fontFamily="monospace">TokenProvider: static, Azure AD, self-signed JWT, or mTLS</text>
        <text x="1110" y="1189" textAnchor="middle" fill="#94a3b8" fontSize="7" fontFamily="monospace">Workload Identity, or a local keypair — renewal never leaves CLNT</text>
        <text x="1110" y="1214" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">ciphertext format unchanged —</text>
        <text x="1110" y="1225" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">CORE SERVICE's real /decrypt reads it as-is</text>

        {/* Arrow: CLNT → SVC, crossing the subscription boundary */}
        <line x1="940" y1="1115" x2="690" y2="1115" stroke="#3b82f6" strokeWidth="1.5" markerEnd="url(#arr-blue)" />
        <text x="815" y="1109" textAnchor="middle" fill="#3b82f6" fontSize="7" fontFamily="monospace">Bearer (3 modes) or mTLS client cert + X-App-ID</text>

        {/* File-format interop note — added later round: BULK File job specifically, DB columns already covered by the note above */}
        <rect x="440" y="1240" width="840" height="75" rx="6" fill="#0a1f1e" stroke="#10b981" strokeWidth="1" />
        <text x="860" y="1256" textAnchor="middle" fill="#10b981" fontSize="8" fontFamily="monospace">BULK File job: each chunk's plaintext base64-encoded before AES-256-GCM — survives CORE SERVICE's own /decrypt UTF-8 response encoding losslessly</text>
        <text x="860" y="1269" textAnchor="middle" fill="#10b981" fontSize="8" fontFamily="monospace">FileBulkJob.reconstructCoreServiceToken(edek_id, iv, tag, ciphertext) — same token format CORE SERVICE itself produces, now true for files too</text>
        <text x="860" y="1282" textAnchor="middle" fill="#10b981" fontSize="8" fontFamily="monospace">Optional compress-before-encrypt: gzip + 1-byte marker inside the AEAD payload, before base64 — decrypt always reads it, no config to coordinate</text>
        <text x="860" y="1295" textAnchor="middle" fill="#555b7a" fontSize="7" fontFamily="monospace">CoreBulkFileInteropTest (hsm-bulk-client) — real CORE SERVICE process, both endpoint shapes + compression, every build</text>

        <defs>
          <marker id="arr-blue" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#3b82f6" />
          </marker>
          <marker id="arr-amber" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#f59e0b" />
          </marker>
          <marker id="arr-yellow" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#eab308" />
          </marker>
          <marker id="arr-purple" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#a78bfa" />
          </marker>
          <marker id="arr-cyan" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#38bdf8" />
          </marker>
          <marker id="arr-orange" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#fb923c" />
          </marker>
          <marker id="arr-green" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#10b981" />
          </marker>
          <marker id="arr-red" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#f87171" />
          </marker>
          <marker id="arr-teal" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#14b8a6" />
          </marker>
        </defs>
      </svg>
    </div>
  );
}

// ── Flows tab — numbered sequence across the 4 request flows ──────────────────
// Plain text/numbered lists by design (not diagram SVGs) — meant to be
// copy-pasted directly into a design doc or handed to any team.
//
// JWTs are APP-LEVEL ONLY, everywhere — issued to the calling App-ID, never
// to individual end-users (minting per-user tokens isn't viable at
// end-user scale — thousands of end-users, one app credential). Every
// end-user operation instead carries an explicit end_user_id field in the
// request itself. Both PlainID's PBAC decision and the Encryption Service's
// own authorization/audit consume that same field — never derived from the
// JWT, which only ever proves the App-ID.
//
// PlainID/PBAC is now a SHARED decision point, consulted INDEPENDENTLY by
// both the Client and the Encryption Service — two separate PDP calls
// against the same policy engine, not one call proxied or forwarded to the
// other. The Client's pre-check (below) can abort before the Service is
// ever called; the Service's own check (in Encrypt/Decrypt) is a second,
// unrelated decision using the Service's own identity — the Service never
// sees the Client's Permit/Deny, and PlainID never forwards one caller's
// decision to the other.
//
// JWTs are APP-LEVEL ONLY, everywhere — issued to the calling App-ID, never
// to individual end-users (minting per-user tokens isn't viable at
// end-user scale — thousands of end-users, one app credential). Every
// end-user operation instead carries an explicit end_user_id field in the
// request itself, consumed independently by the Client's PlainID check,
// the Service's own PlainID check, and the Service's audit_log.
//
// The HSM Service and Auditor SPN live in SEPARATE Azure subscriptions
// (see Architecture Diagram). Auditor never calls the Encryption Service —
// its reads cross the subscription boundary via read-only RBAC directly
// against Managed HSM and EDEK Store.
//
// Azure Key Vault (holds only the CEK) and Managed HSM (holds only the KEK)
// are two distinct resources, not one combined vault.
// Replicated from hsm_project/app/static/index.html's own Sequence Diagram
// (treated as the master copy, per the 2026-07-20 ADR-014 amendment) — same
// 9 participants (Clients, PlainID/PBAC, HSM Service, Azure Managed HSM,
// Azure KV Secrets, EDEK Store, Redis Cache, CEK Rotation Svc, Auditor SPN)
// and the same 6 sections (Startup, Policy Check, Encrypt, Decrypt, CEK
// Rotation, Audit/Scan), including its step numbering scheme (0a/0b/0c,
// 2a/2b, 15a/16a, R1-R6) rather than a purely sequential 1..N count.
const FLOWS = [
  {
    title: '0. Startup (at service init — not per request)',
    color: '#14b8a6',
    steps: [
      'HSM Service fetches current_key — a plain pointer to "alpha" or "beta" — then fetches cek-alpha and its own separately-returned kv_version from Azure KV Secrets, using its Service SPN.',
      'Azure KV Secrets returns the CEK-alpha bytes plus their kv_version, plus the previous slot\'s value if the pod already has one cached (beta, if this isn\'t the first startup) — the pod\'s DEKCache initializes with this, identically across every pod.',
      'The Service starts a background task (cek_reload_loop) that polls current_key every 30s and calls rotate(cek, slot, kv_version) whenever it changes; see the CEK Rotation Flow below for what drives that change.',
    ],
    actors: [
      { id: 'service', label: 'HSM Service' },
      { id: 'kvsecrets', label: 'Azure KV Secrets' },
    ],
    messages: [
      { from: 'service', to: 'kvsecrets', label: 'fetch current_key → "alpha" · fetch cek-alpha → (CEK bytes, kv_version)', stepNum: '0a' },
      { from: 'kvsecrets', to: 'service', dashed: true, label: 'CEK-alpha bytes + kv_version + prev slot (beta, if cached) → DEKCache init', stepNum: '0b' },
      { from: 'service', to: 'service', self: true, label: 'cek_reload_loop: poll current_key every 30s → rotate(cek, slot, kv_ver) on change', stepNum: '0c' },
    ],
  },
  {
    title: '1. Policy Check',
    color: '#eab308',
    steps: [
      'Client sends a policy check to PlainID — its own JWT plus the logged-in end-user\'s identity.',
      'If DENY: the call is blocked — it never reaches the HSM service.',
      'If ALLOW: PlainID enriches the JWT with an AD group claim, and the caller proceeds to the HSM service. PlainID\'s decision stays entirely client-side — the HSM service only ever sees the Bearer JWT (Entra ID, a caller-signed self-issued JWT, or a client cert via mTLS — see AUTHORIZATION.md), never PlainID\'s verdict.',
    ],
    actors: [
      { id: 'client', label: 'Client' },
      { id: 'plainid', label: 'PlainID' },
    ],
    messages: [
      { from: 'client', to: 'plainid', label: 'policy check (JWT + logged-in user identity)', stepNum: 1 },
      { from: 'plainid', to: 'client', dashed: true, variant: 'deny', label: '[DENY] — call blocked, never reaches HSM service', stepNum: '2a' },
      { from: 'plainid', to: 'client', variant: 'allow', label: '[ALLOW] — JWT enriched with AD group claim — caller proceeds to HSM service', stepNum: '2b' },
    ],
  },
  {
    title: '2. Encrypt Flow',
    color: '#10b981',
    steps: [
      'Client calls POST /encrypt directly, presenting { plaintext, encoding, data_classification, end_user_id } — plus an optional dek_name (see panel 2 of the Live Demo tab).',
      'HSM Service validates the JWT, the app_id, and the encrypt scope.',
      'If dek_name was set and a current row already exists for it: when the caller is the same app that owns it, or holds a grant (scope=encrypt), REUSE — unwrap the existing edek_blob (DekCache hit, else a KEK unwrap) and skip straight to the response; no new wrap, no new INSERT.',
      'If a row exists for a different app and the caller has no grant, the call is rejected with 403. If no row exists yet for that name, MINT — continue below exactly as a plain /encrypt call would (the first caller to encrypt under a name becomes its owner).',
      'HSM Service calls Azure Managed HSM to wrap the DEK, using its Service SPN (RSA-OAEP-256).',
      'Azure Managed HSM returns the EDEK (wrapped DEK).',
      'HSM Service generates the DEK + IV and AES-256-GCM encrypts the plaintext.',
      'HSM Service inserts an edek_record (edek_id, blob, owner_app_id, algorithm, …) into the EDEK Store.',
      'EDEK Store returns the new edek_id (UUID) — the REUSE branch above rejoins here with the same edek_id and no INSERT.',
      'HSM Service writes an audit_log entry to Splunk/SIEM (app_id, end_user_id, edek_id, status).',
      'HSM Service returns a single opaque { ciphertext } to the client — the only field the caller needs to store; it bundles the edek_id, IV, ciphertext, and tag internally.',
    ],
    actors: [
      { id: 'client', label: 'Client' },
      { id: 'service', label: 'HSM Service' },
      { id: 'hsm', label: 'Azure Managed HSM' },
      { id: 'edek', label: 'EDEK Store' },
    ],
    messages: [
      { from: 'client', to: 'service', label: 'POST /encrypt { plaintext, encoding, data_classification, end_user_id }', stepNum: 3 },
      { from: 'service', to: 'service', self: true, label: 'validate JWT · app_id · scope=encrypt', stepNum: 4 },
      { from: 'service', to: 'service', self: true, variant: 'allow', label: '[found: same app, or granted (scope=encrypt)] REUSE: unwrap existing edek_blob (DekCache hit, else KEK unwrap)', stepNum: '4a' },
      { from: 'service', to: 'service', self: true, label: '[found, different app, NOT granted] 403 · [not found] MINT: gen fresh DEK, wrap via KEK, INSERT edek_record', stepNum: '4b' },
      { from: 'service', to: 'hsm', label: 'wrap DEK [Service SPN · RSA-OAEP-256]', stepNum: 5 },
      { from: 'hsm', to: 'service', dashed: true, label: 'EDEK (wrapped DEK)', stepNum: 6 },
      { from: 'service', to: 'service', self: true, label: 'gen DEK + IV · AES-256-GCM encrypt plaintext', stepNum: 7 },
      { from: 'service', to: 'edek', label: 'INSERT edek_record (edek_id, blob, owner_app_id, algorithm…)', stepNum: 8 },
      { from: 'edek', to: 'service', dashed: true, label: 'edek_id (UUID) — [REUSE rejoins here — same edek_id, no INSERT]', stepNum: 9 },
      { from: 'service', to: 'service', self: true, label: 'audit_log → Splunk/SIEM (app_id, end_user_id, edek_id, status)', stepNum: 10 },
      { from: 'service', to: 'client', dashed: true, label: '{ ciphertext } — only field client stores', stepNum: 11 },
    ],
  },
  {
    title: '3. Decrypt Flow',
    color: '#f87171',
    steps: [
      'Client calls POST /decrypt directly, presenting { ciphertext, end_user_id } — the single opaque token from /encrypt, passed back as-is and never decoded client-side.',
      'HSM Service looks up the edek_record by edek_id (extracted server-side from the token) in the EDEK Store.',
      'EDEK Store returns { edek_blob, kek_version, owner_app_id, data_class }.',
      'HSM Service asks the Access Store (schema hsm_access): if the caller is the same app as the owner, permit outright; otherwise check the coarse app_grants table, then fall back to the fine-grained per-dek_name app_dek_grants table (scope=decrypt).',
      'Access Store returns granted or denied.',
      'HSM Service checks Redis for the current slot\'s cached DEK first, falling back to the previous slot: GET {slot}:{kv_ver}:{edek_id}.',
      'HIT: the cached DEK bytes are used directly — the unwrap and re-cache steps below are skipped entirely, jumping straight to the final decrypt step. MISS: continue to the Managed HSM unwrap below.',
      '[MISS only] HSM Service calls Azure Managed HSM to unwrap the DEK, using its Service SPN (RSA-OAEP-256).',
      '[MISS only] Azure Managed HSM returns the raw DEK bytes.',
      '[MISS only] HSM Service writes the DEK back into Redis, CEK-encrypted, with a 60s TTL: SET {slot}:{kv_ver}:{edek_id}.',
      'HSM Service AES-256-GCM decrypts the ciphertext and zeroes the DEK immediately.',
      'HSM Service writes an audit_log entry to Splunk/SIEM (app_id, end_user_id, edek_id, status).',
      'HSM Service returns { plaintext } to the client — the only field the caller needs; the DEK is zeroed in memory.',
    ],
    actors: [
      { id: 'client', label: 'Client' },
      { id: 'service', label: 'HSM Service' },
      { id: 'edek', label: 'EDEK Store' },
      { id: 'accessstore', label: 'Access Store' },
      { id: 'redis', label: 'Redis Cache' },
      { id: 'hsm', label: 'Azure Managed HSM' },
    ],
    messages: [
      { from: 'client', to: 'service', label: 'POST /decrypt { ciphertext, end_user_id }', stepNum: 12 },
      { from: 'service', to: 'edek', label: 'SELECT edek_record WHERE id = edek_id', stepNum: 13 },
      { from: 'edek', to: 'service', dashed: true, label: '{ edek_blob, kek_version, owner_app_id, data_class }', stepNum: 14 },
      { from: 'service', to: 'accessstore', label: 'grant check: same app → coarse app_grants → fine-grained app_dek_grants [scope=decrypt]', stepNum: 15 },
      { from: 'accessstore', to: 'service', dashed: true, label: 'granted / denied', stepNum: 15 },
      { from: 'service', to: 'redis', label: 'GET {slot}:{kv_ver}:{edek_id} (current slot first, prev slot fallback)', stepNum: '15a' },
      { from: 'redis', to: 'service', dashed: true, label: 'HIT → cached DEK bytes (skip 16 & 17) / MISS → nil → unwrap below', stepNum: '15a' },
      { from: 'service', to: 'hsm', label: '[MISS only] unwrap DEK [Service SPN · RSA-OAEP-256]', stepNum: 16 },
      { from: 'hsm', to: 'service', dashed: true, label: '[MISS] raw DEK bytes', stepNum: 17 },
      { from: 'service', to: 'redis', label: '[MISS] SET {slot}:{kv_ver}:{edek_id} CEK-encrypted EX 60s', stepNum: '16a' },
      { from: 'service', to: 'service', self: true, label: 'AES-256-GCM decrypt · zero DEK immediately', stepNum: 18 },
      { from: 'service', to: 'service', self: true, label: 'audit_log → Splunk/SIEM (app_id, end_user_id, edek_id, status)', stepNum: 19 },
      { from: 'service', to: 'client', dashed: true, label: '{ plaintext } — only field client needs', stepNum: 20 },
    ],
  },
  {
    title: '4. CEK Rotation Flow (every 4h or immediately on recovery — no pod restart)',
    color: '#eab308',
    steps: [
      'CEK Rotation Svc generates a new 32-byte CEK and writes it to whichever slot is currently inactive — only alpha and beta ever exist, so this is always "the other one" from current_key (shown here as alpha active → writes cek-beta) — using its Rotation SPN, write-only on KV Secrets.',
      'Azure KV Secrets returns the new kv_version for that slot.',
      'CEK Rotation Svc updates current_key to point at the newly-written slot — "beta" in this example — only after the slot bytes are already written, never before.',
      'HSM Service\'s 30s poll (Service SPN, read-only) detects current_key now points at "beta" along with its new kv_version.',
      'HSM Service fetches that slot\'s bytes plus its kv_version from Azure KV Secrets.',
      'HSM Service calls rotate(new_cek, slot, kv_version) — this promotes the previously-active slot to "previous" and installs the newly-written slot as current. Every rotation flips current_key to whichever of alpha/beta was NOT already active, so the next rotation after this one flips right back — alpha→beta→alpha→beta, alternating indefinitely, never a third slot. New cache MISS entries are written under the new slot\'s key; old-slot entries simply expire via their 60s TTL — dual-read covers the ~30s convergence window while pods catch up. If the Rotation Svc itself is down at the 4h mark, pods hold their current CEK indefinitely with no errors, and rotation resumes immediately once it recovers.',
    ],
    actors: [
      { id: 'cekrotationsvc', label: 'CEK Rotation Svc' },
      { id: 'kvsecrets', label: 'Azure KV Secrets' },
      { id: 'service', label: 'HSM Service' },
    ],
    messages: [
      { from: 'cekrotationsvc', to: 'kvsecrets', label: 'gen new 32-byte CEK · write to the inactive slot — alpha or beta, whichever isn\'t current [Rotation SPN]', stepNum: 'R1' },
      { from: 'kvsecrets', to: 'cekrotationsvc', dashed: true, label: 'returns new kv_version for that slot', stepNum: 'R2' },
      { from: 'cekrotationsvc', to: 'kvsecrets', label: 'update current_key → "beta" [Rotation SPN · AFTER slot bytes written]', stepNum: 'R3' },
      { from: 'service', to: 'kvsecrets', dashed: true, label: 'poll detects current_key="beta" + new kv_version [Service SPN · 30s]', stepNum: 'R4' },
      { from: 'kvsecrets', to: 'service', dashed: true, label: 'fetch cek-beta bytes + kv_version', stepNum: 'R5' },
      { from: 'service', to: 'service', self: true, label: 'rotate(new_cek, slot, kv_version) → flips current_key to the other of alpha/beta each time — alternates indefinitely, never a third slot', stepNum: 'R6' },
    ],
  },
  {
    title: '5. Audit / Scan Flow (Auditor SPN — bypasses HSM service entirely)',
    color: '#f87171',
    steps: [
      'Auditor SPN reads the secrets cek-alpha and cek-beta directly from Azure KV Secrets — read-only, no Managed HSM access, crossing the subscription boundary.',
      'Azure KV Secrets returns secret metadata (kv_version, content_type, attributes) — never key bytes to any wrap/unwrap-capable identity, since the Auditor SPN has no Managed HSM RBAC at all.',
      'Auditor SPN runs a read-only DB scan directly against both schemas — hsm_crypto (EDEK Store) and hsm_access (Access Store) — again bypassing the HSM service entirely.',
      'The stores return edek records (wrapped blobs only) plus grants and app registrations. The Auditor SPN has no write access to any DB table, cannot reach the Redis cache, and never routes through the /decrypt endpoint; its own audit trail (Azure KV Diagnostic Logs + Splunk) is a separate pipeline from the HSM service\'s own audit_log.',
    ],
    actors: [
      { id: 'auditor', label: 'Auditor SPN' },
      { id: 'kvsecrets', label: 'Azure KV Secrets' },
      { id: 'edek', label: 'EDEK Store' },
    ],
    messages: [
      { from: 'auditor', to: 'kvsecrets', dashed: true, label: 'get secrets: cek-alpha, cek-beta [read-only · crosses subscription boundary]', stepNum: 21 },
      { from: 'kvsecrets', to: 'auditor', dashed: true, label: 'secret metadata (kv_version, content_type, attributes)', stepNum: 22 },
      { from: 'auditor', to: 'edek', dashed: true, label: 'SELECT * FROM hsm_crypto + hsm_access [read-only · outside subscription]', stepNum: 23 },
      { from: 'edek', to: 'auditor', dashed: true, label: 'edek records (wrapped blobs only)', stepNum: 24 },
    ],
  },
  {
    title: '6. Tier 3: dek_name Reuse (hsm-bulk-client → HSM Service\'s own /dek/issue · /dek/unwrap — not a separate codebase)',
    color: '#a78bfa',
    steps: [
      'hsm-bulk-client (a standalone batch job on hsm-crypto-client, outside the subscription) calls HSM Service\'s own POST /dek/issue once per job run — not once per row — with { key, data_classification, name: dek_name }, authenticating via Bearer (static, Workload Identity, or self-signed JWT) or mTLS client cert.',
      'The SVC lane here is the SAME HSM Service process as the rest of this diagram (formerly a separate hsm-bulk-service codebase, since merged) — kept as its own lane only because CLNT still makes a real network hop to reach it, same as any other caller.',
      'HSM Service looks up by current_dek_name alone — ownership is now GLOBAL (V14; previously scoped per-app). If FOUND and the caller is the same app that owns it, or holds a grant, it reuses the existing edek_blob (unwrapped via HSM Service\'s own Workload Identity against Managed HSM) — the same edek_id is returned on every call for this name, with no new mint and no new INSERT. If FOUND under a different app with no grant, the call is rejected with 403 — the caller needs an encrypt grant via /admin/grants or /dek-grants. If NOT FOUND, HSM Service mints a fresh DEK, KEK-wraps it, and INSERTs into edek_records (tagged with dek_name) — this app becomes its owner.',
      'HSM Service returns { edek_id, wrapped_dek_b64, reused: true|false }, wrapped for the client\'s own public key (RSA-OAEP-256) — never a raw DEK over the wire.',
      'hsm-bulk-client unwraps the DEK locally (its private key never leaves the client) and AES-256-GCM encrypts each row with a fresh IV per call.',
      'One edek_id is reused across many rows/values in the job, each still getting its own token (fresh IV every call) — the resulting ciphertext format is identical to HSM Service\'s own, so its real /decrypt endpoint reads it back with zero awareness this alternate path exists.',
      'The ciphertext is written directly into the client\'s own target table/file — never sent back to HSM Service. A NamedDekRotationScheduler (age-based, default 30d) periodically retires "found" rows above; the next lookup after that falls back to 26b. One scheduler instance covers rows from BOTH /encrypt and /dek/issue (same process, same edek_records table) — no separate SVC scheduler needed since the merge.',
      'BULK File job specifically: each chunk\'s plaintext is base64-encoded before AES-256-GCM — survives HSM Service\'s own /decrypt UTF-8 response encoding losslessly. FileBulkJob.reconstructCoreServiceToken(edek_id, iv, tag, ciphertext) produces the same token format HSM Service itself produces, now true for files too, not just DB columns. An optional compress-before-encrypt step gzips each chunk and prepends a 1-byte raw/gzip marker inside the base64 — decrypt always reads it, no config to coordinate. CoreBulkFileInteropTest exercises both directions plus compression against a real hsm-core-service process on every build.',
      'hsm-spark-adapter shares this exact flow via hsm-crypto-client\'s SvcClient/DekManager — the same /dek/issue, /dek/unwrap, and RSA-OAEP-256→AES-256-GCM steps above, just driven from Spark UDFs (hsm_encrypt/hsm_decrypt) instead of a batch job.',
    ],
    actors: [
      { id: 'clnt', label: 'hsm-bulk-client' },
      { id: 'svc',  label: 'HSM Service (SVC endpoints)' },
    ],
    messages: [
      { from: 'clnt', to: 'svc', label: 'POST /dek/issue { key, data_classification, name: dek_name } [Bearer: static, Workload Identity, or self-signed JWT]', stepNum: 25 },
      { from: 'svc', to: 'svc', self: true, label: 'lookup by current_dek_name (GLOBAL — V14, was per-app)', stepNum: 26 },
      { from: 'svc', to: 'svc', self: true, variant: 'allow', label: '[FOUND, same app or granted] reuse: unwrap edek_blob [Workload Identity · Managed HSM] — skip mint, skip INSERT — same edek_id every call', stepNum: '26a' },
      { from: 'svc', to: 'svc', self: true, variant: 'deny', label: '[FOUND, different app, NOT granted] 403 — request an encrypt grant via /admin/grants or /dek-grants', stepNum: '26b' },
      { from: 'svc', to: 'svc', self: true, label: '[NOT FOUND] mint fresh DEK · KEK-wrap · INSERT edek_records (tagged dek_name) — this app becomes its owner', stepNum: '26c' },
      { from: 'svc', to: 'clnt', dashed: true, label: '{ edek_id, wrapped_dek_b64, reused: true|false } [RSA-OAEP-256, wrapped for CLNT\'s own public key]', stepNum: 27 },
      { from: 'clnt', to: 'clnt', self: true, label: 'RSA-OAEP-256 unwrap locally (private key never leaves CLNT) → AES-256-GCM encrypt (fresh IV every call)', stepNum: 28 },
      { from: 'clnt', to: 'clnt', self: true, label: 'one edek_id → many rows/values, each with its OWN token (fresh IV per call) — token format identical to HSM Service\'s own', stepNum: 29 },
      { from: 'clnt', to: 'clnt', self: true, label: 'BULK File job: base64-encode each chunk before AES-256-GCM · FileBulkJob.reconstructCoreServiceToken(edek_id, iv, tag, ciphertext) · optional gzip + 1-byte marker before base64', stepNum: 30 },
      { from: 'clnt', to: 'clnt', self: true, label: 'hsm-spark-adapter shares this flow via hsm-crypto-client\'s SvcClient/DekManager — same /dek/issue, /dek/unwrap, RSA-OAEP-256→AES-256-GCM, driven from Spark UDFs instead of a batch job', stepNum: 31 },
    ],
  },
];

// ── Mermaid rendering path (see USE_MERMAID_FLOWS above) ────────────────────
// mermaid is lazy-loaded (dynamic import) so its ~500KB+ isn't part of any
// bundle that doesn't visit this page, mirroring PatternTemplates.jsx's own
// loadMermaid(). Kept as a separate module-level singleton here rather than
// sharing one across pages — this page's diagrams are always drawn on a
// fixed dark canvas (#0f1117) regardless of the app's light/dark toggle, so
// its Mermaid init call needs its own fixed-dark theme rather than
// PatternTemplates' light/dark-aware one.
let mermaidModulePromise = null;
function loadMermaid() {
  if (!mermaidModulePromise) {
    mermaidModulePromise = import('mermaid').then((mod) => {
      const mermaid = mod.default;
      mermaid.initialize({ startOnLoad: false, securityLevel: 'strict' });
      return mermaid;
    });
  }
  return mermaidModulePromise;
}

let mermaidRenderCounter = 0;

// Mermaid message text runs to end-of-line after the first colon following
// the arrow, so embedded colons/braces/arrows in our labels are already
// safe verbatim — with one exception: ';' is Mermaid's own statement
// separator (an alternative to a newline between diagram statements), so a
// literal semicolon mid-label truncates the message there and throws the
// rest of the line at the parser as a new (invalid) statement. Swapped for
// '·' — matches this file's own existing compact-separator convention
// elsewhere (e.g. "FIPS 140-2 L3 · AES-256-GCM").
function sanitizeForMermaid(text) {
  return text.replace(/;/g, ' ·');
}

function buildFlowMermaidText(flow) {
  const lines = [
    `%%{init: {'theme':'base', 'themeVariables': {'background':'#0f1117','actorBkg':'#22263a','actorBorder':'${flow.color}','actorTextColor':'${flow.color}','actorLineColor':'#2d3148','signalColor':'${flow.color}','signalTextColor':'#cdd2f0','labelBoxBkgColor':'#22263a','labelBoxBorderColor':'${flow.color}','labelTextColor':'${flow.color}','noteBkgColor':'#22263a','noteBorderColor':'${flow.color}','noteTextColor':'#cdd2f0'}}}%%`,
    'sequenceDiagram',
  ];
  flow.actors.forEach((a) => lines.push(`  participant ${a.id} as ${a.label}`));
  flow.messages.forEach((m) => {
    // Response arrows (dashed:true) render dashed per the master source's
    // own request-solid/response-dashed convention. The 'deny'/'allow'
    // variant used by the hand-SVG renderer to color individual arrows red/
    // green has no per-message equivalent in Mermaid sequence diagrams —
    // the [DENY]/[ALLOW] text markers already in the label carry that
    // meaning instead. See the ADR amendment for this tradeoff.
    const arrow = m.dashed ? '-->>' : '->>';
    lines.push(`  ${m.from}${arrow}${m.to}: ${sanitizeForMermaid(`${m.stepNum}. ${m.label}`)}`);
  });
  return lines.join('\n');
}

function buildOverviewMermaidText(actors, mainRows, rotationRows, auditRows) {
  const first = actors[0].id;
  const last = actors[actors.length - 1].id;
  const lines = [
    `%%{init: {'theme':'base', 'themeVariables': {'background':'#0f1117','actorBkg':'#22263a','actorBorder':'#8b92b8','actorTextColor':'#cdd2f0','actorLineColor':'#2d3148','signalColor':'#8b92b8','signalTextColor':'#cdd2f0','noteBkgColor':'#1a1d27','noteBorderColor':'#555b7a','noteTextColor':'#78716c'}}}%%`,
    'sequenceDiagram',
  ];
  actors.forEach((a) => lines.push(`  participant ${a.id} as ${a.label}`));
  mainRows.forEach((m) => lines.push(`  ${m.from}${m.dashed ? '-->>' : '->>'}${m.to}: ${sanitizeForMermaid(m.label)}`));
  lines.push(`  Note over ${first},${last}: — CEK rotation: independent, every 4h — no path through HSM Service's request handling —`);
  rotationRows.forEach((m) => lines.push(`  ${m.from}${m.dashed ? '-->>' : '->>'}${m.to}: ${sanitizeForMermaid(m.label)}`));
  lines.push(`  Note over ${first},${last}: — audit/scan: outside the Azure subscription — no path through HSM Service —`);
  auditRows.forEach((m) => lines.push(`  ${m.from}-->>${m.to}: ${sanitizeForMermaid(m.label)}`));
  return lines.join('\n');
}

function MermaidSequence({ text }) {
  const [svg, setSvg] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;
    loadMermaid()
      .then((mermaid) => mermaid.render(`hsm-flow-${++mermaidRenderCounter}`, text))
      .then(({ svg: rendered }) => {
        if (!cancelled) { setSvg(rendered); setError(''); }
      })
      .catch((err) => {
        if (!cancelled) { setSvg(''); setError(err.message || 'Could not render this diagram.'); }
      });
    return () => { cancelled = true; };
  }, [text]);

  if (error) return <p style={s.flowStep}>{error}</p>;
  // Mermaid's own SVG output — not user-supplied, generated from this
  // file's own FLOWS data, same trust boundary as PatternTemplates.jsx's
  // dangerouslySetInnerHTML usage for its (admin-authored) diagrams.
  return <div style={s.mermaidWrap} dangerouslySetInnerHTML={{ __html: svg }} />;
}

// Generic sequence-diagram renderer — actors as lifelines, messages drawn
// top-to-bottom in order. Self-messages (self:true) render as a small loop
// back onto the same lifeline (internal processing, no other actor involved).
// Dashed messages (dashed:true) use the shared red marker regardless of the
// flow's own color, to visually flag deny/bypass paths consistently.
// Kept in place, unmodified, as the USE_MERMAID_FLOWS rollback target.
function SequenceDiagram({ actors, messages, color, markerId }) {
  const width = 860;
  const laneGap = width / (actors.length + 1);
  const xFor = (i) => laneGap * (i + 1);
  const topY = 34;
  const rowH = 38;
  const bottomY = topY + messages.length * rowH + 16;

  return (
    <svg viewBox={`0 0 ${width} ${bottomY + 12}`} style={s.seqSvg} role="img">
      <title>Sequence diagram</title>
      <rect width={width} height={bottomY + 12} fill="#0f1117" rx="8" />

      {actors.map((a, i) => (
        <g key={a.id}>
          <rect x={xFor(i) - 58} y={6} width={116} height={24} rx={5} fill="#22263a" stroke={color} strokeWidth="1" />
          <text x={xFor(i)} y={22} textAnchor="middle" fill={color} fontSize="9" fontFamily="monospace">{a.label}</text>
          <line x1={xFor(i)} y1={30} x2={xFor(i)} y2={bottomY} stroke="#2d3148" strokeWidth="1" strokeDasharray="3,3" />
        </g>
      ))}

      {messages.map((m, idx) => {
        const y = topY + 26 + idx * rowH;
        // dashed just means "response arrow" (matching the master source's
        // own request-solid/response-dashed convention) and keeps the
        // flow's own color; only an explicit variant overrides both stroke
        // color and marker — 'deny' for a genuine policy denial, 'allow'
        // for a genuine grant — so a dashed response never reads as an
        // error just because it happens to be a response.
        const stroke = m.variant === 'deny' ? '#f87171' : m.variant === 'allow' ? '#10b981' : color;
        const marker = m.variant === 'deny' ? `url(#${markerId}-deny)` : m.variant === 'allow' ? `url(#${markerId}-allow)` : `url(#${markerId})`;
        if (m.self) {
          const x = xFor(actors.findIndex((a) => a.id === m.from));
          return (
            <g key={idx}>
              <path d={`M ${x},${y} q 42,0 42,13 q 0,13 -42,13`} fill="none" stroke={stroke} strokeWidth="1.2" strokeDasharray={m.dashed ? '4,3' : undefined} markerEnd={marker} />
              <text x={x + 10} y={y - 5} fill="#cdd2f0" fontSize="7.5" fontFamily="monospace">{m.stepNum}. {m.label}</text>
            </g>
          );
        }
        const x1 = xFor(actors.findIndex((a) => a.id === m.from));
        const x2 = xFor(actors.findIndex((a) => a.id === m.to));
        return (
          <g key={idx}>
            <line x1={x1} y1={y} x2={x2} y2={y} stroke={stroke} strokeWidth="1.2" strokeDasharray={m.dashed ? '4,3' : undefined} markerEnd={marker} />
            <text x={(x1 + x2) / 2} y={y - 6} textAnchor="middle" fill="#cdd2f0" fontSize="7.5" fontFamily="monospace">{m.stepNum}. {m.label}</text>
          </g>
        );
      })}

      <defs>
        <marker id={markerId} markerWidth="7" markerHeight="6" refX="6" refY="3" orient="auto">
          <polygon points="0 0, 7 3, 0 6" fill={color} />
        </marker>
        <marker id={`${markerId}-allow`} markerWidth="7" markerHeight="6" refX="6" refY="3" orient="auto">
          <polygon points="0 0, 7 3, 0 6" fill="#10b981" />
        </marker>
        <marker id={`${markerId}-deny`} markerWidth="7" markerHeight="6" refX="6" refY="3" orient="auto">
          <polygon points="0 0, 7 3, 0 6" fill="#f87171" />
        </marker>
      </defs>
    </svg>
  );
}

// ── Overview data — shared by the SVG renderer below and the Mermaid path ──
const OVERVIEW_ACTORS = [
  { id: 'client', label: 'Client' },
  { id: 'plainid', label: 'PlainID' },
  { id: 'service', label: 'HSM Service' },
  { id: 'hsm', label: 'Azure Managed HSM' },
  { id: 'kvsecrets', label: 'Azure KV Secrets' },
  { id: 'edek', label: 'EDEK Store' },
  { id: 'accessstore', label: 'Access Store' },
  { id: 'redis', label: 'Redis Cache' },
  { id: 'cekrotationsvc', label: 'CEK Rotation Svc' },
  { id: 'auditor', label: 'Auditor SPN' },
];
const OVERVIEW_MAIN_ROWS = [
  { from: 'client', to: 'plainid', label: '1. Policy check (JWT + logged-in user identity)', color: '#eab308' },
  { from: 'plainid', to: 'client', label: '2. Permit / Deny decision', color: '#eab308' },
  { from: 'client', to: 'service', label: '3. Encrypt or Decrypt — client\'s own JWT (direct); Encrypt supports an optional dek_name to REUSE instead of mint', color: '#a78bfa' },
  { from: 'service', to: 'accessstore', label: '4. [decrypt only] grant check — app_grants, then app_dek_grants', color: '#fb923c' },
  { from: 'service', to: 'hsm', label: '5. wrap/unwrap (cache miss, or REUSE unwrap) — Service SPN', color: '#a78bfa' },
  { from: 'service', to: 'redis', label: '6. cache GET/SET — {slot}:{kv_ver}:{edek_id}', color: '#14b8a6' },
  { from: 'service', to: 'edek', label: '7. persist / lookup edek_record', color: '#38bdf8' },
  { from: 'service', to: 'client', label: '8. Result: ciphertext / plaintext', color: '#a78bfa' },
];
const OVERVIEW_ROTATION_ROWS = [
  { from: 'cekrotationsvc', to: 'kvsecrets', label: '9. write inactive slot, then flip current_key — HSM Service SPN', color: '#eab308' },
  { from: 'service', to: 'kvsecrets', label: '10. poll detects change (30s) → fetch + rotate() — Service SPN', color: '#eab308', dashed: true },
];
const OVERVIEW_AUDIT_ROWS = [
  { from: 'auditor', to: 'kvsecrets', label: '11. read-only secrets (crosses subscription boundary)', color: '#f87171', dashed: true },
  { from: 'auditor', to: 'edek', label: '12. read-only DB scan — hsm_crypto (crosses subscription boundary)', color: '#f87171', dashed: true },
  { from: 'auditor', to: 'accessstore', label: '13. read-only DB scan — hsm_access grants + registrations', color: '#f87171', dashed: true },
];

// ── End-to-end overview — condensed macro-view across all 9 participants ────
// Deliberately NOT all ~28 individual messages: past ~8-10 arrows across 9
// lifelines a sequence diagram stops being readable. This shows the request
// flow, the CEK Rotation flow, and the Audit/Scan flow as three separated
// row-groups instead, matching the 9-participant list and section grouping
// in hsm_project's own sequence diagram (2026-07-20 ADR-014 amendment) —
// CEK Rotation and Audit/Scan are both independent, out-of-band processes,
// not later steps of the same request lifecycle as Policy Check → Encrypt/
// Decrypt, so each gets its own divider.
// Kept in place, unmodified, as the USE_MERMAID_FLOWS rollback target.
function OverviewSequenceDiagram() {
  const width = 1300;
  const actors = OVERVIEW_ACTORS;
  const laneGap = width / (actors.length + 1);
  const xFor = (i) => laneGap * (i + 1);

  const mainRows = OVERVIEW_MAIN_ROWS;
  const rotationRows = OVERVIEW_ROTATION_ROWS;
  const auditRows = OVERVIEW_AUDIT_ROWS;

  const topY = 34;
  const rowH = 42;
  const dividerH = 32;
  const mainBottom = topY + 26 + mainRows.length * rowH;
  const divider1Y = mainBottom + dividerH / 2;
  const rotationTop = mainBottom + dividerH;
  const rotationBottom = rotationTop + rotationRows.length * rowH;
  const divider2Y = rotationBottom + dividerH / 2;
  const auditTop = rotationBottom + dividerH;
  const bottomY = auditTop + auditRows.length * rowH + 16;

  return (
    <svg viewBox={`0 0 ${width} ${bottomY + 12}`} style={s.seqSvg} role="img">
      <title>End-to-end overview across the request, CEK rotation, and audit/scan flows</title>
      <rect width={width} height={bottomY + 12} fill="#0f1117" rx="8" />

      {actors.map((a, i) => (
        <g key={a.id}>
          <rect x={xFor(i) - 60} y={6} width={120} height={24} rx={5} fill="#22263a" stroke="#8b92b8" strokeWidth="1" />
          <text x={xFor(i)} y={22} textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">{a.label}</text>
          <line x1={xFor(i)} y1={30} x2={xFor(i)} y2={bottomY} stroke="#2d3148" strokeWidth="1" strokeDasharray="3,3" />
        </g>
      ))}

      {mainRows.map((m, idx) => {
        const y = topY + 26 + idx * rowH;
        const x1 = xFor(actors.findIndex((a) => a.id === m.from));
        const x2 = xFor(actors.findIndex((a) => a.id === m.to));
        return (
          <g key={`main-${idx}`}>
            <line x1={x1} y1={y} x2={x2} y2={y} stroke={m.color} strokeWidth="1.4" strokeDasharray={m.dashed ? '4,3' : undefined} markerEnd={`url(#ov-arrow-main-${idx})`} />
            <text x={(x1 + x2) / 2} y={y - 6} textAnchor="middle" fill="#cdd2f0" fontSize="8" fontFamily="monospace">{m.label}</text>
          </g>
        );
      })}

      <line x1="20" y1={divider1Y} x2={width - 20} y2={divider1Y} stroke="#555b7a" strokeWidth="1" strokeDasharray="6,4" />
      <rect x={width / 2 - 220} y={divider1Y - 11} width="440" height="20" rx="4" fill="#0f1117" />
      <text x={width / 2} y={divider1Y + 4} textAnchor="middle" fill="#78716c" fontSize="8" fontFamily="monospace">— CEK rotation: independent, every 4h — no path through HSM Service's request handling —</text>

      {rotationRows.map((m, idx) => {
        const y = rotationTop + 20 + idx * rowH;
        const x1 = xFor(actors.findIndex((a) => a.id === m.from));
        const x2 = xFor(actors.findIndex((a) => a.id === m.to));
        return (
          <g key={`rotation-${idx}`}>
            <line x1={x1} y1={y} x2={x2} y2={y} stroke={m.color} strokeWidth="1.4" strokeDasharray={m.dashed ? '4,3' : undefined} markerEnd={`url(#ov-arrow-rotation-${idx})`} />
            <text x={(x1 + x2) / 2} y={y - 6} textAnchor="middle" fill="#cdd2f0" fontSize="8" fontFamily="monospace">{m.label}</text>
          </g>
        );
      })}

      <line x1="20" y1={divider2Y} x2={width - 20} y2={divider2Y} stroke="#555b7a" strokeWidth="1" strokeDasharray="6,4" />
      <rect x={width / 2 - 220} y={divider2Y - 11} width="440" height="20" rx="4" fill="#0f1117" />
      <text x={width / 2} y={divider2Y + 4} textAnchor="middle" fill="#78716c" fontSize="8" fontFamily="monospace">— audit/scan: outside the Azure subscription — no path through HSM Service —</text>

      {auditRows.map((m, idx) => {
        const y = auditTop + 20 + idx * rowH;
        const x1 = xFor(actors.findIndex((a) => a.id === m.from));
        const x2 = xFor(actors.findIndex((a) => a.id === m.to));
        return (
          <g key={`audit-${idx}`}>
            <line x1={x1} y1={y} x2={x2} y2={y} stroke={m.color} strokeWidth="1.4" strokeDasharray="4,3" markerEnd={`url(#ov-arrow-audit-${idx})`} />
            <text x={(x1 + x2) / 2} y={y - 6} textAnchor="middle" fill="#cdd2f0" fontSize="8" fontFamily="monospace">{m.label}</text>
          </g>
        );
      })}

      <defs>
        {mainRows.map((m, idx) => (
          <marker key={`main-${idx}`} id={`ov-arrow-main-${idx}`} markerWidth="7" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 7 3, 0 6" fill={m.color} />
          </marker>
        ))}
        {rotationRows.map((m, idx) => (
          <marker key={`rotation-${idx}`} id={`ov-arrow-rotation-${idx}`} markerWidth="7" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 7 3, 0 6" fill={m.color} />
          </marker>
        ))}
        {auditRows.map((m, idx) => (
          <marker key={`audit-${idx}`} id={`ov-arrow-audit-${idx}`} markerWidth="7" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 7 3, 0 6" fill={m.color} />
          </marker>
        ))}
      </defs>
    </svg>
  );
}

function FlowsSequence() {
  let stepNumber = 0;
  return (
    <>
      <section style={s.panel}>
        <div style={s.panelHead}>
          <h3 style={{ ...s.panelTitle, color: '#8b92b8' }}>0. End-to-End Overview</h3>
          <p style={s.panelSub}>Macro view across all six flows (Startup, Policy Check, Encrypt, Decrypt, CEK Rotation, Audit/Scan) — not a merge of every individual step across 9 lifelines, past which a sequence diagram stops being readable. See each flow below for the full detail.</p>
        </div>
        <div style={s.seqWrap}>
          {USE_MERMAID_FLOWS
            ? <MermaidSequence text={buildOverviewMermaidText(OVERVIEW_ACTORS, OVERVIEW_MAIN_ROWS, OVERVIEW_ROTATION_ROWS, OVERVIEW_AUDIT_ROWS)} />
            : <OverviewSequenceDiagram />}
        </div>
      </section>

      {FLOWS.map((flow, flowIdx) => (
        <section key={flow.title} style={s.panel}>
          <div style={s.panelHead}>
            <h3 style={{ ...s.panelTitle, color: flow.color }}>{flow.title}</h3>
          </div>

          <div style={s.seqWrap}>
            {USE_MERMAID_FLOWS
              ? <MermaidSequence text={buildFlowMermaidText(flow)} />
              : (
                <SequenceDiagram
                  actors={flow.actors}
                  messages={flow.messages}
                  color={flow.color}
                  markerId={`seq-arrow-${flowIdx}`}
                />
              )}
          </div>

          <ol style={s.flowStepList}>
            {flow.steps.map((step) => {
              stepNumber += 1;
              return (
                <li key={stepNumber} style={s.flowStep}>
                  <span style={{ ...s.flowStepNum, color: flow.color }}>{stepNumber}</span>
                  <span>{step}</span>
                </li>
              );
            })}
          </ol>
        </section>
      ))}
    </>
  );
}

// ── Component ──────────────────────────────────────────────────────────────────
export default function HsmDemo() {
  // ── Tab state, deep-linkable via #diagram ──────────────────────────────────
  function tabFromHash() {
    const h = window.location.hash.slice(1);
    return h === 'diagram' || h === 'flows' || h === 'status' ? h : 'demo';
  }
  const [activeTab, setActiveTab] = useState(tabFromHash);

  useEffect(() => {
    function onHash() { setActiveTab(tabFromHash()); }
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  function selectTab(tab) {
    setActiveTab(tab);
    const base = window.location.pathname + window.location.search;
    window.history.replaceState(null, '', tab === 'demo' ? base : `${base}#${tab}`);
  }

  // ── Demo apps ────────────────────────────────────────────────────────────────
  const [apps, setApps] = useState([]);
  const [selectedAppId, setSelectedAppId] = useState(null);
  const [appsError, setAppsError] = useState(null);

  useEffect(() => {
    callApi('/demo/apps').then((res) => {
      if (res.ok && res.data) {
        setApps(res.data.apps || []);
        if (res.data.apps?.length) setSelectedAppId(res.data.apps[0].app_id);
      } else {
        setAppsError(errMessage(res, 'Could not load demo apps — is the HSM service running?'));
      }
    });
  }, []);

  const selectedApp = apps.find((a) => a.app_id === selectedAppId) || null;
  const grantApp     = apps.find((a) => a.scopes?.includes('grant')) || null;

  // ── Panel 2: Encrypt ─────────────────────────────────────────────────────────
  const [plaintext, setPlaintext]       = useState('');
  const [dataClass, setDataClass]       = useState('');
  const [dekName, setDekName]           = useState('');
  const [encryptResult, setEncryptResult] = useState(null);
  const [encryptError, setEncryptError]   = useState(null);
  const [encrypting, setEncrypting]       = useState(false);

  // end_user_id is an explicit request field, not derived from the app's JWT
  // (the JWT only ever proves App-ID — see Architecture Diagram tab). It's
  // per-operation, not per-app — each call independently states which
  // end-user it's acting on behalf of — so it lives on the Encrypt and
  // Decrypt panels rather than on the app picker above them.
  const [encryptEndUserId, setEncryptEndUserId] = useState('');

  async function handleEncrypt() {
    if (!selectedApp || !plaintext.trim()) return;
    setEncrypting(true);
    setEncryptError(null);
    const res = await callApi('/encrypt', {
      method: 'POST',
      app: selectedApp,
      // X-Response-Detail: full — this demo's field-breakdown panel explains
      // every field it gets back, so it needs the informational/audit fields
      // (edek_id, owner_app_id, algorithm, encoding, kek_version) that real
      // callers don't get by default.
      extraHeaders: { 'X-Response-Detail': 'full' },
      body: { plaintext, data_classification: dataClass || null, dek_name: dekName.trim() || null, end_user_id: encryptEndUserId.trim() || undefined, context: { source: 'demo-ui' } },
    });
    setEncrypting(false);
    if (res.ok) {
      setEncryptResult(res.data);
      setDecryptForm({ ciphertextToken: res.data.ciphertext || '' });
      setDecryptResult(null);
      setDecryptError(null);
      loadEdekRecords();
    } else {
      setEncryptError(errMessage(res, 'Encrypt failed'));
    }
  }

  // ── Panel 3: Decrypt ─────────────────────────────────────────────────────────
  // A single opaque ciphertext field replaces the old edek_id/iv/ciphertext/tag
  // quadruplet — the caller stores just this one token and passes it back as-is.
  const [decryptForm, setDecryptForm] = useState({ ciphertextToken: '' });
  const [decryptResult, setDecryptResult] = useState(null);
  const [decryptError, setDecryptError]   = useState(null);
  const [decrypting, setDecrypting]       = useState(false);

  // ── Simulated Redis cache state (client-side only) ──────────────────────────
  // The external HSM service owns the real cache — this repo has no visibility
  // into whether a given decrypt was actually a cache hit or a cache miss on
  // the service side. cacheSeen is purely a local record of which edek_ids
  // have already been decrypted once in THIS browser session, used to render
  // a "simulated" hit/miss badge that illustrates the caching behavior without
  // claiming to reflect the real service's internal cache state. edek_id is
  // decoded from the token client-side (edekIdFromToken) since /decrypt no
  // longer takes or returns it directly.
  const [cacheSeen, setCacheSeen] = useState({}); // edek_id -> decrypt count this session

  // Independent of encryptEndUserId — a decrypt is frequently performed on
  // behalf of a different end-user than the one who originally encrypted the
  // data (that's exactly what the Cross-App Decrypt Grants panel below
  // demonstrates), so the two fields must not be tied together.
  const [decryptEndUserId, setDecryptEndUserId] = useState('');

  async function handleDecrypt() {
    if (!selectedApp) return;
    setDecrypting(true);
    setDecryptError(null);
    const token = decryptForm.ciphertextToken.trim();
    const edekId = edekIdFromToken(token);
    const res = await callApi('/decrypt', {
      method: 'POST',
      app: selectedApp,
      // X-Response-Detail: full — needs owner_app_id in the response below.
      extraHeaders: { 'X-Response-Detail': 'full' },
      body: {
        ciphertext:   token,
        end_user_id:  decryptEndUserId.trim() || undefined,
      },
    });
    setDecrypting(false);
    if (res.ok) {
      const simulatedHit = !!(edekId && cacheSeen[edekId]);
      setDecryptResult({ ...res.data, decrypted_as: selectedApp.app_id, end_user_id_sent: decryptEndUserId.trim() || null, cache_hit_simulated: simulatedHit });
      if (edekId) setCacheSeen((prev) => ({ ...prev, [edekId]: (prev[edekId] || 0) + 1 }));
    } else {
      setDecryptError(errMessage(res, 'Decrypt failed'));
    }
  }

  // ── Panel 4: Key rotation + HSM state ───────────────────────────────────────
  const [rotateResult, setRotateResult] = useState(null);
  const [rotateError, setRotateError]   = useState(null);
  const [rotating, setRotating]         = useState(false);
  const [hsmState, setHsmState]         = useState(null);

  const loadHsmState = useCallback(() => {
    callApi('/demo/hsm-state').then((res) => { if (res.ok) setHsmState(res.data); });
  }, []);
  useEffect(() => { loadHsmState(); }, [loadHsmState]);

  async function handleRotate() {
    if (!selectedApp) return;
    setRotating(true);
    setRotateError(null);
    setRotateResult(null);
    // Uses the SELECTED app's identity (not forced to an admin app) — this lets
    // the demo show a 403 denial when a non-"rotate" app attempts rotation.
    const res = await callApi('/admin/rotate-kek', { method: 'POST', app: selectedApp });
    setRotating(false);
    if (res.ok) {
      setRotateResult(res.data);
      loadHsmState();
    } else {
      setRotateError(errMessage(res, 'Rotation failed'));
    }
  }

  // ── Panel 5: Cross-app decrypt grants ───────────────────────────────────────
  const [grants, setGrants]         = useState([]);
  const [granteeId, setGranteeId]   = useState('');
  const [ownerId, setOwnerId]       = useState('');
  const [grantError, setGrantError] = useState(null);
  const [grantBusy, setGrantBusy]   = useState(false);

  const loadGrants = useCallback(() => {
    if (!grantApp) return;
    callApi('/admin/grants', { app: grantApp }).then((res) => { if (res.ok) setGrants(res.data.grants || []); });
  }, [grantApp]);
  useEffect(() => { loadGrants(); }, [loadGrants]);

  async function handleAddGrant() {
    if (!grantApp || !granteeId || !ownerId) return;
    setGrantBusy(true);
    setGrantError(null);
    // Grant actions always act as whichever app holds the "grant" scope,
    // regardless of the app selected in panel 1.
    const res = await callApi('/admin/grants', {
      method: 'POST',
      app: grantApp,
      body: { grantee_app_id: granteeId, owner_app_id: ownerId },
    });
    setGrantBusy(false);
    if (res.ok) {
      setGranteeId('');
      setOwnerId('');
      loadGrants();
    } else {
      setGrantError(errMessage(res, 'Could not add grant'));
    }
  }

  async function handleRevokeGrant(g) {
    if (!grantApp) return;
    await callApi('/admin/grants', {
      method: 'DELETE',
      app: grantApp,
      body: { grantee_app_id: g.grantee_app_id, owner_app_id: g.owner_app_id },
    });
    loadGrants();
  }


  // ── Panel 6: Latest EDEK records ────────────────────────────────────────────
  const [edekRecords, setEdekRecords] = useState([]);
  const loadEdekRecords = useCallback(() => {
    callApi('/demo/edek-records?limit=15').then((res) => { if (res.ok) setEdekRecords(res.data.records || []); });
  }, []);
  useEffect(() => { loadEdekRecords(); }, [loadEdekRecords]);

  // ── Panel 7: Consumer application table ─────────────────────────────────────
  const [custName, setCustName]                 = useState('');
  const [custEmail, setCustEmail]                = useState('');
  const [custAccountNumber, setCustAccountNumber] = useState('');
  const [accounts, setAccounts]                   = useState([]);
  const [accountsError, setAccountsError]         = useState(null);
  const [creatingAccount, setCreatingAccount]     = useState(false);
  const [revealAsId, setRevealAsId]               = useState(null);
  const [revealEndUserId, setRevealEndUserId]     = useState('');
  const [revealed, setRevealed]                   = useState({});
  const [revealErrors, setRevealErrors]           = useState({});
  const [revealBusyId, setRevealBusyId]           = useState(null);

  const loadAccounts = useCallback(() => {
    callApi('/demo/consumer/accounts').then((res) => { if (res.ok) setAccounts(res.data.accounts || []); });
  }, []);
  useEffect(() => { loadAccounts(); }, [loadAccounts]);

  useEffect(() => {
    if (!revealAsId && selectedAppId) setRevealAsId(selectedAppId);
  }, [selectedAppId, revealAsId]);

  async function handleCreateAccount() {
    if (!custName.trim() || !custEmail.trim() || !custAccountNumber.trim()) return;
    setCreatingAccount(true);
    setAccountsError(null);
    // No auth header — simulates payments-svc's own backend having already authenticated.
    const res = await callApi('/demo/consumer/accounts', {
      method: 'POST',
      body: { customer_name: custName, email: custEmail, account_number: custAccountNumber },
    });
    setCreatingAccount(false);
    if (res.ok) {
      setCustName('');
      setCustEmail('');
      setCustAccountNumber('');
      loadAccounts();
    } else {
      setAccountsError(errMessage(res, 'Could not create account'));
    }
  }

  async function handleReveal(id) {
    setRevealBusyId(id);
    setRevealErrors((prev) => ({ ...prev, [id]: null }));
    const res = await callApi(`/demo/consumer/accounts/${id}/reveal`, {
      method: 'POST',
      body: { reveal_as: revealAsId, end_user_id: revealEndUserId.trim() || undefined },
    });
    setRevealBusyId(null);
    if (res.ok) {
      setRevealed((prev) => ({ ...prev, [id]: res.data.account_number }));
    } else {
      setRevealErrors((prev) => ({ ...prev, [id]: errMessage(res, 'Reveal denied') }));
    }
  }

  // ── Panel 8: Live audit trail — polled every 3s ─────────────────────────────
  const [auditEvents, setAuditEvents] = useState([]);
  useEffect(() => {
    let cancelled = false;
    function load() {
      callApi('/demo/audit-log?limit=30').then((res) => {
        if (!cancelled && res.ok) setAuditEvents(res.data.events || []);
      });
    }
    load();
    const id = setInterval(load, 3000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  // ── Panel: Development Status — replicated from the master's own Development
  // Status tab (see ADR-014's hsm-core-service amendment). Backed by the real
  // service's DB (DevStatusController), not a bundled static file, so edits
  // survive a restart there — same "call the real service directly" pattern
  // as every other panel in this page; no Express proxy, no local mock state.
  const [devStatusItems, setDevStatusItems]     = useState([]);
  const [devStatusUpdated, setDevStatusUpdated] = useState('');
  const [devStatusDrafts, setDevStatusDrafts]   = useState({}); // id -> { item, status, notes }
  const [devStatusError, setDevStatusError]     = useState(null);
  const [devStatusConfirmDelete, setDevStatusConfirmDelete] = useState(null); // id awaiting a second click
  const [newStatusCategory, setNewStatusCategory] = useState('');
  const [newStatusItem, setNewStatusItem]         = useState('');
  const [newStatusStatus, setNewStatusStatus]     = useState('N');
  const [newStatusNotes, setNewStatusNotes]       = useState('');
  const [devStatusBusy, setDevStatusBusy]         = useState(false);

  const loadDevStatus = useCallback(() => {
    callApi('/demo/dev-status').then((res) => {
      if (!res.ok) { setDevStatusError(errMessage(res, 'Could not load development status')); return; }
      const items = res.data?.items || [];
      setDevStatusItems(items);
      setDevStatusError(null);
      const latest = items.reduce((max, r) => (r.updated_at && (!max || r.updated_at > max)) ? r.updated_at : max, null);
      setDevStatusUpdated(latest
        ? `${items.length} tracked items · last updated ${new Date(latest).toLocaleString()}`
        : `${items.length} tracked items`);
      setDevStatusDrafts(Object.fromEntries(items.map((r) => [r.id, { item: r.item, status: r.status, notes: r.notes || '' }])));
    });
  }, []);
  useEffect(() => { if (activeTab === 'status') loadDevStatus(); }, [activeTab, loadDevStatus]);

  const devStatusCategories = [...new Set(devStatusItems.map((r) => r.category))];
  const devStatusGroups = devStatusCategories.map((category) => ({
    category,
    rows: devStatusItems.filter((r) => r.category === category),
  }));

  function updateDevStatusDraft(id, field, value) {
    setDevStatusDrafts((prev) => ({ ...prev, [id]: { ...prev[id], [field]: value } }));
  }

  async function handleSaveDevStatus(row) {
    const draft = devStatusDrafts[row.id] || {};
    setDevStatusBusy(true);
    const res = await callApi(`/demo/dev-status/${row.id}`, {
      method: 'PUT',
      body: { category: row.category, item: draft.item, status: draft.status, notes: draft.notes },
    });
    setDevStatusBusy(false);
    if (res.ok) loadDevStatus();
    else setDevStatusError(errMessage(res, 'Could not save item'));
  }

  async function handleDeleteDevStatus(id) {
    if (devStatusConfirmDelete !== id) {
      setDevStatusConfirmDelete(id);
      setTimeout(() => setDevStatusConfirmDelete((cur) => (cur === id ? null : cur)), 3000);
      return;
    }
    setDevStatusConfirmDelete(null);
    const res = await callApi(`/demo/dev-status/${id}`, { method: 'DELETE' });
    if (res.ok) loadDevStatus();
    else setDevStatusError(errMessage(res, 'Could not delete item'));
  }

  async function handleAddDevStatus() {
    if (!newStatusCategory.trim() || !newStatusItem.trim()) {
      setDevStatusError('Category and item are required.');
      return;
    }
    setDevStatusBusy(true);
    const res = await callApi('/demo/dev-status', {
      method: 'POST',
      body: { category: newStatusCategory.trim(), item: newStatusItem.trim(), status: newStatusStatus, notes: newStatusNotes.trim() },
    });
    setDevStatusBusy(false);
    if (res.ok) {
      setNewStatusCategory(''); setNewStatusItem(''); setNewStatusStatus('N'); setNewStatusNotes('');
      loadDevStatus();
    } else {
      setDevStatusError(errMessage(res, 'Could not add item'));
    }
  }

  // ── Render ───────────────────────────────────────────────────────────────────
  return (
    <div style={s.container}>
      <div style={s.pageHeader}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <span style={s.headerIcon}>🔐</span>
          <div>
            <div style={s.headerTitle}>Sensec HSM Demo</div>
            <div style={s.headerSub}>Centralized envelope-encryption microservice</div>
          </div>
        </div>
      </div>

      {/* ── Tabs ── */}
      <div style={s.tabStrip} role="tablist">
        <button
          role="tab"
          aria-selected={activeTab === 'demo'}
          style={{ ...s.tab, ...(activeTab === 'demo' ? s.tabActive : {}) }}
          onClick={() => selectTab('demo')}
        >
          Live Demo
        </button>
        <button
          role="tab"
          aria-selected={activeTab === 'diagram'}
          style={{ ...s.tab, ...(activeTab === 'diagram' ? s.tabActive : {}) }}
          onClick={() => selectTab('diagram')}
        >
          Architecture Diagram
        </button>
        <button
          role="tab"
          aria-selected={activeTab === 'flows'}
          style={{ ...s.tab, ...(activeTab === 'flows' ? s.tabActive : {}) }}
          onClick={() => selectTab('flows')}
        >
          Flows
        </button>
        <button
          role="tab"
          aria-selected={activeTab === 'status'}
          style={{ ...s.tab, ...(activeTab === 'status' ? s.tabActive : {}) }}
          onClick={() => selectTab('status')}
        >
          Development Status
        </button>
      </div>

      <div style={s.body}>
        {/* ── Tab 1: Live Demo ── */}
        {activeTab === 'demo' && <div role="tabpanel" style={s.demoScroll}>
          {appsError && <div style={s.errorBanner}>{appsError}</div>}

          {/* Panel 1: App picker */}
          <Panel title="1. Choose a Calling App" sub="Each app has its own token and permitted scopes — exactly like a real registered application.">
            <select style={s.selectFull} value={selectedAppId || ''} onChange={(e) => setSelectedAppId(e.target.value)}>
              {apps.map((a) => <option key={a.app_id} value={a.app_id}>{a.app_id}</option>)}
            </select>
            {selectedApp && <ScopeChips scopes={selectedApp.scopes} />}
          </Panel>

          {/* Panel 2: Encrypt */}
          <Panel title="2. Encrypt" sub="End User ID is sent as an explicit request field — never derived from the app's JWT, which only ever proves App-ID (see Architecture Diagram tab).">
            <textarea
              style={s.textarea}
              placeholder="Plaintext to encrypt…"
              value={plaintext}
              onChange={(e) => setPlaintext(e.target.value)}
            />
            <div style={s.formRow}>
              <label style={s.label}>Data classification</label>
              <select style={s.select} value={dataClass} onChange={(e) => setDataClass(e.target.value)}>
                <option value="">none</option>
                <option value="pii">pii</option>
                <option value="pci">pci</option>
                <option value="internal">internal</option>
              </select>
            </div>
            <div style={s.formRow}>
              <label style={s.label} title="Encrypt the same name twice and watch Latest EDEK Records stay at one row instead of growing — the second call reuses the DEK instead of minting a new one. Leave blank for the default: a fresh DEK every call.">DEK Name</label>
              <input
                style={s.input}
                placeholder="e.g. customers.ssn (optional)"
                value={dekName}
                onChange={(e) => setDekName(e.target.value)}
              />
            </div>
            <div style={s.formRow}>
              <label style={s.label}>End User ID</label>
              <input
                style={s.input}
                placeholder="end_user_id (optional)"
                value={encryptEndUserId}
                onChange={(e) => setEncryptEndUserId(e.target.value)}
              />
              <button style={s.primaryBtn} onClick={handleEncrypt} disabled={encrypting || !plaintext.trim()}>
                {encrypting ? 'Encrypting…' : 'Encrypt'}
              </button>
            </div>
            {encryptError && <div style={s.errorBanner}>{encryptError}</div>}
            {encryptResult && (
              <LabeledRows
                rows={ENCRYPT_FIELD_ORDER.map((key) => ({
                  label: key,
                  value: encryptResult[key] ?? '—',
                  explainer: FIELD_EXPLAINERS[key],
                }))}
              />
            )}
          </Panel>

          {/* Panel 3: Decrypt */}
          <Panel title="3. Decrypt" sub="ciphertext auto-filled from the Encrypt response above — a single opaque token, never decoded client-side. End User ID is independent of the Encrypt panel's — a decrypt is often performed on behalf of a different end-user than the one who originally encrypted the data (see Cross-App Decrypt Grants below).">
            <textarea
              style={s.textarea}
              placeholder="ciphertext…  (auto-filled after Encrypt above)"
              value={decryptForm.ciphertextToken}
              onChange={(e) => setDecryptForm({ ciphertextToken: e.target.value })}
            />
            <div style={s.formRow}>
              <label style={s.label}>End User ID</label>
              <input
                style={s.input}
                placeholder="end_user_id (optional)"
                value={decryptEndUserId}
                onChange={(e) => setDecryptEndUserId(e.target.value)}
              />
            </div>
            <button style={s.primaryBtn} onClick={handleDecrypt} disabled={decrypting || !decryptForm.ciphertextToken.trim()}>
              {decrypting ? 'Decrypting…' : 'Decrypt'}
            </button>
            {decryptError && <div style={s.errorBanner}>{decryptError}</div>}
            {decryptResult && (
              <LabeledRows
                rows={[
                  { label: 'plaintext',     value: decryptResult.plaintext },
                  { label: 'owner_app_id',  value: decryptResult.owner_app_id },
                  { label: 'decrypted_as',  value: decryptResult.decrypted_as, explainer: 'The app identity that performed this decrypt — compare against owner_app_id to see a cross-app grant in action' },
                  { label: 'end_user_id',   value: decryptResult.end_user_id ?? decryptResult.end_user_id_sent ?? '—', explainer: 'Explicit request field, not JWT-derived — the actual value used for this call\'s audit_log entry' },
                  { label: 'redis_cache',   value: decryptResult.cache_hit_simulated ? 'simulated hit — skipped HSM' : 'simulated miss — unwrapped via HSM', explainer: 'Simulated locally: the real HSM service owns cache state and does not echo it back — see panel 3b below' },
                ]}
              />
            )}
          </Panel>

          {/* Panel 3b: Redis Cache (simulated) */}
          <Panel title="3b. Redis Cache — Simulated" sub="The real cache lives in the external HSM service, which doesn't expose its hit/miss state to this demo. This panel tracks which edek_ids have been decrypted before in this browser session, purely to illustrate the cache behavior described in the Architecture Diagram and Flows tabs.">
            {Object.keys(cacheSeen).length === 0 ? (
              <p style={s.muted}>No decrypts yet this session.</p>
            ) : (
              <table style={s.table}>
                <thead>
                  <tr><th style={s.th}>edek_id</th><th style={s.th}>Decrypts this session</th><th style={s.th}>Next decrypt would be</th></tr>
                </thead>
                <tbody>
                  {Object.entries(cacheSeen).map(([id, count]) => (
                    <tr key={id}>
                      <td style={s.tdMono}>{truncate(id, 20)}</td>
                      <td style={s.td}>{count}</td>
                      <td style={s.td}>simulated hit</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            {Object.keys(cacheSeen).length > 0 && (
              <button style={{ ...s.dangerBtn, marginTop: '0.75rem' }} onClick={() => setCacheSeen({})}>Clear simulated cache</button>
            )}
          </Panel>

          {/* Panel 4: Key rotation + HSM state */}
          <div style={s.grid2}>
            <Panel title="4. Key Rotation" sub="Requires the rotate scope — try this with a non-admin app selected to see a denial.">
              <button style={s.primaryBtn} onClick={handleRotate} disabled={rotating}>
                {rotating ? 'Rotating…' : 'Rotate KEK'}
              </button>
              {rotateError && <div style={s.errorBanner}>{rotateError}</div>}
              {rotateResult && (
                <LabeledRows
                  rows={[
                    { label: 'new_kek_version', value: rotateResult.new_kek_version },
                    { label: 'records_queued',  value: rotateResult.records_queued },
                  ]}
                />
              )}
            </Panel>

            <Panel title="Simulated HSM State">
              {hsmState ? (
                <table style={s.table}>
                  <thead>
                    <tr><th style={s.th}>Version</th><th style={s.th}>Key Size</th><th style={s.th}>Created</th><th style={s.th}>Current</th></tr>
                  </thead>
                  <tbody>
                    {(hsmState.versions || []).map((v) => (
                      <tr key={v.version}>
                        <td style={s.td}>{v.version}</td>
                        <td style={s.td}>{v.key_length_bits} bits</td>
                        <td style={s.td}>{fmtTime(v.created_at)}</td>
                        <td style={s.td}>{v.is_current ? '✓' : ''}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : <p style={s.muted}>Loading…</p>}
            </Panel>
          </div>

          {/* Panel 5: Cross-app decrypt grants */}
          <Panel title="5. Cross-App Decrypt Grants" sub="Always managed by whichever app holds the grant scope, regardless of the app selected above.">
            {!grantApp ? (
              <p style={s.muted}>No demo app holds the "grant" scope.</p>
            ) : (
              <>
                <div style={s.formRow}>
                  <select style={s.select} value={granteeId} onChange={(e) => setGranteeId(e.target.value)}>
                    <option value="">Grantee app…</option>
                    {apps.map((a) => <option key={a.app_id} value={a.app_id}>{a.app_id}</option>)}
                  </select>
                  <select style={s.select} value={ownerId} onChange={(e) => setOwnerId(e.target.value)}>
                    <option value="">Owner app…</option>
                    {apps.map((a) => <option key={a.app_id} value={a.app_id}>{a.app_id}</option>)}
                  </select>
                  <button style={s.primaryBtn} onClick={handleAddGrant} disabled={grantBusy || !granteeId || !ownerId}>
                    {grantBusy ? 'Adding…' : 'Add Grant'}
                  </button>
                </div>
                {grantError && <div style={s.errorBanner}>{grantError}</div>}
                <table style={s.table}>
                  <thead>
                    <tr><th style={s.th}>Grantee</th><th style={s.th}>Owner</th><th style={s.th}>Created</th><th style={s.th}></th></tr>
                  </thead>
                  <tbody>
                    {grants.map((g) => (
                      <tr key={`${g.grantee_app_id}:${g.owner_app_id}`}>
                        <td style={s.td}>{g.grantee_app_id}</td>
                        <td style={s.td}>{g.owner_app_id}</td>
                        <td style={s.td}>{g.created_at ? new Date(g.created_at).toLocaleString() : '-'}</td>
                        <td style={s.td}><button style={s.dangerBtn} onClick={() => handleRevokeGrant(g)}>Revoke</button></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </>
            )}
          </Panel>

          {/* Panel 6: Latest EDEK records */}
          <Panel title="6. Latest EDEK Records">
            <table style={s.table}>
              <thead>
                <tr>
                  <th style={s.th}>EDEK ID</th><th style={s.th}>Owner</th><th style={s.th}>KEK Ver</th>
                  <th style={s.th}>Algorithm</th><th style={s.th}>Encoding</th><th style={s.th}>Classification</th>
                  <th style={s.th}>DEK Name</th><th style={s.th}>Status</th><th style={s.th}>Wrapped Blob</th><th style={s.th}>Created</th>
                </tr>
              </thead>
              <tbody>
                {edekRecords.map((r) => (
                  <tr key={r.edek_id}>
                    <td style={s.tdMono}>{truncate(r.edek_id, 12)}</td>
                    <td style={s.td}>{r.app_id}</td>
                    <td style={s.td}>{r.kek_version}</td>
                    <td style={s.td}>{r.algorithm}</td>
                    <td style={s.td}>{r.encoding}</td>
                    <td style={s.td}>{r.data_classification || '—'}</td>
                    <td style={s.td}>{r.dek_name || '—'}</td>
                    <td style={s.td}>{r.rotation_status}</td>
                    <td style={s.tdMono}>{truncate(r.edek_blob_preview, 24)}</td>
                    <td style={s.td}>{fmtTime(r.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Panel>

          {/* Panel 7: Consumer application table */}
          <Panel title="7. Consumer Application Table" sub="Simulates payments-svc's own database — a separate schema from this service's EDEK store. A single ciphertext column bundles everything needed for a future decrypt — no separate edek_id, IV, or tag columns required. Every account here shares the same DEK Name (customers.account_number) — create a second account and watch Latest EDEK Records stay flat instead of growing, same reuse behavior as panel 2's DEK Name field.">
            <div style={s.formGrid}>
              <input style={s.input} placeholder="Customer Name" value={custName} onChange={(e) => setCustName(e.target.value)} />
              <input style={s.input} placeholder="Email" value={custEmail} onChange={(e) => setCustEmail(e.target.value)} />
              <input style={s.input} placeholder="Account Number" value={custAccountNumber} onChange={(e) => setCustAccountNumber(e.target.value)} />
              <button style={s.primaryBtn} onClick={handleCreateAccount} disabled={creatingAccount}>
                {creatingAccount ? 'Creating…' : 'Create Account'}
              </button>
            </div>
            {accountsError && <div style={s.errorBanner}>{accountsError}</div>}

            <div style={s.formRow}>
              <label style={s.label}>Reveal as</label>
              <select style={s.select} value={revealAsId || ''} onChange={(e) => setRevealAsId(e.target.value)}>
                {apps.map((a) => <option key={a.app_id} value={a.app_id}>{a.app_id}</option>)}
              </select>
              <label style={s.label}>End User ID</label>
              <input
                style={s.input}
                placeholder="end_user_id (optional)"
                value={revealEndUserId}
                onChange={(e) => setRevealEndUserId(e.target.value)}
              />
            </div>

            <div style={s.schemaNote}>
              <strong>Column plan for this table:</strong>
              <table style={s.table}>
                <thead>
                  <tr><th style={s.th}>Column</th><th style={s.th}>Type</th><th style={s.th}>Sensitive?</th><th style={s.th}>Why this type</th></tr>
                </thead>
                <tbody>
                  <tr>
                    <td style={s.tdMono}>customer_name</td><td style={s.td}>VARCHAR(128)</td>
                    <td style={{ ...s.td, color: 'var(--success)' }}>non-sensitive</td><td style={s.td}>Ordinary app data</td>
                  </tr>
                  <tr>
                    <td style={s.tdMono}>email</td><td style={s.td}>VARCHAR(256)</td>
                    <td style={{ ...s.td, color: 'var(--success)' }}>non-sensitive</td><td style={s.td}>Ordinary app data</td>
                  </tr>
                  <tr>
                    <td style={s.tdMono}>ciphertext</td><td style={s.td}>VARCHAR(512)</td>
                    <td style={{ ...s.td, color: 'var(--error)' }}>sensitive</td>
                    <td style={s.td}>Single opaque token — bundles EDEK ID, IV, tag, and ciphertext; base64url (printable ASCII); ~60 byte fixed overhead + 1.4× plaintext</td>
                  </tr>
                </tbody>
              </table>
            </div>

            <table style={s.table}>
              <thead>
                <tr>
                  <th style={s.th}>ID</th><th style={s.th}>Customer</th><th style={s.th}>Email</th>
                  <th style={s.th}>ciphertext</th><th style={s.th}>DEK Name</th><th style={s.th}>Created</th><th style={s.th}></th>
                </tr>
              </thead>
              <tbody>
                {accounts.map((acc) => (
                  <tr key={acc.id}>
                    <td style={s.td}>{acc.id}</td>
                    <td style={s.td}>{acc.customer_name}</td>
                    <td style={s.td}>{acc.email}</td>
                    <td style={s.tdMono}>
                      {revealed[acc.id] ? <strong style={{ color: 'var(--success)' }}>{revealed[acc.id]}</strong> : truncate(acc.ciphertext, 18)}
                    </td>
                    <td style={s.td}>{acc.dek_name || '—'}</td>
                    <td style={s.td}>{acc.created_at ? new Date(acc.created_at).toLocaleString() : '-'}</td>
                    <td style={s.td}>
                      <button style={s.primaryBtnSmall} disabled={revealBusyId === acc.id} onClick={() => handleReveal(acc.id)}>
                        {revealBusyId === acc.id ? '…' : 'Reveal'}
                      </button>
                      {revealErrors[acc.id] && <div style={s.errorInline}>{revealErrors[acc.id]}</div>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Panel>

          {/* Panel 8: Live audit trail */}
          <Panel title="8. Live Audit Trail" sub="Polls every 3s. Denials (failed scope checks, auth failures, grant rejections, blocked apps) appear with status = failure.">
            <table style={s.table}>
              <thead>
                <tr><th style={s.th}>Time</th><th style={s.th}>Event</th><th style={s.th}>App</th><th style={s.th}>Status</th><th style={s.th}>Detail</th></tr>
              </thead>
              <tbody>
                {auditEvents.map((ev, i) => (
                  <tr key={`${ev._epoch || i}-${i}`}>
                    <td style={s.td}>{fmtTime(ev._epoch)}</td>
                    <td style={s.td}>{ev.event_type}</td>
                    <td style={s.td}>{ev.app_id || ev.sub || '—'}</td>
                    <td style={{ ...s.td, color: ev.status === 'failure' ? 'var(--error)' : 'var(--success)', fontWeight: 700 }}>
                      {ev.status}
                    </td>
                    <td style={s.td}>
                      {[ev.reason, ev.edek_id && `edek:${truncate(ev.edek_id, 8)}`, ev.owner_app_id && `owner:${ev.owner_app_id}`, ev.caller_ip]
                        .filter(Boolean).join(' · ') || '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Panel>
        </div>}

        {/* ── Tab 2: Architecture Diagram ──
            Sticky-header scroll pattern: flex-shrink:0 header + flex:1;
            overflow-y:auto body. The header is plain HTML, not SVG — the
            common pitfall is trying position:sticky *inside* an SVG's
            coordinate system, which doesn't behave reliably. Keeping the
            header outside the <svg> entirely sidesteps that. ── */}
        {activeTab === 'diagram' && (
          <div role="tabpanel" style={s.diagramTabWrap}>
            <div style={s.diagramHeader}>
              <h3 style={s.diagramHeaderTitle}>HSM Core Service — Architecture</h3>
              <p style={s.diagramHeaderSub}>Cross-subscription 2-SPN split (HSM Service vs. Auditor) · shared PlainID PBAC (Client + Service) · KEK/CEK on separate resources · Redis DEK cache w/ CEK hot-reload</p>
            </div>
            <div style={s.diagramScrollBody}>
              <ArchitectureDiagram />
            </div>
          </div>
        )}

        {/* ── Tab 3: Flows — 6-flow numbered sequence, plain text/cards ── */}
        {activeTab === 'flows' && (
          <div role="tabpanel" style={s.demoScroll}>
            <FlowsSequence />
          </div>
        )}

        {/* ── Tab 4: Development Status — live CRUD against the real service's
            own DB-backed tracker (GET/POST/PUT/DELETE /demo/dev-status),
            replicated from the master's own tab of the same name. ── */}
        {activeTab === 'status' && (
          <div role="tabpanel" style={s.demoScroll}>
            <Panel
              title="Development Status"
              sub="Tracks both this port's own component-by-component build-out and the open backlog items discovered along the way (e.g. compliance gaps, un-tested integrations). N = not started, P = in progress, C = completed."
            >
              {devStatusError && <div style={s.errorBanner}>{devStatusError}</div>}
              {devStatusUpdated && <p style={s.muted}>{devStatusUpdated}</p>}

              {devStatusGroups.map(({ category, rows }) => (
                <div key={category} style={{ marginTop: '1.2rem' }}>
                  <div style={s.statusCategoryHeading}>{category}</div>
                  <table style={s.table}>
                    <thead>
                      <tr>
                        <th style={{ ...s.th, width: '4.5rem' }}>Status</th>
                        <th style={s.th}>Item</th>
                        <th style={s.th}>Notes</th>
                        <th style={{ ...s.th, width: '9.5rem' }}></th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((r) => {
                        const draft = devStatusDrafts[r.id] || { item: r.item, status: r.status, notes: r.notes || '' };
                        return (
                          <tr key={r.id}>
                            <td style={s.td}>
                              <select
                                style={s.select}
                                value={draft.status}
                                onChange={(e) => updateDevStatusDraft(r.id, 'status', e.target.value)}
                              >
                                <option value="N">N</option>
                                <option value="P">P</option>
                                <option value="C">C</option>
                              </select>
                            </td>
                            <td style={s.td}>
                              <input
                                style={s.input}
                                value={draft.item}
                                onChange={(e) => updateDevStatusDraft(r.id, 'item', e.target.value)}
                              />
                            </td>
                            <td style={s.td}>
                              <input
                                style={s.input}
                                value={draft.notes}
                                onChange={(e) => updateDevStatusDraft(r.id, 'notes', e.target.value)}
                              />
                            </td>
                            <td style={s.td}>
                              <div style={{ display: 'flex', gap: 6 }}>
                                <button style={s.primaryBtn} disabled={devStatusBusy} onClick={() => handleSaveDevStatus(r)}>Save</button>
                                <button style={s.dangerBtn} onClick={() => handleDeleteDevStatus(r.id)}>
                                  {devStatusConfirmDelete === r.id ? 'Confirm?' : 'Delete'}
                                </button>
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ))}

              <div style={s.statusCategoryHeading}>Add Item</div>
              <div style={s.formRow}>
                <input
                  style={s.input}
                  placeholder="Core Port"
                  list="statusCategoryList"
                  value={newStatusCategory}
                  onChange={(e) => setNewStatusCategory(e.target.value)}
                />
                <datalist id="statusCategoryList">
                  {devStatusCategories.map((c) => <option key={c} value={c} />)}
                </datalist>
                <input
                  style={{ ...s.input, flex: 2, minWidth: '14rem' }}
                  placeholder="New tracked item…"
                  value={newStatusItem}
                  onChange={(e) => setNewStatusItem(e.target.value)}
                />
                <select style={s.select} value={newStatusStatus} onChange={(e) => setNewStatusStatus(e.target.value)}>
                  <option value="N">N — Not started</option>
                  <option value="P">P — In progress</option>
                  <option value="C">C — Completed</option>
                </select>
                <input
                  style={{ ...s.input, flex: 2, minWidth: '12rem' }}
                  placeholder="Optional notes"
                  value={newStatusNotes}
                  onChange={(e) => setNewStatusNotes(e.target.value)}
                />
                <button style={s.primaryBtn} disabled={devStatusBusy} onClick={handleAddDevStatus}>+ Add Item</button>
              </div>
            </Panel>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────
const s = {
  container: { display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden', background: 'var(--bg)' },
  pageHeader: { padding: '1rem 1.25rem 0.75rem', borderBottom: '1px solid var(--border)', background: 'var(--surface)', flexShrink: 0 },
  headerIcon: { fontSize: '1.75rem', lineHeight: 1, flexShrink: 0 },
  headerTitle: { fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.02em' },
  headerSub: { fontSize: '0.78rem', color: 'var(--text-secondary)', marginTop: 2 },

  tabStrip: { display: 'flex', borderBottom: '1px solid var(--border)', background: 'var(--surface)', padding: '0 1rem', flexShrink: 0 },
  tab: {
    padding: '0.6rem 1.1rem', border: 'none', borderBottom: '2.5px solid transparent',
    background: 'transparent', color: 'var(--text-secondary)', fontSize: '0.85rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit', transition: 'color 0.15s',
  },
  tabActive: { color: 'var(--accent)', borderBottom: '2.5px solid var(--accent)' },

  body: { flex: 1, overflow: 'hidden', minHeight: 0, display: 'flex' },
  demoScroll: { flex: 1, overflowY: 'auto', padding: '1.25rem', display: 'flex', flexDirection: 'column', gap: '1.25rem' },

  panel: {
    background: 'var(--surface)', border: '1px solid var(--border)',
    borderLeft: '4px solid var(--accent)', borderRadius: 14,
    padding: '1.5rem 1.75rem', boxShadow: 'var(--shadow)',
  },
  panelHead: { marginBottom: '1rem' },
  panelTitle: { fontSize: '1.1rem', fontWeight: 700, color: 'var(--text-primary)', margin: 0, letterSpacing: '-0.01em' },
  panelSub: { fontSize: '0.85rem', color: 'var(--text-secondary)', margin: '0.35rem 0 0', lineHeight: 1.5 },

  grid2: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.25rem' },

  chipRow: { display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: '0.85rem' },
  chip: {
    fontSize: '0.8rem', fontWeight: 600, padding: '0.4rem 0.9rem', borderRadius: 20,
    borderWidth: '1.5px', borderStyle: 'solid', borderColor: 'var(--border)', fontFamily: 'inherit',
  },
  chipGranted: { background: 'var(--accent-light)', color: 'var(--accent)', borderColor: 'var(--accent)' },
  chipDenied: { color: 'var(--text-secondary)', opacity: 0.55, textDecoration: 'line-through' },

  textarea: {
    width: '100%', minHeight: 80, padding: '0.6rem 0.75rem', borderRadius: 8,
    border: '1.5px solid var(--border)', background: 'var(--bg)', color: 'var(--text-primary)',
    fontFamily: 'inherit', fontSize: '0.85rem', outline: 'none', resize: 'vertical', boxSizing: 'border-box',
  },
  formRow: { display: 'flex', gap: 8, alignItems: 'center', marginTop: '0.6rem', flexWrap: 'wrap' },
  formGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 8, marginTop: '0.6rem' },
  label: { fontSize: '0.78rem', fontWeight: 600, color: 'var(--text-secondary)' },
  select: {
    padding: '0.45rem 0.6rem', borderRadius: 7, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)', fontSize: '0.82rem', fontFamily: 'inherit', outline: 'none',
  },
  selectFull: {
    width: '100%', padding: '0.65rem 0.9rem', borderRadius: 9, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)', fontSize: '0.95rem', fontFamily: 'inherit',
    outline: 'none', boxSizing: 'border-box',
  },
  input: {
    padding: '0.45rem 0.6rem', borderRadius: 7, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)', fontSize: '0.82rem', fontFamily: 'inherit', outline: 'none', boxSizing: 'border-box',
  },
  primaryBtn: {
    padding: '0.5rem 1rem', borderRadius: 8, border: 'none', background: 'var(--accent)',
    color: '#fff', fontSize: '0.82rem', fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },
  primaryBtnSmall: {
    padding: '0.3rem 0.7rem', borderRadius: 6, border: 'none', background: 'var(--accent)',
    color: '#fff', fontSize: '0.75rem', fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },
  dangerBtn: {
    padding: '0.3rem 0.7rem', borderRadius: 6, border: '1px solid var(--error)', background: 'transparent',
    color: 'var(--error)', fontSize: '0.75rem', fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },
  statusCategoryHeading: {
    margin: '1.2rem 0 0.4rem', fontSize: '0.9rem', color: 'var(--muted)',
    textTransform: 'uppercase', letterSpacing: '0.03em', fontWeight: 700,
  },

  errorBanner: {
    marginTop: '0.6rem', padding: '0.5rem 0.75rem', borderRadius: 8,
    background: 'rgba(220,38,38,0.08)', border: '1px solid var(--error)', color: 'var(--error)', fontSize: '0.8rem',
  },
  errorInline: { color: 'var(--error)', fontSize: '0.7rem', marginTop: 4 },
  muted: { color: 'var(--text-secondary)', fontSize: '0.82rem' },

  rowsBox: { marginTop: '0.75rem', display: 'flex', flexDirection: 'column', gap: 6 },
  row: { padding: '0.45rem 0.65rem', borderRadius: 7, background: 'var(--bg)', border: '1px solid var(--border)' },
  rowHead: { display: 'flex', justifyContent: 'space-between', gap: 10, flexWrap: 'wrap' },
  rowLabel: { fontSize: '0.75rem', fontWeight: 700, color: 'var(--accent)', fontFamily: 'ui-monospace, SFMono-Regular, monospace' },
  rowExplainer: { fontSize: '0.7rem', color: 'var(--text-secondary)', fontStyle: 'italic' },
  rowValue: {
    fontSize: '0.8rem', color: 'var(--text-primary)', marginTop: 3, wordBreak: 'break-all',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
  },

  table: { width: '100%', borderCollapse: 'collapse', marginTop: '0.6rem', fontSize: '0.78rem' },
  th: {
    textAlign: 'left', padding: '0.4rem 0.5rem', borderBottom: '1.5px solid var(--border)',
    color: 'var(--text-secondary)', fontWeight: 700, fontSize: '0.7rem', letterSpacing: '0.03em', textTransform: 'uppercase',
  },
  td: { padding: '0.4rem 0.5rem', borderBottom: '1px solid var(--border)', color: 'var(--text-primary)' },
  tdMono: {
    padding: '0.4rem 0.5rem', borderBottom: '1px solid var(--border)', color: 'var(--text-primary)',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '0.74rem',
  },

  schemaNote: {
    marginTop: '0.9rem', marginBottom: '0.9rem', fontSize: '0.8rem',
    color: 'var(--text-secondary)', display: 'flex', flexDirection: 'column', gap: 6,
  },

  diagramWrap: { display: 'flex', justifyContent: 'center', padding: '0.5rem' },
  diagramSvg: { width: '100%', maxWidth: 900, borderRadius: 12, border: '1px solid var(--border)' },

  // ── Sticky-header scroll pattern for the Architecture Diagram tab ──────────
  // flex-shrink:0 header + flex:1;overflow-y:auto body — the header stays
  // outside the <svg> entirely (plain HTML), so position:sticky is never
  // attempted inside SVG's own coordinate system.
  diagramTabWrap: { flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' },
  diagramHeader: {
    flexShrink: 0, padding: '0.85rem 1.25rem', borderBottom: '1px solid var(--border)',
    background: 'var(--surface)',
  },
  diagramHeaderTitle: { margin: 0, fontSize: '0.95rem', fontWeight: 700, color: 'var(--text-primary)' },
  diagramHeaderSub: { margin: '4px 0 0', fontSize: '0.78rem', color: 'var(--text-secondary)' },
  diagramScrollBody: { flex: 1, overflowY: 'auto', padding: '1rem' },

  // ── Flows tab — sequence diagram + plain numbered list per flow ────────────
  seqWrap: { margin: '0.85rem 0 0', display: 'flex', justifyContent: 'center' },
  seqSvg: { width: '100%', maxWidth: 860, borderRadius: 10, border: '1px solid var(--border)' },
  mermaidWrap: { width: '100%', maxWidth: 900, borderRadius: 10, border: '1px solid var(--border)', background: '#0f1117', padding: '0.5rem', overflowX: 'auto' },
  flowStepList: { listStyle: 'none', margin: '0.75rem 0 0', padding: 0, display: 'flex', flexDirection: 'column', gap: '0.6rem' },
  flowStep: { display: 'flex', gap: '0.65rem', fontSize: '0.85rem', color: 'var(--text-primary)', lineHeight: 1.5 },
  flowStepNum: { flexShrink: 0, fontWeight: 700, fontFamily: 'ui-monospace, SFMono-Regular, monospace', minWidth: 22 },
};
