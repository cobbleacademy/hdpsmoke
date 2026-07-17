import { useState, useEffect, useCallback } from 'react';

// ── This service is fully independent of this app's backend ──────────────────
// Istio routes /api/sensec/hsm/v1/* directly to the HSM encryption service's own
// pod. No Express proxy here — see vite.config.js for the local-dev-only proxy.
const HSM_BASE = '/api/sensec/hsm/v1';

const ALL_SCOPES = ['encrypt', 'decrypt', 'rotate', 'grant', 'manage_apps'];

const ENCRYPT_FIELD_EXPLAINERS = {
  edek_id:      'Reference to the wrapped data key — never the key itself',
  owner_app_id: "Bound into the AES-GCM tag as AAD; decrypt fails if this doesn't match",
  algorithm:    'Cipher used — persisted per-record for future algorithm migrations',
  encoding:     'utf8 vs base64 — tells the caller how to interpret plaintext later',
  iv_b64:       'Random per call — same plaintext never produces the same ciphertext twice',
  tag_b64:      "GCM auth tag — proves ciphertext and owner_app_id weren't tampered with",
  kek_version:  'Which HSM master key version wrapped this record',
};

const ENCRYPT_FIELD_ORDER = ['edek_id', 'owner_app_id', 'algorithm', 'encoding', 'iv_b64', 'tag_b64', 'kek_version'];

// ── Fetch helper — attaches Authorization + X-App-ID when an app is given ────
async function callApi(path, { method = 'GET', body, app } = {}) {
  const headers = { 'Content-Type': 'application/json' };
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
      <svg viewBox="0 0 1180 900" xmlns="http://www.w3.org/2000/svg" role="img" style={s.diagramSvg}>
        <title>HSM Encryption Service Architecture — Subscription-Isolated, 2-SPN Split</title>
        <desc>Two independent Azure subscriptions: the Sensec HSM Service subscription (Encryption Service, Redis DEK cache, Azure Key Vault holding the CEK, a separate Managed HSM holding the KEK, EDEK Store, Grants+Rotation) and a separate Governance/Audit subscription (Auditor SPN), which reaches into the HSM subscription only via read-only cross-subscription RBAC — never through the Encryption Service. PlainID is a shared PBAC decision point consulted independently by both the Client (before it ever calls the Service) and the Service itself (as its own authorization step) — neither call is proxied through the other; PlainID never forwards a decision from one caller to the other. All JWTs (to PlainID and to the Service) are app-level only, never per-end-user; every end-user operation instead carries an explicit end_user_id field. Post-unwrap DEKs are cached in Redis, encrypted with a Cache Encryption Key (CEK) held in Azure Key Vault (a distinct resource from the Managed HSM housing the KEK) — each pod runs a DEKCache that independently reads the CEK via Workload Identity and supports hot-reload rotation with dual-read/backfill against versioned Redis keys.</desc>

        <rect width="1180" height="900" fill="#0f1117" />

        {/* ── Callers ── */}
        <rect x="20" y="30" width="160" height="210" rx="8" fill="#1a1d27" stroke="#2d3148" strokeWidth="1.5" />
        <text x="100" y="50" textAnchor="middle" fill="#8b92b8" fontSize="10" letterSpacing="1" fontFamily="monospace">CALLERS (N CLIENTS)</text>
        <rect x="35" y="60" width="130" height="34" rx="5" fill="#22263a" stroke="#3b82f6" strokeWidth="1" />
        <text x="100" y="82" textAnchor="middle" fill="#3b82f6" fontFamily="monospace">App A</text>
        <rect x="35" y="104" width="130" height="34" rx="5" fill="#22263a" stroke="#3b82f6" strokeWidth="1" />
        <text x="100" y="126" textAnchor="middle" fill="#3b82f6" fontFamily="monospace">App B</text>
        <rect x="35" y="148" width="130" height="34" rx="5" fill="#22263a" stroke="#3b82f6" strokeWidth="1" />
        <text x="100" y="170" textAnchor="middle" fill="#3b82f6" fontFamily="monospace">App C …</text>
        <text x="100" y="200" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Bearer JWT (App-ID) +</text>
        <text x="100" y="212" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">end_user_id (request field)</text>
        <text x="100" y="224" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">+ App-ID header</text>

        {/* ── PlainID — SHARED PBAC, consulted independently by Client and
            Service. Neither call is proxied through the other; this is two
            separate PDP decisions against the same policy engine, not
            middleware in the request path of either caller. ── */}
        <rect x="210" y="30" width="190" height="210" rx="8" fill="#1a1d27" stroke="#f59e0b" strokeWidth="1.5" />
        <text x="305" y="50" textAnchor="middle" fill="#f59e0b" fontSize="9" letterSpacing="0.5" fontFamily="monospace">PLAINID (SHARED PBAC)</text>
        <text x="305" y="62" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">independent PDP calls — not a proxy</text>

        <rect x="225" y="70" width="160" height="20" rx="4" fill="#22263a" />
        <text x="305" y="83" textAnchor="middle" fill="#cdd2f0" fontSize="8" fontFamily="monospace">Evaluate App-ID + end_user_id</text>

        <rect x="225" y="94" width="160" height="20" rx="4" fill="#78350f" />
        <text x="305" y="107" textAnchor="middle" fill="#fbbf24" fontSize="8" fontFamily="monospace">Client: Permit/Deny → Client</text>

        <rect x="225" y="118" width="160" height="20" rx="4" fill="#083344" stroke="#22d3ee" strokeWidth="1" />
        <text x="305" y="131" textAnchor="middle" fill="#67e8f9" fontSize="8" fontFamily="monospace">Service: Permit/Deny → own authz</text>

        <text x="305" y="152" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">Service never sees Client's decision</text>
        <text x="305" y="163" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">and vice-versa — two PDP calls,</text>
        <text x="305" y="174" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">not a shared/forwarded one</text>

        <line x1="180" y1="70" x2="210" y2="80" stroke="#f59e0b" strokeWidth="1.2" markerEnd="url(#arr-amber)" />
        <line x1="210" y1="100" x2="180" y2="110" stroke="#f59e0b" strokeWidth="1.2" markerEnd="url(#arr-amber)" />
        <text x="195" y="192" textAnchor="middle" fill="#f59e0b" fontSize="7" fontFamily="monospace" transform="rotate(-90 195 192)">client pre-check</text>

        {/* Direct client → service call — independent of the PlainID hop above */}
        <line x1="180" y1="220" x2="450" y2="220" stroke="#3b82f6" strokeWidth="1.5" markerEnd="url(#arr-blue)" />
        <text x="315" y="214" textAnchor="middle" fill="#3b82f6" fontSize="9" fontFamily="monospace">HTTPS/TLS — JWT (App-ID) + end_user_id, direct</text>

        {/* ── HSM Service subscription boundary ── */}
        <rect x="430" y="10" width="740" height="555" rx="10" fill="none" stroke="#555b7a" strokeWidth="1.5" strokeDasharray="7,4" />
        <text x="446" y="26" fill="#8b92b8" fontSize="8" letterSpacing="0.5" fontFamily="monospace">SUBSCRIPTION BOUNDARY — SENSEC HSM SERVICE (own Azure subscription)</text>

        <rect x="450" y="40" width="200" height="360" rx="8" fill="#1a1d27" stroke="#a78bfa" strokeWidth="2" />
        <text x="550" y="62" textAnchor="middle" fill="#a78bfa" fontSize="10" letterSpacing="1" fontFamily="monospace">ENCRYPTION SERVICE</text>
        <text x="550" y="76" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">FastAPI · /api/sensec/hsm/v1</text>

        <rect x="465" y="83" width="170" height="22" rx="4" fill="#22263a" stroke="#fbbf24" strokeWidth="1" />
        <text x="550" y="97" textAnchor="middle" fill="#fbbf24" fontSize="8" fontFamily="monospace">JWT (App-ID) + end_user_id Check</text>

        <rect x="465" y="109" width="170" height="20" rx="4" fill="#083344" stroke="#22d3ee" strokeWidth="1" />
        <text x="550" y="122" textAnchor="middle" fill="#67e8f9" fontSize="8" fontFamily="monospace">→ own PlainID PBAC call</text>

        <rect x="465" y="133" width="170" height="50" rx="5" fill="#22263a" stroke="#10b981" strokeWidth="1" />
        <text x="550" y="151" textAnchor="middle" fill="#10b981" fontSize="10" fontFamily="monospace">POST /encrypt</text>
        <text x="550" y="166" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Gen DEK → AES-256-GCM</text>
        <text x="550" y="178" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">random IV → wrap DEK</text>

        <rect x="465" y="187" width="170" height="50" rx="5" fill="#22263a" stroke="#f87171" strokeWidth="1" />
        <text x="550" y="205" textAnchor="middle" fill="#f87171" fontSize="10" fontFamily="monospace">POST /decrypt</text>
        <text x="550" y="220" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Grant check → unwrap</text>
        <text x="550" y="232" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">AES-256-GCM decrypt</text>

        <rect x="465" y="241" width="170" height="38" rx="5" fill="#22263a" stroke="#fb923c" strokeWidth="1" />
        <text x="550" y="259" textAnchor="middle" fill="#fb923c" fontSize="10" fontFamily="monospace">/admin/rotate-kek · /grants</text>
        <text x="550" y="272" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Re-wrap EDEKs · manage grants</text>

        <rect x="465" y="283" width="170" height="30" rx="5" fill="#22263a" stroke="#64748b" strokeWidth="1" />
        <text x="550" y="302" textAnchor="middle" fill="#94a3b8" fontSize="10" fontFamily="monospace">GET /health · /apps/status</text>

        <rect x="465" y="317" width="170" height="22" rx="4" fill="#1e3a2f" stroke="#10b981" strokeWidth="1" />
        <text x="550" y="332" textAnchor="middle" fill="#10b981" fontSize="9" fontFamily="monospace">FIPS 140-2 Level 3 · AES-256-GCM</text>

        <rect x="465" y="343" width="170" height="18" rx="4" fill="#78350f" />
        <text x="550" y="355" textAnchor="middle" fill="#fbbf24" fontSize="7" fontFamily="monospace">Audit tags: end_user_id / operator_id</text>

        {/* Service → PlainID — its own independent PDP call, dashed to
            distinguish from the solid client→service call above. */}
        <path d="M 465,119 L 400,119 L 400,128 L 385,128" fill="none" stroke="#22d3ee" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-teal)" />
        <path d="M 385,118 L 400,118 L 400,110 L 465,110" fill="none" stroke="#22d3ee" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-teal)" />

        {/* ── Redis Cache — with CEK hot-reload detail ── */}
        <rect x="700" y="40" width="230" height="200" rx="8" fill="#1a1d27" stroke="#22d3ee" strokeWidth="2" />
        <text x="815" y="60" textAnchor="middle" fill="#22d3ee" fontSize="10" letterSpacing="1" fontFamily="monospace">REDIS CACHE</text>
        <text x="815" y="72" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">post-unwrap DEK cache</text>

        <rect x="715" y="80" width="200" height="20" rx="4" fill="#083344" stroke="#22d3ee" strokeWidth="1" />
        <text x="815" y="94" textAnchor="middle" fill="#67e8f9" fontSize="8" fontFamily="monospace">AES-256-GCM(CEK, DEK)</text>

        <rect x="715" y="104" width="200" height="20" rx="4" fill="#2d1b47" stroke="#fbbf24" strokeWidth="1" />
        <text x="815" y="118" textAnchor="middle" fill="#fbbf24" fontSize="8" fontFamily="monospace">key: dek:{'{'}ver{'}'}:{'{'}edek_id{'}'}</text>

        <rect x="715" y="128" width="200" height="20" rx="4" fill="#083344" stroke="#22d3ee" strokeWidth="1" />
        <text x="815" y="142" textAnchor="middle" fill="#67e8f9" fontSize="8" fontFamily="monospace">CEK Hot-Reload — DEKCache.rotate()</text>

        <rect x="715" y="152" width="200" height="30" rx="4" fill="#22263a" />
        <text x="815" y="164" textAnchor="middle" fill="#cdd2f0" fontSize="7" fontFamily="monospace">dual-read old ver(s) on miss</text>
        <text x="815" y="176" textAnchor="middle" fill="#cdd2f0" fontSize="7" fontFamily="monospace">then backfill @ current ver</text>

        <text x="815" y="196" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">see CEK HOT-RELOAD DESIGN panel below</text>

        <line x1="650" y1="90" x2="700" y2="90" stroke="#22d3ee" strokeWidth="1.5" markerEnd="url(#arr-teal)" />
        <text x="675" y="82" textAnchor="middle" fill="#22d3ee" fontSize="8" fontFamily="monospace">cache check / write</text>

        {/* ── Azure Key Vault — general purpose, holds only the CEK.
            Deliberately a separate resource from the Managed HSM below. ── */}
        <rect x="950" y="40" width="200" height="100" rx="8" fill="#1a1d27" stroke="#e879f9" strokeWidth="2" />
        <text x="1050" y="60" textAnchor="middle" fill="#e879f9" fontSize="10" letterSpacing="1" fontFamily="monospace">AZURE KEY VAULT</text>
        <text x="1050" y="72" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">general purpose · Standard tier</text>
        <rect x="965" y="80" width="170" height="24" rx="4" fill="#083344" stroke="#22d3ee" strokeWidth="1" />
        <text x="1050" y="96" textAnchor="middle" fill="#67e8f9" fontSize="8" fontFamily="monospace">CEK (AES-256) — cache-only</text>
        <text x="1050" y="118" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">SPN-read · no HSM boundary</text>
        <text x="1050" y="130" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">distinct resource from Managed HSM</text>

        <line x1="650" y1="115" x2="950" y2="80" stroke="#e879f9" strokeWidth="1" strokeDasharray="3,3" markerEnd="url(#arr-pink)" />
        <text x="800" y="103" textAnchor="middle" fill="#e879f9" fontSize="7" fontFamily="monospace">CEK fetch — startup, per-pod (Workload Identity)</text>

        {/* ── Managed HSM — holds only the KEK, its own dedicated resource ── */}
        <rect x="950" y="160" width="200" height="140" rx="8" fill="#1a1d27" stroke="#a78bfa" strokeWidth="2" />
        <text x="1050" y="180" textAnchor="middle" fill="#a78bfa" fontSize="10" letterSpacing="1" fontFamily="monospace">MANAGED HSM</text>
        <text x="1050" y="192" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">Azure Key Vault HSM · FIPS 140-2 L3</text>

        <rect x="965" y="200" width="170" height="30" rx="4" fill="#2d1b47" stroke="#a78bfa" strokeWidth="1" />
        <text x="1050" y="215" textAnchor="middle" fill="#e9d5ff" fontSize="10" fontFamily="monospace">KEK (RSA-HSM 4096)</text>
        <text x="1050" y="227" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">never leaves HSM boundary</text>

        <rect x="965" y="238" width="170" height="22" rx="4" fill="#22263a" />
        <text x="1050" y="253" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Key Versioning</text>

        <rect x="965" y="266" width="170" height="22" rx="4" fill="#22263a" />
        <text x="1050" y="281" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Auto-rotation Policy</text>

        <line x1="650" y1="210" x2="950" y2="220" stroke="#a78bfa" strokeWidth="1.5" markerEnd="url(#arr-purple)" />
        <text x="800" y="205" textAnchor="middle" fill="#a78bfa" fontSize="9" fontFamily="monospace">wrap/unwrap</text>
        <rect x="768" y="212" width="64" height="16" rx="4" fill="#422006" stroke="#fbbf24" strokeWidth="1.2" />
        <text x="800" y="223" textAnchor="middle" fill="#fbbf24" fontSize="8" fontFamily="monospace" fontWeight="bold">SVC SPN</text>

        {/* ── Grants + Rotation ── */}
        <rect x="700" y="260" width="230" height="120" rx="8" fill="#1a1d27" stroke="#fb923c" strokeWidth="1.5" />
        <text x="815" y="280" textAnchor="middle" fill="#fb923c" fontSize="10" letterSpacing="1" fontFamily="monospace">GRANTS + ROTATION</text>
        <rect x="715" y="288" width="200" height="22" rx="4" fill="#22263a" />
        <text x="815" y="303" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">grantee → owner pairs</text>
        <rect x="715" y="316" width="200" height="22" rx="4" fill="#22263a" />
        <text x="815" y="331" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">default-deny, audited</text>
        <rect x="715" y="344" width="200" height="20" rx="4" fill="#22263a" />
        <text x="815" y="358" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">monthly KEK re-wrap job</text>

        <line x1="930" y1="270" x2="950" y2="290" stroke="#fb923c" strokeWidth="1.5" markerEnd="url(#arr-orange)" />
        <line x1="700" y1="320" x2="650" y2="260" stroke="#fb923c" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-orange)" />

        {/* ── EDEK Store ── */}
        <rect x="450" y="420" width="200" height="130" rx="8" fill="#1a1d27" stroke="#38bdf8" strokeWidth="1.5" />
        <text x="550" y="442" textAnchor="middle" fill="#38bdf8" fontSize="10" letterSpacing="1" fontFamily="monospace">EDEK STORE</text>
        <text x="550" y="456" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">PostgreSQL</text>

        <rect x="465" y="464" width="170" height="22" rx="4" fill="#22263a" />
        <text x="550" y="479" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">edek_id · blob · owner app_id</text>

        <rect x="465" y="492" width="170" height="22" rx="4" fill="#22263a" />
        <text x="550" y="507" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">algorithm · encoding · class.</text>

        <rect x="465" y="520" width="170" height="14" rx="3" fill="#22263a" />
        <text x="550" y="531" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">encrypted at rest · TDE</text>

        <line x1="550" y1="400" x2="550" y2="420" stroke="#38bdf8" strokeWidth="1.5" markerEnd="url(#arr-cyan)" />

        {/* ── Encrypt/Decrypt payload flow reference ── */}
        <rect x="10" y="590" width="430" height="260" rx="8" fill="#1a1d27" stroke="#2d3148" strokeWidth="1.5" />
        <text x="225" y="610" textAnchor="middle" fill="#8b92b8" fontSize="10" letterSpacing="1" fontFamily="monospace">ENCRYPT / DECRYPT PAYLOAD FLOW</text>

        <text x="26" y="630" fill="#10b981" fontSize="10" fontFamily="monospace">Encrypt:</text>
        <rect x="26" y="638" width="395" height="60" rx="4" fill="#22263a" />
        <text x="38" y="652" fill="#555b7a" fontSize="8" fontFamily="monospace">Request:  {'{'} plaintext, encoding, ..., end_user_id {'}'}</text>
        <text x="38" y="666" fill="#555b7a" fontSize="8" fontFamily="monospace">Generate:  DEK = random_bytes(32)   IV = random_bytes(12)</text>
        <text x="38" y="680" fill="#555b7a" fontSize="8" fontFamily="monospace">Cipher  =  AES-256-GCM(DEK, IV, plaintext, AAD=owner_app_id)</text>
        <text x="38" y="692" fill="#555b7a" fontSize="8" fontFamily="monospace">EDEK    =  KEK.wrap(DEK) via Managed HSM → EDEK Store</text>

        <text x="26" y="712" fill="#cdd2f0" fontSize="8" fontFamily="monospace">Response: {'{'} edek_id, owner_app_id, algorithm, iv, ciphertext, tag {'}'}</text>
        <text x="26" y="726" fill="#fbbf24" fontSize="7" fontFamily="monospace">end_user_id: explicit request field (not JWT, not AAD)</text>

        <text x="26" y="746" fill="#f87171" fontSize="10" fontFamily="monospace">Decrypt:</text>
        <rect x="26" y="754" width="395" height="44" rx="4" fill="#22263a" />
        <text x="38" y="768" fill="#555b7a" fontSize="8" fontFamily="monospace">Request:   {'{'} edek_id, iv, ciphertext, tag, end_user_id {'}'}</text>
        <text x="38" y="782" fill="#555b7a" fontSize="8" fontFamily="monospace">Lookup owner → grant → PlainID → cache/HSM.unwrap → decrypt</text>
        <text x="38" y="794" fill="#555b7a" fontSize="8" fontFamily="monospace">Response:  {'{'} plaintext, owner_app_id {'}'}  DEK zeroed</text>

        <text x="26" y="816" fill="#22d3ee" fontSize="7" fontFamily="monospace">Cache: DEK encrypted w/ CEK → Redis(dek:{'{'}ver{'}'}:{'{'}edek_id{'}'}) — next decrypt skips HSM</text>
        <text x="26" y="828" fill="#78716c" fontSize="7" fontFamily="monospace">Split HSM: Managed HSM = KEK only · Azure Key Vault = CEK only</text>
        <text x="26" y="840" fill="#78716c" fontSize="7" fontFamily="monospace">Auditor SPN: cross-subscription, never through this service</text>

        {/* ── CEK Hot-Reload design — plain-text summary, not literal code.
            Every fact here is also stated in the Redis Cache box above and
            in Decrypt flow steps 20-22 (Flows tab); this panel exists only
            to spell those same facts out in one place, not to introduce new
            behavior, so it stays prose rather than a pseudocode block. ── */}
        <rect x="460" y="590" width="460" height="200" rx="8" fill="#1a1d27" stroke="#22d3ee" strokeWidth="1.5" />
        <text x="690" y="610" textAnchor="middle" fill="#22d3ee" fontSize="10" letterSpacing="1" fontFamily="monospace">CEK HOT-RELOAD DESIGN</text>
        <text x="690" y="622" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">DEKCache — per-pod, in-memory CEK + Redis-backed DEK cache</text>

        <rect x="476" y="630" width="428" height="150" rx="4" fill="#22263a" />
        <text x="488" y="646" fill="#cdd2f0" fontSize="8" fontFamily="monospace">Constructed once per pod with a Redis client, a Key</text>
        <text x="488" y="658" fill="#cdd2f0" fontSize="8" fontFamily="monospace">Vault client, the CEK secret name, and a refresh interval.</text>
        <text x="488" y="674" fill="#fbbf24" fontSize="8" fontFamily="monospace">rotate(new_version) — atomic in-memory CEK swap,</text>
        <text x="488" y="686" fill="#fbbf24" fontSize="8" fontFamily="monospace">no service restart.</text>
        <text x="488" y="702" fill="#67e8f9" fontSize="8" fontFamily="monospace">Redis key: dek:{'{'}version{'}'}:{'{'}edek_id{'}'} — a retired CEK</text>
        <text x="488" y="714" fill="#67e8f9" fontSize="8" fontFamily="monospace">version is never silently misread as current.</text>
        <text x="488" y="730" fill="#cdd2f0" fontSize="8" fontFamily="monospace">On a miss under the current version: dual-read the last</text>
        <text x="488" y="742" fill="#cdd2f0" fontSize="8" fontFamily="monospace">N prior versions' keys, decrypt with the matching</text>
        <text x="488" y="754" fill="#cdd2f0" fontSize="8" fontFamily="monospace">historical CEK, and backfill under the current version.</text>
        <text x="488" y="770" fill="#78716c" fontSize="7" fontFamily="monospace">Net effect: zero-downtime rotation, no bulk re-encrypt job.</text>

        {/* ── Auditor subscription boundary — pushed to the far right, a
            genuinely separate Azure subscription from the HSM Service; the
            only paths in are two read-only cross-subscription RBAC arrows,
            never through the Encryption Service. ── */}
        <rect x="940" y="590" width="230" height="230" rx="10" fill="none" stroke="#555b7a" strokeWidth="1.5" strokeDasharray="7,4" />
        <text x="956" y="606" fill="#8b92b8" fontSize="8" letterSpacing="0.5" fontFamily="monospace">SUBSCRIPTION BOUNDARY — GOVERNANCE / AUDIT</text>

        <rect x="955" y="620" width="200" height="185" rx="8" fill="#1a1d27" stroke="#f43f5e" strokeWidth="1.5" />
        <text x="1055" y="640" textAnchor="middle" fill="#f43f5e" fontSize="10" letterSpacing="1" fontFamily="monospace">AUDITOR SPN</text>
        <text x="1055" y="652" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">read-only · own subscription</text>

        <rect x="970" y="662" width="170" height="22" rx="4" fill="#22263a" />
        <text x="1055" y="676" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">KV Metadata (read-only)</text>

        <rect x="970" y="690" width="170" height="22" rx="4" fill="#22263a" />
        <text x="1055" y="704" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">EDEK Store (read-only)</text>

        <rect x="970" y="718" width="170" height="20" rx="4" fill="#3f1220" stroke="#f43f5e" strokeWidth="1" />
        <text x="1055" y="731" textAnchor="middle" fill="#fca5a5" fontSize="8" fontFamily="monospace">→ KV Diagnostic Logs</text>

        <rect x="970" y="742" width="170" height="20" rx="4" fill="#3f1220" stroke="#f43f5e" strokeWidth="1" />
        <text x="1055" y="755" textAnchor="middle" fill="#fca5a5" fontSize="7" fontFamily="monospace">Governance Scope: BYPASS</text>

        <text x="1055" y="775" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">no identity or network path</text>
        <text x="1055" y="786" textAnchor="middle" fill="#78716c" fontSize="7" fontFamily="monospace">through Encryption Service</text>

        {/* Cross-subscription bypass — dashed, routed through the gap
            between the HSM subscription boundary (bottom edge y=565) and
            the bottom row of panels (top edge y=590) to reach Managed HSM
            and EDEK Store without ever touching the Encryption Service. */}
        <path d="M 1050,590 L 1050,300" fill="none" stroke="#f43f5e" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-red)" />
        <text x="1058" y="450" textAnchor="middle" fill="#f43f5e" fontSize="7" fontFamily="monospace" transform="rotate(-90 1058 450)">cross-subscription RBAC — KEK metadata (read-only)</text>

        <path d="M 955,577 L 550,577 L 550,550" fill="none" stroke="#f43f5e" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-red)" />
        <text x="750" y="573" textAnchor="middle" fill="#f43f5e" fontSize="7" fontFamily="monospace">cross-subscription RBAC — DB read-only scan</text>

        <defs>
          <marker id="arr-blue" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#3b82f6" />
          </marker>
          <marker id="arr-amber" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#f59e0b" />
          </marker>
          <marker id="arr-purple" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#a78bfa" />
          </marker>
          <marker id="arr-cyan" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#38bdf8" />
          </marker>
          <marker id="arr-teal" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#22d3ee" />
          </marker>
          <marker id="arr-pink" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#e879f9" />
          </marker>
          <marker id="arr-orange" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#fb923c" />
          </marker>
          <marker id="arr-red" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#f43f5e" />
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
const FLOWS = [
  {
    title: '1. Policy Check (Client pre-check)',
    color: '#f59e0b',
    steps: [
      'Client app authenticates as itself and obtains an app-level JWT (App-ID) — not a per-end-user token; the same app JWT is reused across all of that app\'s calls, for every end-user.',
      'Client sends a decision request to PlainID: its app JWT (App-ID) plus an explicit end_user_id field identifying which end-user the action is for. This exchange is entirely client-side — the Encryption Service is not involved and has no visibility into it.',
      'PlainID evaluates its PBAC policy against the App-ID and end_user_id for the requested action/resource class.',
      'If denied: PlainID returns Deny to the Client, which aborts — no call is ever made to the Encryption Service.',
      'If permitted: PlainID returns Permit, and the Client independently calls the Encryption Service directly (Encrypt or Decrypt below). The Service does not receive or trust this Permit — it performs its own, entirely separate PlainID check once the call arrives (see step 8 / step 17 below).',
    ],
    actors: [
      { id: 'client', label: 'Client' },
      { id: 'plainid', label: 'PlainID' },
    ],
    messages: [
      { from: 'client', to: 'plainid', label: 'App JWT (App-ID) + end_user_id → decision request', stepNum: '1–2' },
      { from: 'plainid', to: 'plainid', self: true, label: 'Evaluate PBAC policy', stepNum: 3 },
      { from: 'plainid', to: 'client', dashed: true, label: 'Deny → Client aborts, no call made', stepNum: 4 },
      { from: 'plainid', to: 'client', label: 'Permit → Client proceeds independently', stepNum: 5 },
    ],
  },
  {
    title: '2. Encrypt',
    color: '#10b981',
    steps: [
      'Client calls POST /encrypt directly (having independently obtained Permit from PlainID above), presenting its own app-level Bearer JWT (App-ID) + an explicit end_user_id field in the request body + App-ID header.',
      'Encryption Service validates the JWT to authenticate the App-ID, and reads the end_user_id field from the request.',
      'Encryption Service makes its OWN, independent call to PlainID — using its own identity, not the Client\'s — to authorize this specific App-ID + end_user_id for the encrypt operation. This is a second, unrelated PDP decision; the Service never sees or trusts the Client\'s earlier Permit. A Service-side deny rejects the request and logs to audit_log before any key material is touched.',
      'Encryption Service generates a new Data Encryption Key (DEK) and a random IV.',
      'Encryption Service encrypts the plaintext locally using AES-256-GCM with the DEK.',
      'Encryption Service calls Managed HSM (the dedicated Azure Key Vault HSM resource holding the KEK — a separate resource from the Azure Key Vault below) — using its Service SPN — to wrap (encrypt) the DEK with the current KEK, producing an EDEK.',
      'Encryption Service persists the EDEK plus metadata (owner app_id, algorithm, key version) to the EDEK Store (PostgreSQL).',
      'Encryption Service encrypts the just-generated DEK a second time, locally, using the Cache Encryption Key (CEK) held in the separate Azure Key Vault resource and loaded by the pod\'s DEKCache at startup — and writes the result to Redis under the versioned key dek:{ver}:{edek_id}, where {ver} is the CEK version. This lets a decrypt of the same edek_id skip the HSM round-trip entirely (see Decrypt below).',
      'Encryption Service returns the ciphertext + edek_id to the client; the action is recorded in audit_log — tagged with both the Client (App-ID) and the end_user_id field from the request, since the call itself arrives under the Client SPN and wouldn\'t otherwise be attributable to a specific end-user.',
    ],
    actors: [
      { id: 'client', label: 'Client' },
      { id: 'service', label: 'Encryption Svc' },
      { id: 'plainid', label: 'PlainID' },
      { id: 'hsm', label: 'Managed HSM' },
      { id: 'edek', label: 'EDEK Store' },
      { id: 'redis', label: 'Redis Cache' },
    ],
    messages: [
      { from: 'client', to: 'service', label: 'POST /encrypt — JWT (App-ID) + end_user_id (direct)', stepNum: 6 },
      { from: 'service', to: 'service', self: true, label: 'Validate JWT (App-ID), read end_user_id field', stepNum: 7 },
      { from: 'service', to: 'plainid', dashed: true, label: 'own independent PBAC check (Service identity)', stepNum: 8 },
      { from: 'plainid', to: 'service', dashed: true, label: 'Permit/Deny → Service (not forwarded from Client)', stepNum: 8 },
      { from: 'service', to: 'service', self: true, label: 'Gen DEK + IV, AES-256-GCM encrypt', stepNum: '9–10' },
      { from: 'service', to: 'hsm', label: 'wrap(DEK) — Service SPN', stepNum: 11 },
      { from: 'hsm', to: 'service', label: 'EDEK', stepNum: 11 },
      { from: 'service', to: 'edek', label: 'persist EDEK + metadata', stepNum: 12 },
      { from: 'service', to: 'redis', label: 'write dek:{ver}:{edek_id} = AES-256-GCM(CEK, DEK)', stepNum: 13 },
      { from: 'service', to: 'client', label: 'ciphertext + edek_id → audit_log (Client + User)', stepNum: 14 },
    ],
  },
  {
    title: '3. Decrypt',
    color: '#f87171',
    steps: [
      'Client calls POST /decrypt directly (having independently obtained Permit from PlainID above), presenting ciphertext + edek_id + its own app-level Bearer JWT (App-ID) + an explicit end_user_id field + App-ID header.',
      'Encryption Service validates the JWT to authenticate the App-ID, and reads the end_user_id field from the request.',
      'Encryption Service makes its OWN, independent call to PlainID for this decrypt operation — the same pattern as Encrypt step 8, a second unrelated PDP decision using the Service\'s own identity. This runs before the grant/ownership check below, and before the cache is ever consulted.',
      'Encryption Service looks up the EDEK record by edek_id in the EDEK Store, and verifies the requesting app_id matches the owner (or holds an active grant).',
      'If there\'s no ownership match or active grant: reject (403) and log the denial to audit_log (Client + end_user_id). A cached DEK is exactly as access-controlled as one freshly unwrapped from the HSM — this check always runs first.',
      'Encryption Service checks Redis for a cached DEK under the versioned key dek:{ver}:{edek_id}, where {ver} is the pod\'s current CEK version. On a cache hit, it decrypts the cached blob locally with the CEK and skips straight to the final decrypt step below — no HSM call at all.',
      'On a cache miss (e.g. the pod\'s current CEK version has no entry for this edek_id — most commonly right after a CEK rotation): the DEKCache dual-reads the last N prior CEK versions\' keys before giving up. A hit there is decrypted with the matching historical CEK and immediately backfilled under the current version — see the CEK Hot-Reload Design panel in the Architecture Diagram.',
      'If dual-read also misses: Encryption Service calls Managed HSM — using its Service SPN — to unwrap the EDEK back into the plaintext DEK, then writes the freshly-unwrapped DEK back into Redis (encrypted with the current CEK), so subsequent decrypts of this edek_id become cache hits.',
      'Encryption Service decrypts the ciphertext locally using the DEK (from cache, dual-read backfill, or the HSM) and the stored IV (AES-256-GCM).',
      'Encryption Service returns the plaintext to the client; the DEK is zeroed immediately, and the action is recorded in audit_log — tagged with both the Client (App-ID) and the end_user_id field from the request.',
    ],
    actors: [
      { id: 'client', label: 'Client' },
      { id: 'service', label: 'Encryption Svc' },
      { id: 'plainid', label: 'PlainID' },
      { id: 'edek', label: 'EDEK Store' },
      { id: 'redis', label: 'Redis Cache' },
      { id: 'hsm', label: 'Managed HSM' },
    ],
    messages: [
      { from: 'client', to: 'service', label: 'POST /decrypt — JWT (App-ID) + end_user_id (direct)', stepNum: 15 },
      { from: 'service', to: 'service', self: true, label: 'Validate JWT (App-ID), read end_user_id field', stepNum: 16 },
      { from: 'service', to: 'plainid', dashed: true, label: 'own independent PBAC check (Service identity)', stepNum: 17 },
      { from: 'plainid', to: 'service', dashed: true, label: 'Permit/Deny → Service (not forwarded from Client)', stepNum: 17 },
      { from: 'service', to: 'edek', label: 'lookup owner by edek_id', stepNum: 18 },
      { from: 'edek', to: 'service', label: 'owner_app_id, algorithm', stepNum: 18 },
      { from: 'service', to: 'client', dashed: true, label: '403 if no grant/ownership → audit_log', stepNum: 19 },
      { from: 'service', to: 'redis', label: 'GET dek:{ver}:{edek_id}', stepNum: 20 },
      { from: 'redis', to: 'service', dashed: true, label: 'hit → decrypt w/ CEK, skip HSM', stepNum: 20 },
      { from: 'service', to: 'hsm', label: 'dual-read miss → unwrap(EDEK) — Service SPN', stepNum: 21 },
      { from: 'hsm', to: 'service', label: 'DEK', stepNum: 21 },
      { from: 'service', to: 'redis', label: 'backfill dek:{ver}:{edek_id} = AES-256-GCM(CEK, DEK)', stepNum: 22 },
      { from: 'service', to: 'service', self: true, label: 'AES-256-GCM decrypt', stepNum: 23 },
      { from: 'service', to: 'client', label: 'plaintext (DEK zeroed) → audit_log (Client + User)', stepNum: 24 },
    ],
  },
  {
    title: '4. Audit / Scan (separate subscription)',
    color: '#f43f5e',
    steps: [
      'Auditor — a separate identity/tool in a wholly separate Azure subscription from the HSM Service, not an app client — authenticates directly to Azure AD using the Auditor SPN, scoped to read-only permissions only.',
      'Auditor calls Managed HSM directly — crossing the subscription boundary via read-only RBAC, never through the Encryption Service — to read KEK metadata: version history, rotation timestamps, and access policies.',
      'Every Auditor SPN call against Managed HSM is captured automatically by its own Diagnostic Logs (Azure Monitor/Log Analytics) — not the Encryption Service\'s audit_log, since the Auditor SPN never touches the service or its subscription\'s resources beyond this read.',
      'Auditor separately connects directly to the EDEK Store (PostgreSQL) using a read-only database role — again crossing the subscription boundary, never through the Encryption Service — to scan EDEK records (owner app_id, algorithm, key version, timestamps) for compliance reporting.',
      'Auditor cross-references Diagnostic Logs with the EDEK Store scan results to reconcile key usage and rotation compliance (e.g. flagging EDEKs still wrapped under a retired KEK version).',
      'Findings are compiled into a compliance/audit report — entirely outside the Encryption Service\'s own audit_log and PlainID/PBAC governance scope, and outside the HSM Service\'s subscription entirely, by design, so oversight isn\'t mediated (or potentially blocked) by the same subscription being audited.',
    ],
    actors: [
      { id: 'auditor', label: 'Auditor SPN' },
      { id: 'hsm', label: 'Managed HSM' },
      { id: 'edek', label: 'EDEK Store' },
    ],
    messages: [
      { from: 'auditor', to: 'auditor', self: true, label: 'Authenticate via Auditor SPN', stepNum: 25 },
      { from: 'auditor', to: 'hsm', dashed: true, label: 'cross-subscription RBAC — read KEK metadata', stepNum: 26 },
      { from: 'hsm', to: 'auditor', label: 'version history, rotation ts → Diagnostic Logs', stepNum: 27 },
      { from: 'auditor', to: 'edek', dashed: true, label: 'cross-subscription RBAC — read-only DB scan', stepNum: 28 },
      { from: 'edek', to: 'auditor', label: 'EDEK records', stepNum: 28 },
      { from: 'auditor', to: 'auditor', self: true, label: 'Reconcile logs vs scan', stepNum: 29 },
      { from: 'auditor', to: 'auditor', self: true, label: 'Compile compliance report', stepNum: 30 },
    ],
  },
];

// Generic sequence-diagram renderer — actors as lifelines, messages drawn
// top-to-bottom in order. Self-messages (self:true) render as a small loop
// back onto the same lifeline (internal processing, no other actor involved).
// Dashed messages (dashed:true) use the shared red marker regardless of the
// flow's own color, to visually flag deny/bypass paths consistently.
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
        const stroke = m.dashed ? '#f43f5e' : color;
        const marker = m.dashed ? `url(#${markerId}-deny)` : `url(#${markerId})`;
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
        <marker id={`${markerId}-deny`} markerWidth="7" markerHeight="6" refX="6" refY="3" orient="auto">
          <polygon points="0 0, 7 3, 0 6" fill="#f43f5e" />
        </marker>
      </defs>
    </svg>
  );
}

// ── End-to-end overview — condensed macro-view, not a 30-step merge ──────────
// Deliberately NOT all 24 individual messages: past ~8-10 arrows across 6
// lifelines a sequence diagram stops being readable. This shows the 4 flows
// as macro-phases instead, with Audit/Scan visually separated by a divider
// to make clear it's an independent, out-of-band process — not step 6 of
// the same request lifecycle as Policy Check → Encrypt/Decrypt.
function OverviewSequenceDiagram() {
  const width = 1000;
  const actors = [
    { id: 'client', label: 'Client' },
    { id: 'plainid', label: 'PlainID' },
    { id: 'service', label: 'Encryption Svc' },
    { id: 'hsm', label: 'Managed HSM' },
    { id: 'edek', label: 'EDEK Store' },
    { id: 'redis', label: 'Redis Cache' },
    { id: 'auditor', label: 'Auditor SPN' },
  ];
  const laneGap = width / (actors.length + 1);
  const xFor = (i) => laneGap * (i + 1);

  // PlainID is a SHARED PDP consulted independently by both the Client
  // (row 1-2, before it ever calls the Service) and the Service itself
  // (row 4, its own separate check once the call arrives) — two distinct
  // decisions, never one forwarded to the other. Row 6 condenses
  // Encrypt/Decrypt's cache read-or-write into one arrow — the per-flow
  // diagrams below spell out the versioned key + dual-read/backfill hit/miss
  // branch in full. Managed HSM (KEK) and the separate Azure Key Vault (CEK)
  // are two distinct resources — only Managed HSM appears here since the CEK
  // fetch happens once at pod startup, not per-request.
  const mainRows = [
    { from: 'client', to: 'plainid', label: '1. Client pre-check — App JWT (App-ID) + end_user_id', color: '#f59e0b' },
    { from: 'plainid', to: 'client', label: '2. Permit / Deny decision', color: '#f59e0b' },
    { from: 'client', to: 'service', label: '3. Encrypt or Decrypt — JWT (App-ID) + end_user_id (direct)', color: '#a78bfa' },
    { from: 'service', to: 'plainid', label: "4. Service's own independent PBAC check", color: '#22d3ee', dashed: true },
    { from: 'service', to: 'hsm', label: '5. wrap/unwrap (dual-read cache miss) — Service SPN', color: '#a78bfa' },
    { from: 'service', to: 'redis', label: '6. dek:{ver}:{edek_id} read/write — AES-256-GCM(CEK, DEK)', color: '#22d3ee' },
    { from: 'service', to: 'edek', label: '7. persist / lookup EDEK', color: '#38bdf8' },
    { from: 'service', to: 'client', label: '8. Result: ciphertext / plaintext', color: '#a78bfa' },
  ];
  const auditRows = [
    { from: 'auditor', to: 'hsm', label: '9. Audit: cross-subscription RBAC — KEK metadata (read-only)', color: '#f43f5e', dashed: true },
    { from: 'auditor', to: 'edek', label: '10. Audit: cross-subscription RBAC — DB scan (read-only)', color: '#f43f5e', dashed: true },
  ];

  const topY = 34;
  const rowH = 42;
  const dividerH = 32;
  const mainBottom = topY + 26 + mainRows.length * rowH;
  const dividerY = mainBottom + dividerH / 2;
  const auditTop = mainBottom + dividerH;
  const bottomY = auditTop + auditRows.length * rowH + 16;

  return (
    <svg viewBox={`0 0 ${width} ${bottomY + 12}`} style={s.seqSvg} role="img">
      <title>End-to-end overview across all four flows</title>
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

      <line x1="20" y1={dividerY} x2={width - 20} y2={dividerY} stroke="#555b7a" strokeWidth="1" strokeDasharray="6,4" />
      <rect x={width / 2 - 210} y={dividerY - 11} width="420" height="20" rx="4" fill="#0f1117" />
      <text x={width / 2} y={dividerY + 4} textAnchor="middle" fill="#78716c" fontSize="8" fontFamily="monospace">— independent / out-of-band — no path through Encryption Svc —</text>

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
          <p style={s.panelSub}>Macro view across all four flows — not a merge of all 30 steps (past ~8 arrows on 7 lifelines a sequence diagram stops being readable). See each flow below for the full detail.</p>
        </div>
        <div style={s.seqWrap}>
          <OverviewSequenceDiagram />
        </div>
      </section>

      {FLOWS.map((flow, flowIdx) => (
        <section key={flow.title} style={s.panel}>
          <div style={s.panelHead}>
            <h3 style={{ ...s.panelTitle, color: flow.color }}>{flow.title}</h3>
          </div>

          <div style={s.seqWrap}>
            <SequenceDiagram
              actors={flow.actors}
              messages={flow.messages}
              color={flow.color}
              markerId={`seq-arrow-${flowIdx}`}
            />
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
    return h === 'diagram' || h === 'flows' ? h : 'demo';
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
  const manageApp    = apps.find((a) => a.scopes?.includes('manage_apps')) || null;

  // ── Panel 2: Encrypt ─────────────────────────────────────────────────────────
  const [plaintext, setPlaintext]       = useState('');
  const [dataClass, setDataClass]       = useState('');
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
      body: { plaintext, encoding: 'utf8', data_classification: dataClass || null, end_user_id: encryptEndUserId.trim() || undefined },
    });
    setEncrypting(false);
    if (res.ok) {
      setEncryptResult(res.data);
      setDecryptForm({
        edekId:        res.data.edek_id,
        ivB64:         res.data.iv_b64,
        ciphertextB64: res.data.ciphertext_b64,
        tagB64:        res.data.tag_b64,
      });
      setDecryptResult(null);
      setDecryptError(null);
      loadEdekRecords();
    } else {
      setEncryptError(errMessage(res, 'Encrypt failed'));
    }
  }

  // ── Panel 3: Decrypt ─────────────────────────────────────────────────────────
  const [decryptForm, setDecryptForm] = useState({ edekId: '', ivB64: '', ciphertextB64: '', tagB64: '' });
  const [decryptResult, setDecryptResult] = useState(null);
  const [decryptError, setDecryptError]   = useState(null);
  const [decrypting, setDecrypting]       = useState(false);

  // ── Simulated Redis cache state (client-side only) ──────────────────────────
  // The external HSM service owns the real cache — this repo has no visibility
  // into whether a given decrypt was actually a cache hit or a cache miss on
  // the service side. cacheSeen is purely a local record of which edek_ids
  // have already been decrypted once in THIS browser session, used to render
  // a "simulated" hit/miss badge that illustrates the caching behavior without
  // claiming to reflect the real service's internal cache state.
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
    const edekId = decryptForm.edekId;
    const res = await callApi('/decrypt', {
      method: 'POST',
      app: selectedApp,
      body: {
        edek_id:        edekId,
        iv_b64:         decryptForm.ivB64,
        ciphertext_b64: decryptForm.ciphertextB64,
        tag_b64:        decryptForm.tagB64,
        end_user_id:    decryptEndUserId.trim() || undefined,
      },
    });
    setDecrypting(false);
    if (res.ok) {
      const simulatedHit = !!cacheSeen[edekId];
      setDecryptResult({ ...res.data, decrypted_as: selectedApp.app_id, end_user_id_sent: decryptEndUserId.trim() || null, cache_hit_simulated: simulatedHit });
      setCacheSeen((prev) => ({ ...prev, [edekId]: (prev[edekId] || 0) + 1 }));
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

  // ── Bonus panel: block / restore apps (manage_apps scope) ──────────────────
  const [appActive, setAppActive]           = useState({}); // app_id -> bool, locally tracked
  const [appStatusBusy, setAppStatusBusy]   = useState(null);
  const [appStatusError, setAppStatusError] = useState(null);

  async function handleToggleAppStatus(appId, nextActive) {
    if (!manageApp) return;
    setAppStatusBusy(appId);
    setAppStatusError(null);
    // Same pattern as Grants — always acts as whichever app holds "manage_apps".
    const res = await callApi('/admin/apps/status', {
      method: 'POST',
      app: manageApp,
      body: { app_id: appId, active: nextActive },
    });
    setAppStatusBusy(null);
    if (res.ok) {
      setAppActive((prev) => ({ ...prev, [appId]: res.data.active }));
    } else {
      setAppStatusError(errMessage(res, 'Could not update app status'));
    }
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
      body: { reveal_as: revealAsId },
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
              <label style={s.label}>End User ID</label>
              <input
                style={s.input}
                placeholder="end_user_id (optional)"
                value={encryptEndUserId}
                onChange={(e) => setEncryptEndUserId(e.target.value)}
              />
            </div>
            <div style={s.formRow}>
              <label style={s.label}>Data classification</label>
              <select style={s.select} value={dataClass} onChange={(e) => setDataClass(e.target.value)}>
                <option value="">none</option>
                <option value="pii">pii</option>
                <option value="pci">pci</option>
                <option value="internal">internal</option>
              </select>
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
                  explainer: ENCRYPT_FIELD_EXPLAINERS[key],
                }))}
              />
            )}
          </Panel>

          {/* Panel 3: Decrypt */}
          <Panel title="3. Decrypt" sub="edek_id/iv/ciphertext/tag auto-filled from the Encrypt response above. End User ID is independent of the Encrypt panel's — a decrypt is often performed on behalf of a different end-user than the one who originally encrypted the data (see Cross-App Decrypt Grants below).">
            <div style={s.formGrid}>
              <input style={s.input} placeholder="edek_id" value={decryptForm.edekId}
                onChange={(e) => setDecryptForm((f) => ({ ...f, edekId: e.target.value }))} />
              <input style={s.input} placeholder="iv_b64" value={decryptForm.ivB64}
                onChange={(e) => setDecryptForm((f) => ({ ...f, ivB64: e.target.value }))} />
              <input style={s.input} placeholder="ciphertext_b64" value={decryptForm.ciphertextB64}
                onChange={(e) => setDecryptForm((f) => ({ ...f, ciphertextB64: e.target.value }))} />
              <input style={s.input} placeholder="tag_b64" value={decryptForm.tagB64}
                onChange={(e) => setDecryptForm((f) => ({ ...f, tagB64: e.target.value }))} />
            </div>
            <div style={s.formRow}>
              <label style={s.label}>End User ID</label>
              <input
                style={s.input}
                placeholder="end_user_id (optional)"
                value={decryptEndUserId}
                onChange={(e) => setDecryptEndUserId(e.target.value)}
              />
            </div>
            <button style={s.primaryBtn} onClick={handleDecrypt} disabled={decrypting || !decryptForm.edekId}>
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
                    <tr><th style={s.th}>Grantee</th><th style={s.th}>Owner</th><th style={s.th}></th></tr>
                  </thead>
                  <tbody>
                    {grants.map((g) => (
                      <tr key={`${g.grantee_app_id}:${g.owner_app_id}`}>
                        <td style={s.td}>{g.grantee_app_id}</td>
                        <td style={s.td}>{g.owner_app_id}</td>
                        <td style={s.td}><button style={s.dangerBtn} onClick={() => handleRevokeGrant(g)}>Revoke</button></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </>
            )}
          </Panel>

          {/* Bonus panel: block/restore apps — same pattern as Grants */}
          <Panel title="App Access Control" sub="Always managed by whichever app holds the manage_apps scope. Blocking never affects existing grant checks.">
            {!manageApp ? (
              <p style={s.muted}>No demo app holds the "manage_apps" scope.</p>
            ) : (
              <>
                {appStatusError && <div style={s.errorBanner}>{appStatusError}</div>}
                <table style={s.table}>
                  <thead>
                    <tr><th style={s.th}>App</th><th style={s.th}>Status</th><th style={s.th}></th></tr>
                  </thead>
                  <tbody>
                    {apps.map((a) => {
                      const active = appActive[a.app_id] !== false;
                      return (
                        <tr key={a.app_id}>
                          <td style={s.td}>{a.app_id}</td>
                          <td style={s.td}>{active ? '🟢 active' : '🔴 blocked'}</td>
                          <td style={s.td}>
                            <button
                              style={active ? s.dangerBtn : s.primaryBtnSmall}
                              disabled={appStatusBusy === a.app_id}
                              onClick={() => handleToggleAppStatus(a.app_id, !active)}
                            >
                              {appStatusBusy === a.app_id ? '…' : active ? 'Block' : 'Restore'}
                            </button>
                          </td>
                        </tr>
                      );
                    })}
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
                  <th style={s.th}>Status</th><th style={s.th}>Wrapped Blob</th><th style={s.th}>Created</th>
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
                    <td style={s.td}>{r.rotation_status}</td>
                    <td style={s.tdMono}>{truncate(r.edek_blob_preview, 24)}</td>
                    <td style={s.td}>{fmtTime(r.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Panel>

          {/* Panel 7: Consumer application table */}
          <Panel title="7. Consumer Application Table" sub="Simulates payments-svc's own database — a separate schema from this service's EDEK store.">
            <div style={s.legend}>
              <span>customer_name / email — <code>VARCHAR</code> (non-sensitive)</span>
              <span>edek_id — <code>UUID</code></span>
              <span>iv_b64 — <code>CHAR(16)</code> (always exactly 16 chars)</span>
              <span>tag_b64 — <code>CHAR(24)</code> (always exactly 24 chars)</span>
              <span>ciphertext_b64 — <code>TEXT</code> (unbounded — scales with plaintext)</span>
            </div>

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
            </div>

            <table style={s.table}>
              <thead>
                <tr>
                  <th style={s.th}>ID</th><th style={s.th}>Customer</th><th style={s.th}>Email</th>
                  <th style={s.th}>Account Number</th><th style={s.th}>edek_id</th>
                  <th style={s.th}>iv_b64</th><th style={s.th}>tag_b64</th><th style={s.th}></th>
                </tr>
              </thead>
              <tbody>
                {accounts.map((acc) => (
                  <tr key={acc.id}>
                    <td style={s.td}>{acc.id}</td>
                    <td style={s.td}>{acc.customer_name}</td>
                    <td style={s.td}>{acc.email}</td>
                    <td style={s.tdMono}>
                      {revealed[acc.id] ? <strong style={{ color: 'var(--success)' }}>{revealed[acc.id]}</strong> : truncate(acc.account_number_ciphertext_preview, 18)}
                    </td>
                    <td style={s.tdMono}>{truncate(acc.edek_id, 12)}</td>
                    <td style={s.tdMono}>{acc.iv_b64}</td>
                    <td style={s.tdMono}>{acc.tag_b64}</td>
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
              <h3 style={s.diagramHeaderTitle}>HSM Encryption Service — Architecture</h3>
              <p style={s.diagramHeaderSub}>Cross-subscription 2-SPN split (HSM Service vs. Auditor) · shared PlainID PBAC (Client + Service) · KEK/CEK on separate resources · Redis DEK cache w/ CEK hot-reload</p>
            </div>
            <div style={s.diagramScrollBody}>
              <ArchitectureDiagram />
            </div>
          </div>
        )}

        {/* ── Tab 3: Flows — 30-step numbered sequence, plain text/cards ── */}
        {activeTab === 'flows' && (
          <div role="tabpanel" style={s.demoScroll}>
            <FlowsSequence />
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

  legend: {
    display: 'flex', flexDirection: 'column', gap: 3, marginBottom: '0.75rem',
    fontSize: '0.75rem', color: 'var(--text-secondary)', background: 'var(--bg)',
    border: '1px solid var(--border)', borderRadius: 8, padding: '0.6rem 0.75rem',
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
  flowStepList: { listStyle: 'none', margin: '0.75rem 0 0', padding: 0, display: 'flex', flexDirection: 'column', gap: '0.6rem' },
  flowStep: { display: 'flex', gap: '0.65rem', fontSize: '0.85rem', color: 'var(--text-primary)', lineHeight: 1.5 },
  flowStepNum: { flexShrink: 0, fontWeight: 700, fontFamily: 'ui-monospace, SFMono-Regular, monospace', minWidth: 22 },
};
