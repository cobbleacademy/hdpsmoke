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
      <svg viewBox="0 0 900 680" xmlns="http://www.w3.org/2000/svg" role="img" style={s.diagramSvg}>
        <title>HSM Encryption Service Architecture</title>
        <desc>Centralized encryption service using Azure Key Vault HSM with DEK/KEK envelope encryption pattern</desc>

        <rect width="900" height="680" fill="#0f1117" />

        <rect x="20" y="20" width="160" height="220" rx="8" fill="#1a1d27" stroke="#2d3148" strokeWidth="1.5" />
        <text x="100" y="40" textAnchor="middle" fill="#8b92b8" fontSize="10" letterSpacing="1" fontFamily="monospace">CALLERS</text>
        <rect x="35" y="50" width="130" height="34" rx="5" fill="#22263a" stroke="#3b82f6" strokeWidth="1" />
        <text x="100" y="72" textAnchor="middle" fill="#3b82f6" fontFamily="monospace">App A</text>
        <rect x="35" y="94" width="130" height="34" rx="5" fill="#22263a" stroke="#3b82f6" strokeWidth="1" />
        <text x="100" y="116" textAnchor="middle" fill="#3b82f6" fontFamily="monospace">App B</text>
        <rect x="35" y="138" width="130" height="34" rx="5" fill="#22263a" stroke="#3b82f6" strokeWidth="1" />
        <text x="100" y="160" textAnchor="middle" fill="#3b82f6" fontFamily="monospace">App C</text>
        <text x="100" y="205" textAnchor="middle" fill="#555b7a" fontSize="10" fontFamily="monospace">Bearer JWT</text>
        <text x="100" y="218" textAnchor="middle" fill="#555b7a" fontSize="10" fontFamily="monospace">+ App-ID header</text>

        <line x1="180" y1="130" x2="230" y2="130" stroke="#3b82f6" strokeWidth="1.5" markerEnd="url(#arr-blue)" />
        <text x="205" y="124" textAnchor="middle" fill="#3b82f6" fontSize="9" fontFamily="monospace">HTTPS/TLS</text>

        <rect x="230" y="60" width="160" height="140" rx="8" fill="#1a1d27" stroke="#f59e0b" strokeWidth="1.5" />
        <text x="310" y="80" textAnchor="middle" fill="#f59e0b" fontSize="10" letterSpacing="1" fontFamily="monospace">AUTH MIDDLEWARE</text>
        <rect x="245" y="90" width="130" height="24" rx="4" fill="#22263a" />
        <text x="310" y="106" textAnchor="middle" fill="#cdd2f0" fontSize="10" fontFamily="monospace">JWT Validation</text>
        <rect x="245" y="120" width="130" height="24" rx="4" fill="#22263a" />
        <text x="310" y="136" textAnchor="middle" fill="#cdd2f0" fontSize="10" fontFamily="monospace">App-ID + Grant Check</text>
        <rect x="245" y="150" width="130" height="24" rx="4" fill="#22263a" />
        <text x="310" y="166" textAnchor="middle" fill="#cdd2f0" fontSize="10" fontFamily="monospace">Scope Enforcement</text>
        <rect x="245" y="178" width="130" height="16" rx="4" fill="#78350f" />
        <text x="310" y="190" textAnchor="middle" fill="#fbbf24" fontSize="9" fontFamily="monospace">Audit Log → SIEM</text>

        <line x1="390" y1="130" x2="440" y2="130" stroke="#f59e0b" strokeWidth="1.5" markerEnd="url(#arr-amber)" />

        <rect x="440" y="20" width="200" height="280" rx="8" fill="#1a1d27" stroke="#a78bfa" strokeWidth="2" />
        <text x="540" y="42" textAnchor="middle" fill="#a78bfa" fontSize="10" letterSpacing="1" fontFamily="monospace">ENCRYPTION SERVICE</text>
        <text x="540" y="56" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">FastAPI · /api/sensec/hsm/v1</text>

        <rect x="455" y="65" width="170" height="50" rx="5" fill="#22263a" stroke="#10b981" strokeWidth="1" />
        <text x="540" y="83" textAnchor="middle" fill="#10b981" fontSize="10" fontFamily="monospace">POST /encrypt</text>
        <text x="540" y="98" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Gen DEK → AES-256-GCM</text>
        <text x="540" y="110" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">random IV → wrap DEK</text>

        <rect x="455" y="125" width="170" height="50" rx="5" fill="#22263a" stroke="#f87171" strokeWidth="1" />
        <text x="540" y="143" textAnchor="middle" fill="#f87171" fontSize="10" fontFamily="monospace">POST /decrypt</text>
        <text x="540" y="158" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Grant check → unwrap</text>
        <text x="540" y="170" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">AES-256-GCM decrypt</text>

        <rect x="455" y="185" width="170" height="38" rx="5" fill="#22263a" stroke="#fb923c" strokeWidth="1" />
        <text x="540" y="203" textAnchor="middle" fill="#fb923c" fontSize="10" fontFamily="monospace">/admin/rotate-kek · /grants</text>
        <text x="540" y="216" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">Re-wrap EDEKs · manage grants</text>

        <rect x="455" y="233" width="170" height="30" rx="5" fill="#22263a" stroke="#64748b" strokeWidth="1" />
        <text x="540" y="252" textAnchor="middle" fill="#94a3b8" fontSize="10" fontFamily="monospace">GET /health · /apps/status</text>

        <rect x="455" y="272" width="170" height="22" rx="4" fill="#1e3a2f" stroke="#10b981" strokeWidth="1" />
        <text x="540" y="287" textAnchor="middle" fill="#10b981" fontSize="9" fontFamily="monospace">FIPS 140-2 Level 3 · AES-256-GCM</text>

        <line x1="640" y1="130" x2="690" y2="130" stroke="#a78bfa" strokeWidth="1.5" markerEnd="url(#arr-purple)" />
        <text x="665" y="120" textAnchor="middle" fill="#a78bfa" fontSize="9" fontFamily="monospace">wrap/unwrap</text>

        <rect x="690" y="60" width="190" height="200" rx="8" fill="#1a1d27" stroke="#e879f9" strokeWidth="2" />
        <text x="785" y="82" textAnchor="middle" fill="#e879f9" fontSize="10" letterSpacing="1" fontFamily="monospace">AZURE KEY VAULT</text>
        <text x="785" y="96" textAnchor="middle" fill="#e879f9" fontSize="9" fontFamily="monospace">Managed HSM · FIPS 140-2 L3</text>

        <rect x="705" y="106" width="160" height="30" rx="4" fill="#2d1b47" stroke="#e879f9" strokeWidth="1" />
        <text x="785" y="121" textAnchor="middle" fill="#e879f9" fontSize="10" fontFamily="monospace">KEK (RSA-HSM 4096)</text>
        <text x="785" y="133" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">never leaves HSM boundary</text>

        <rect x="705" y="145" width="160" height="24" rx="4" fill="#22263a" />
        <text x="785" y="161" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Key Versioning</text>

        <rect x="705" y="177" width="160" height="24" rx="4" fill="#22263a" />
        <text x="785" y="193" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Auto-rotation Policy</text>

        <rect x="705" y="209" width="160" height="24" rx="4" fill="#22263a" />
        <text x="785" y="225" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Role-based Access (RBAC)</text>

        <rect x="705" y="241" width="160" height="14" rx="3" fill="#22263a" />
        <text x="785" y="252" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">Managed Identity (no secrets)</text>

        <rect x="440" y="340" width="200" height="120" rx="8" fill="#1a1d27" stroke="#38bdf8" strokeWidth="1.5" />
        <text x="540" y="362" textAnchor="middle" fill="#38bdf8" fontSize="10" letterSpacing="1" fontFamily="monospace">EDEK STORE</text>
        <text x="540" y="376" textAnchor="middle" fill="#555b7a" fontSize="9" fontFamily="monospace">PostgreSQL</text>

        <rect x="455" y="384" width="170" height="22" rx="4" fill="#22263a" />
        <text x="540" y="399" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">edek_id · blob · owner app_id</text>

        <rect x="455" y="412" width="170" height="22" rx="4" fill="#22263a" />
        <text x="540" y="427" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">algorithm · encoding · class.</text>

        <rect x="455" y="440" width="170" height="14" rx="3" fill="#22263a" />
        <text x="540" y="451" textAnchor="middle" fill="#555b7a" fontSize="8" fontFamily="monospace">encrypted at rest · TDE</text>

        <line x1="540" y1="300" x2="540" y2="340" stroke="#38bdf8" strokeWidth="1.5" markerEnd="url(#arr-cyan)" />

        <rect x="690" y="310" width="190" height="110" rx="8" fill="#1a1d27" stroke="#fb923c" strokeWidth="1.5" />
        <text x="785" y="330" textAnchor="middle" fill="#fb923c" fontSize="10" letterSpacing="1" fontFamily="monospace">GRANTS + ROTATION</text>
        <rect x="705" y="338" width="160" height="22" rx="4" fill="#22263a" />
        <text x="785" y="353" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">grantee → owner pairs</text>
        <rect x="705" y="366" width="160" height="22" rx="4" fill="#22263a" />
        <text x="785" y="381" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">default-deny, audited</text>
        <rect x="705" y="394" width="160" height="20" rx="4" fill="#22263a" />
        <text x="785" y="408" textAnchor="middle" fill="#cdd2f0" fontSize="9" fontFamily="monospace">monthly KEK re-wrap job</text>

        <line x1="785" y1="310" x2="785" y2="260" stroke="#fb923c" strokeWidth="1.5" markerEnd="url(#arr-orange)" />
        <line x1="690" y1="365" x2="640" y2="410" stroke="#fb923c" strokeWidth="1.2" strokeDasharray="4,3" markerEnd="url(#arr-orange)" />

        <rect x="20" y="460" width="400" height="200" rx="8" fill="#1a1d27" stroke="#2d3148" strokeWidth="1.5" />
        <text x="220" y="480" textAnchor="middle" fill="#8b92b8" fontSize="10" letterSpacing="1" fontFamily="monospace">ENCRYPT PAYLOAD FLOW</text>

        <text x="36" y="500" fill="#10b981" fontSize="10" fontFamily="monospace">Encrypt:</text>
        <rect x="36" y="508" width="365" height="60" rx="4" fill="#22263a" />
        <text x="50" y="522" fill="#555b7a" fontSize="9" fontFamily="monospace">Request:  {'{'} plaintext, encoding, data_classification, context {'}'}</text>
        <text x="50" y="536" fill="#555b7a" fontSize="9" fontFamily="monospace">Generate:  DEK = random_bytes(32)   IV = random_bytes(12)</text>
        <text x="50" y="550" fill="#555b7a" fontSize="9" fontFamily="monospace">Cipher  =  AES-256-GCM(DEK, IV, plaintext, AAD=owner_app_id)</text>
        <text x="50" y="562" fill="#555b7a" fontSize="9" fontFamily="monospace">EDEK    =  KEK.wrap(DEK)  →  stored in EDEK Store</text>

        <text x="36" y="582" fill="#cdd2f0" fontSize="9" fontFamily="monospace">Response: {'{'} edek_id, owner_app_id, algorithm, iv, ciphertext, tag {'}'}</text>

        <text x="36" y="600" fill="#f87171" fontSize="10" fontFamily="monospace">Decrypt:</text>
        <rect x="36" y="608" width="365" height="44" rx="4" fill="#22263a" />
        <text x="50" y="622" fill="#555b7a" fontSize="9" fontFamily="monospace">Request:   {'{'} edek_id, iv, ciphertext, tag {'}'}</text>
        <text x="50" y="636" fill="#555b7a" fontSize="9" fontFamily="monospace">Lookup owner → grant check → HSM.unwrap → AES-256-GCM decrypt</text>
        <text x="50" y="648" fill="#555b7a" fontSize="9" fontFamily="monospace">Response:  {'{'} plaintext, owner_app_id {'}'}  DEK zeroed immediately</text>

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
          <marker id="arr-orange" markerWidth="8" markerHeight="6" refX="6" refY="3" orient="auto">
            <polygon points="0 0, 8 3, 0 6" fill="#fb923c" />
          </marker>
        </defs>
      </svg>
    </div>
  );
}

// ── Component ──────────────────────────────────────────────────────────────────
export default function HsmDemo() {
  // ── Tab state, deep-linkable via #diagram ──────────────────────────────────
  const [activeTab, setActiveTab] = useState(() => (window.location.hash === '#diagram' ? 'diagram' : 'demo'));

  useEffect(() => {
    function onHash() { setActiveTab(window.location.hash === '#diagram' ? 'diagram' : 'demo'); }
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  function selectTab(tab) {
    setActiveTab(tab);
    const base = window.location.pathname + window.location.search;
    window.history.replaceState(null, '', tab === 'diagram' ? `${base}#diagram` : base);
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

  async function handleEncrypt() {
    if (!selectedApp || !plaintext.trim()) return;
    setEncrypting(true);
    setEncryptError(null);
    const res = await callApi('/encrypt', {
      method: 'POST',
      app: selectedApp,
      body: { plaintext, encoding: 'utf8', data_classification: dataClass || null },
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

  async function handleDecrypt() {
    if (!selectedApp) return;
    setDecrypting(true);
    setDecryptError(null);
    const res = await callApi('/decrypt', {
      method: 'POST',
      app: selectedApp,
      body: {
        edek_id:        decryptForm.edekId,
        iv_b64:         decryptForm.ivB64,
        ciphertext_b64: decryptForm.ciphertextB64,
        tag_b64:        decryptForm.tagB64,
      },
    });
    setDecrypting(false);
    if (res.ok) {
      setDecryptResult({ ...res.data, decrypted_as: selectedApp.app_id });
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
          <Panel title="2. Encrypt">
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
          <Panel title="3. Decrypt" sub="Auto-filled from the Encrypt response above.">
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
                ]}
              />
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

        {/* ── Tab 2: Architecture Diagram ── */}
        {activeTab === 'diagram' && (
          <div role="tabpanel" style={s.demoScroll}>
            <ArchitectureDiagram />
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
};
