// ── Field-level access resolution for the Payload Library Sample App view ─────
// Pure functions, no React. See docs/adr/0020-payload-sample-app-view.md.
//
// The provider response carries per-field access decisions (not data):
//   { response: [ { access: [ { path, actions: [ { action } ] } ] }, ... ] }
// Decisions from every response[] element are merged (single entity).
// The payload YAML's `fields:` block supplies the full field list, which of
// them are sensitive, and fake example values. Rules:
//   - non-sensitive field         → always shown
//   - sensitive + FULL            → shown
//   - sensitive + PARTIAL         → last 4 chars only
//   - sensitive + no decision     → restricted (deny-by-default)
//   - several actions for a path  → most restrictive wins
//   - unknown action / bad shape  → restricted (fail closed)

export const FULL    = 'FULL';
export const PARTIAL = 'PARTIAL';
export const DENY    = 'DENY';

// Higher = more restrictive
const RANK = { [FULL]: 0, [PARTIAL]: 1, [DENY]: 2 };

function normaliseAction(raw) {
  const a = typeof raw === 'string' ? raw.trim().toUpperCase() : '';
  return a === FULL || a === PARTIAL ? a : DENY;
}

function mostRestrictive(a, b) {
  return RANK[a] >= RANK[b] ? a : b;
}

// Keep the last 4 characters; values of 4 chars or fewer are fully masked so
// "PARTIAL" never reveals the whole value.
export function maskLast4(value) {
  const str = value == null ? '' : String(value);
  if (str.length === 0) return '';
  if (str.length <= 4) return '*'.repeat(str.length);
  return '*'.repeat(str.length - 4) + str.slice(-4);
}

// Accepts the response body as an object or a JSON string (history entries
// may hold either). Returns { recognised, decisions: Map<path, action>, warnings }.
export function collectDecisions(body) {
  let data = body;
  if (typeof data === 'string') {
    try { data = JSON.parse(data); }
    catch { data = null; }
  }

  // The provider splits one entity's decisions across several response[]
  // elements (typically one access entry each), so collect from all of them.
  const groups = Array.isArray(data?.response)
    ? data.response.filter(g => Array.isArray(g?.access))
    : [];
  if (groups.length === 0) {
    return {
      recognised: false,
      decisions: new Map(),
      warnings: ['Response shape not recognised (expected response[].access[]) — all sensitive fields treated as restricted.'],
    };
  }

  const decisions = new Map();
  const warnings  = [];
  const skipped   = data.response.length - groups.length;
  if (skipped > 0) {
    warnings.push(`${skipped} response element(s) have no "access" list — ignored.`);
  }
  for (const entry of groups.flatMap(g => g.access)) {
    const path = typeof entry?.path === 'string' ? entry.path : '';
    if (!path) {
      warnings.push('An access entry has no "path" — ignored.');
      continue;
    }
    const actions = Array.isArray(entry.actions) ? entry.actions : [];
    // Empty actions list → deny
    let combined = actions.length === 0 ? DENY : FULL;
    for (const a of actions) {
      const norm = normaliseAction(a?.action);
      if (!(String(a?.action ?? '').trim().toUpperCase() in RANK)) {
        warnings.push(`Unknown action "${a?.action}" on "${path}" — treated as restricted.`);
      }
      combined = mostRestrictive(combined, norm);
    }
    // Same path in several access entries → combine those too
    decisions.set(path, decisions.has(path) ? mostRestrictive(decisions.get(path), combined) : combined);
  }

  return { recognised: true, decisions, warnings };
}

// fields: the payload's `fields:` block. body: provider response (object or string).
// Returns { rows: [{ key, label, sensitive, access, display, reason }], warnings }.
// `access` is null for non-sensitive fields; `display` is null when restricted.
export function resolveFields(fields, body) {
  const { recognised, decisions, warnings } = collectDecisions(body);
  const list = Array.isArray(fields) ? fields : [];
  const known = new Set(list.map(f => f?.key));

  const rows = list.filter(f => f?.key).map(f => {
    const base = { key: f.key, label: f.label || f.key, sensitive: !!f.sensitive };
    const value = f.example == null ? '' : String(f.example);

    if (!f.sensitive) {
      return { ...base, access: null, display: value, reason: 'Not sensitive — always shown' };
    }

    const decision = decisions.get(f.key);
    if (decision === FULL) {
      return { ...base, access: FULL, display: value, reason: 'Provider: FULL' };
    }
    if (decision === PARTIAL) {
      return { ...base, access: PARTIAL, display: maskLast4(value), reason: 'Provider: PARTIAL · last 4 shown' };
    }
    return {
      ...base,
      access: DENY,
      display: null,
      reason: !recognised
        ? 'Response not recognised — restricted'
        : decision === DENY
          ? 'Provider: no usable action — restricted'
          : 'Not in response — deny-by-default',
    };
  });

  const extra = [...decisions.keys()].filter(p => !known.has(p));
  if (extra.length > 0) {
    warnings.push(`Response has paths not declared in fields: ${extra.join(', ')} — not shown.`);
  }

  return { rows, warnings };
}
