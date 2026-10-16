'use strict';

// ── Provider response headers + trace IDs ─────────────────────────────────────
// Pure functions, no I/O. See docs/adr/0021-provider-trace-ids.md.
//
// Services disagree on how they spell the same ID — `requestid`,
// `x-request-id`, `request_id`, `X-Request-ID` — so trace headers are matched
// on a normalised name (lowercase, leading "x-" dropped, "-"/"_" removed)
// rather than exact spelling. All of the above normalise to "requestid".

const DEFAULT_TRACE_NAMES = 'requestid,transactionid,correlationid';

const LABELS = {
  requestid:     'Request ID',
  transactionid: 'Transaction ID',
  correlationid: 'Correlation ID',
};

// Never forwarded to the browser
const SENSITIVE = new Set([
  'set-cookie', 'set-cookie2', 'cookie',
  'authorization', 'www-authenticate',
]);

function isSensitive(name) {
  return SENSITIVE.has(name) || name.startsWith('proxy-');
}

function normaliseTraceName(name) {
  return String(name).trim().toLowerCase().replace(/^x-/, '').replace(/[-_]/g, '');
}

// PROVIDER_TRACE_HEADERS lists base names; any spelling of each one matches.
function getTraceNames(raw = process.env.PROVIDER_TRACE_HEADERS) {
  const list = (raw && raw.trim()) ? raw : DEFAULT_TRACE_NAMES;
  return [...new Set(list.split(',').map(normaliseTraceName).filter(Boolean))];
}

// entries: iterable of [name, value] (fetch Headers.entries() or Object.entries()).
// Returns a plain { lowercaseName: value } object with sensitive headers removed.
function filterHeaders(entries) {
  const out = {};
  for (const [rawName, rawValue] of entries || []) {
    const name = String(rawName).toLowerCase();
    if (isSensitive(name)) continue;
    const value = Array.isArray(rawValue) ? rawValue.join(', ') : String(rawValue ?? '');
    out[name] = out[name] !== undefined ? `${out[name]}, ${value}` : value;
  }
  return out;
}

// Returns [{ label, header, value }] in trace-name order. When two spellings
// carry different values (e.g. provider `requestid` + gateway `x-request-id`)
// both are kept; identical values are shown once.
function extractTraceIds(headers, traceNames = getTraceNames()) {
  const found = [];
  for (const base of traceNames) {
    const seen = new Set();
    for (const [header, value] of Object.entries(headers || {})) {
      if (normaliseTraceName(header) !== base || !value || seen.has(value)) continue;
      seen.add(value);
      found.push({ label: LABELS[base] || base, header, value });
    }
  }
  return found;
}

module.exports = { filterHeaders, extractTraceIds, getTraceNames, normaliseTraceName };
