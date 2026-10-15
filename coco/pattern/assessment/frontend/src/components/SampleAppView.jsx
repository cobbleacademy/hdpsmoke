import { useState } from 'react';
import { resolveFields, FULL, PARTIAL } from '../utils/fieldAccess.js';

// ── Sample App view ───────────────────────────────────────────────────────────
// Renders a provider access-decision response as a consumer-style screen,
// applying FULL / PARTIAL / deny-by-default to the payload's fake example
// values. Knows nothing about Payload Library — props only (see ADR-020).
//
//   fields      payload YAML `fields:` block
//   body        provider response (object or JSON string)
//   title       card heading (payload name)
//   sourceLabel e.g. "Live", "Saved sample", "History · 10:42:01"

const BADGE = {
  [FULL]:    { label: 'FULL',          color: 'var(--success)' },
  [PARTIAL]: { label: 'PARTIAL',       color: 'var(--warning)' },
  DENY:      { label: 'DENIED',        color: 'var(--error)' },
  none:      { label: 'not sensitive', color: 'var(--text-secondary)' },
};

export default function SampleAppView({ fields, body, title, sourceLabel }) {
  const [explain, setExplain] = useState(true);
  const { rows, warnings } = resolveFields(fields, body);

  return (
    <div style={s.wrap}>
      <div style={s.card}>
        <div style={s.head}>
          <div style={{ minWidth: 0 }}>
            <p style={s.title}>{title}</p>
            <p style={s.sub}>Sample app{sourceLabel ? ` · ${sourceLabel}` : ''}</p>
          </div>
          <label style={s.toggle}>
            <input
              type="checkbox"
              checked={explain}
              onChange={(e) => setExplain(e.target.checked)}
              style={{ cursor: 'pointer' }}
            />
            Explain
          </label>
        </div>

        {rows.map(r => {
          const badge = BADGE[r.access ?? 'none'];
          return (
            <div key={r.key} style={s.row}>
              <span style={s.label}>{r.label}</span>
              <span style={{ minWidth: 0 }}>
                {r.display === null ? (
                  <span style={s.restricted}>🔒 Restricted</span>
                ) : (
                  <span style={r.access === PARTIAL ? s.mono : s.value}>{r.display || '—'}</span>
                )}
                {explain && <span style={s.reason}>{r.reason}</span>}
              </span>
              {explain ? (
                <span style={{ ...s.badge, color: badge.color, background: `color-mix(in srgb, ${badge.color} 14%, transparent)` }}>
                  {badge.label}
                </span>
              ) : <span />}
            </div>
          );
        })}
      </div>

      {explain && warnings.length > 0 && (
        <ul style={s.warnings}>
          {warnings.map((w, i) => <li key={i}>⚠ {w}</li>)}
        </ul>
      )}
    </div>
  );
}

const s = {
  wrap: {
    padding: '1rem 1.125rem',
    background: 'var(--bg)',
  },
  card: {
    background: 'var(--surface)',
    border: '1px solid var(--border)',
    borderRadius: 'var(--radius)',
    padding: '0.875rem 1.125rem',
    maxWidth: '560px',
  },
  head: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: '0.75rem',
    marginBottom: '0.5rem',
  },
  title: {
    margin: 0,
    fontWeight: 700,
    fontSize: '0.95rem',
    color: 'var(--text-primary)',
  },
  sub: {
    margin: 0,
    fontSize: '0.75rem',
    color: 'var(--text-secondary)',
  },
  toggle: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.35rem',
    fontSize: '0.78rem',
    color: 'var(--text-secondary)',
    cursor: 'pointer',
    flexShrink: 0,
  },
  row: {
    display: 'grid',
    gridTemplateColumns: '120px minmax(0, 1fr) auto',
    gap: '0.5rem',
    alignItems: 'center',
    padding: '0.5rem 0',
    borderTop: '1px solid var(--border)',
    fontSize: '0.85rem',
  },
  label: {
    color: 'var(--text-secondary)',
    fontSize: '0.8rem',
  },
  value: {
    color: 'var(--text-primary)',
    overflowWrap: 'anywhere',
  },
  mono: {
    color: 'var(--text-primary)',
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    overflowWrap: 'anywhere',
  },
  restricted: {
    color: 'var(--text-secondary)',
    fontStyle: 'italic',
  },
  reason: {
    display: 'block',
    fontSize: '0.7rem',
    color: 'var(--text-secondary)',
    marginTop: '0.1rem',
  },
  badge: {
    fontSize: '0.68rem',
    fontWeight: 700,
    padding: '2px 8px',
    borderRadius: '999px',
    whiteSpace: 'nowrap',
  },
  warnings: {
    margin: '0.75rem 0 0',
    padding: 0,
    listStyle: 'none',
    fontSize: '0.75rem',
    color: 'var(--warning)',
    maxWidth: '560px',
  },
};
