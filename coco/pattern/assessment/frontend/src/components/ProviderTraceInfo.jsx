import { useState } from 'react';

// ── Provider trace IDs + response headers (ADR-021) ───────────────────────────
// TraceIdStrip: copyable chips for the provider's request/transaction IDs plus
// the x-correlation-id the backend sent ("Our ID"). HeadersPanel: collapsible
// list of response headers (sensitive ones already removed by the backend).

function shorten(value, max) {
  return value.length > max ? value.slice(0, max) + '…' : value;
}

function CopyChip({ label, header, value, max = 28 }) {
  const [copied, setCopied] = useState(false);
  function copy() {
    navigator.clipboard.writeText(value).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }
  return (
    <button onClick={copy} style={s.chip} title={`${header ? header + ': ' : ''}${value}\nClick to copy`}>
      <span style={s.chipLabel}>{label}</span>
      {header && <span style={s.chipHeader}>{header}</span>}
      <span style={s.chipValue}>{copied ? '✓ copied' : shorten(value, max)}</span>
    </button>
  );
}

export function TraceIdStrip({ traceIds, correlationId, compact = false }) {
  const ids = Array.isArray(traceIds) ? traceIds : [];
  // Provider echoing our x-correlation-id back is not a separate ID
  const providerIds = ids.filter(t => t.value !== correlationId);
  const echoed = providerIds.length !== ids.length;
  if (providerIds.length === 0 && !correlationId) return null;

  if (compact) {
    // Batch rows: first provider ID, else ours
    const first = providerIds[0];
    return first
      ? <CopyChip label={first.label} value={first.value} max={18} />
      : <CopyChip label="Our ID" value={correlationId} max={18} />;
  }

  return (
    <div style={s.strip}>
      {providerIds.map(t => (
        <CopyChip key={`${t.header}:${t.value}`} label={t.label} header={t.header} value={t.value} />
      ))}
      {correlationId && (
        <CopyChip
          label={echoed ? 'Our ID · echoed' : 'Our ID'}
          header="x-correlation-id"
          value={correlationId}
        />
      )}
    </div>
  );
}

export function HeadersPanel({ headers }) {
  const [open, setOpen] = useState(false);
  const entries = Object.entries(headers || {}).sort(([a], [b]) => a.localeCompare(b));
  if (entries.length === 0) return null;
  return (
    <div style={s.headersWrap}>
      <button onClick={() => setOpen(o => !o)} style={s.headersToggle}>
        {open ? '▾' : '▸'} Response headers ({entries.length})
      </button>
      {open && (
        <table style={s.table}>
          <tbody>
            {entries.map(([name, value]) => (
              <tr key={name}>
                <td style={s.thName}>{name}</td>
                <td style={s.tdValue}>{value}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

const mono = 'ui-monospace, SFMono-Regular, Menlo, monospace';

const s = {
  strip: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: '0.4rem',
    padding: '0.5rem 1rem',
    borderBottom: '1px solid var(--border)',
  },
  chip: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: '0.35rem',
    padding: '2px 8px',
    borderRadius: '999px',
    border: '1px solid var(--border)',
    background: 'var(--surface)',
    color: 'var(--text-primary)',
    fontFamily: 'inherit',
    fontSize: '0.7rem',
    cursor: 'pointer',
    maxWidth: '100%',
  },
  chipLabel: {
    fontWeight: 700,
    color: 'var(--text-secondary)',
    whiteSpace: 'nowrap',
  },
  chipHeader: {
    fontFamily: mono,
    color: 'var(--text-secondary)',
    whiteSpace: 'nowrap',
  },
  chipValue: {
    fontFamily: mono,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  headersWrap: {
    borderTop: '1px solid var(--border)',
    padding: '0.4rem 1rem 0.6rem',
  },
  headersToggle: {
    background: 'none',
    border: 'none',
    padding: '0.2rem 0',
    cursor: 'pointer',
    fontFamily: 'inherit',
    fontSize: '0.75rem',
    fontWeight: 600,
    color: 'var(--text-secondary)',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    marginTop: '0.35rem',
    fontFamily: mono,
    fontSize: '0.72rem',
  },
  thName: {
    padding: '0.2rem 0.75rem 0.2rem 0',
    color: 'var(--text-secondary)',
    verticalAlign: 'top',
    whiteSpace: 'nowrap',
  },
  tdValue: {
    padding: '0.2rem 0',
    color: 'var(--text-primary)',
    overflowWrap: 'anywhere',
  },
};
