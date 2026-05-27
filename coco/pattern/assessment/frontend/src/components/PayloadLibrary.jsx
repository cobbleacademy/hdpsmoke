import { useState, useEffect } from 'react';
import jsYaml from 'js-yaml';

export default function PayloadLibrary() {
  const [payloads, setPayloads] = useState([]);
  const [fetchStatus, setFetchStatus] = useState('loading');
  const [errorMsg, setErrorMsg] = useState('');
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    fetch(`${import.meta.env.BASE_URL}payloads.yaml`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.text();
      })
      .then((text) => {
        const parsed = jsYaml.load(text);
        if (!parsed?.payloads?.length) throw new Error('payloads key missing or empty');
        setPayloads(parsed.payloads);
        setFetchStatus('ready');
      })
      .catch((err) => {
        setErrorMsg(`Could not load payloads: ${err.message}`);
        setFetchStatus('error');
      });
  }, []);

  function getPrettyJson(entry) {
    try {
      return JSON.stringify(JSON.parse(entry.payload), null, 2);
    } catch {
      return entry.payload;
    }
  }

  function handleCopy() {
    const text = getPrettyJson(payloads[selectedIndex]);
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    });
  }

  if (fetchStatus === 'loading') {
    return (
      <div style={styles.screen}>
        <div style={styles.spinner} />
        <p style={styles.loadingText}>Loading payloads…</p>
      </div>
    );
  }

  if (fetchStatus === 'error') {
    return (
      <div style={styles.screen}>
        <p style={styles.errorText}>{errorMsg}</p>
      </div>
    );
  }

  const selected = payloads[selectedIndex];

  return (
    <div style={styles.page}>
      <div style={styles.container}>
        <header style={styles.header}>
          <h1 style={styles.title}>Payload Library</h1>
          <p style={styles.subtitle}>10 sample API payloads. Click one to inspect its JSON.</p>
        </header>

        <div style={styles.columns}>
          {/* Left: list */}
          <div style={styles.list}>
            {payloads.map((item, i) => (
              <button
                key={i}
                onClick={() => setSelectedIndex(i)}
                style={{
                  ...styles.listItem,
                  ...(i === selectedIndex ? styles.listItemActive : {}),
                }}
              >
                <span style={styles.listIndex}>{String(i + 1).padStart(2, '0')}</span>
                <span style={styles.listName}>{item.name}</span>
                {i === selectedIndex && <span style={styles.listArrow}>›</span>}
              </button>
            ))}
          </div>

          {/* Right: detail */}
          <div style={styles.detail}>
            <div style={styles.detailHeader}>
              <div>
                <h2 style={styles.detailTitle}>{selected.name}</h2>
                <span style={styles.detailBadge}>JSON Payload</span>
              </div>
              <button
                onClick={handleCopy}
                style={{
                  ...styles.copyBtn,
                  ...(copied ? styles.copyBtnSuccess : {}),
                }}
              >
                {copied ? '✓ Copied!' : 'Copy JSON'}
              </button>
            </div>
            <pre style={styles.pre}>{getPrettyJson(selected)}</pre>
          </div>
        </div>
      </div>
    </div>
  );
}

const styles = {
  page: {
    padding: '2.5rem 2rem',
    animation: 'fadeIn 0.3s ease',
  },
  container: {
    maxWidth: 960,
    margin: '0 auto',
  },
  header: {
    marginBottom: '1.75rem',
  },
  title: {
    fontSize: '1.6rem',
    fontWeight: 800,
    color: 'var(--text-primary)',
    margin: 0,
    marginBottom: '0.375rem',
  },
  subtitle: {
    fontSize: '0.9rem',
    color: 'var(--text-secondary)',
    margin: 0,
  },
  columns: {
    display: 'grid',
    gridTemplateColumns: '260px 1fr',
    gap: '1.25rem',
    alignItems: 'start',
  },
  list: {
    display: 'flex',
    flexDirection: 'column',
    gap: '4px',
    background: 'var(--surface)',
    border: '1px solid var(--border)',
    borderRadius: 'var(--radius)',
    padding: '0.5rem',
    boxShadow: 'var(--shadow)',
  },
  listItem: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.625rem',
    padding: '0.6rem 0.75rem',
    borderRadius: '8px',
    border: '1px solid transparent',
    background: 'transparent',
    cursor: 'pointer',
    width: '100%',
    textAlign: 'left',
    fontFamily: 'inherit',
    color: 'var(--text-primary)',
    transition: 'all 0.15s ease',
  },
  listItemActive: {
    background: 'var(--accent-light)',
    borderColor: 'var(--accent)',
  },
  listIndex: {
    fontSize: '0.68rem',
    fontWeight: 700,
    color: 'var(--text-secondary)',
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    flexShrink: 0,
    width: '1.5rem',
  },
  listName: {
    fontSize: '0.845rem',
    fontWeight: 600,
    flex: 1,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  listArrow: {
    color: 'var(--accent)',
    fontWeight: 700,
    fontSize: '1rem',
    flexShrink: 0,
  },
  detail: {
    background: 'var(--surface)',
    border: '1px solid var(--border)',
    borderRadius: 'var(--radius)',
    boxShadow: 'var(--shadow)',
    overflow: 'hidden',
  },
  detailHeader: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '1rem 1.25rem',
    borderBottom: '1px solid var(--border)',
    gap: '1rem',
    flexWrap: 'wrap',
  },
  detailTitle: {
    fontSize: '1rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
    margin: 0,
    marginBottom: '0.25rem',
  },
  detailBadge: {
    display: 'inline-block',
    fontSize: '0.68rem',
    fontWeight: 700,
    letterSpacing: '0.06em',
    color: 'var(--accent-dark)',
    background: 'var(--accent-light)',
    borderRadius: '5px',
    padding: '2px 8px',
  },
  copyBtn: {
    padding: '0.45rem 1rem',
    borderRadius: '8px',
    border: '1.5px solid var(--accent)',
    background: 'transparent',
    color: 'var(--accent)',
    fontFamily: 'inherit',
    fontSize: '0.8rem',
    fontWeight: 600,
    cursor: 'pointer',
    transition: 'all 0.15s ease',
    flexShrink: 0,
  },
  copyBtnSuccess: {
    background: '#f0fdf4',
    borderColor: '#22c55e',
    color: '#15803d',
  },
  pre: {
    margin: 0,
    padding: '1.25rem',
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: '0.8rem',
    lineHeight: 1.6,
    color: 'var(--text-primary)',
    background: 'var(--bg)',
    overflowX: 'auto',
    whiteSpace: 'pre',
  },
  screen: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    minHeight: '60vh',
    gap: '1rem',
  },
  spinner: {
    width: 32,
    height: 32,
    borderRadius: '50%',
    border: '3px solid var(--accent-light)',
    borderTopColor: 'var(--accent)',
    animation: 'spin 0.8s linear infinite',
  },
  loadingText: {
    color: 'var(--text-secondary)',
    fontSize: '0.9rem',
  },
  errorText: {
    color: '#dc2626',
    fontSize: '0.9rem',
    textAlign: 'center',
    maxWidth: 400,
  },
};
