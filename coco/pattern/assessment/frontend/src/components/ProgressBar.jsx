export default function ProgressBar({ current, total, progress }) {
  return (
    <div style={styles.wrapper}>
      <div style={styles.label}>
        <span style={styles.counter}>
          Question <strong>{current}</strong> of {total}
        </span>
        <span style={styles.pct}>{Math.round(progress)}%</span>
      </div>
      <div style={styles.track}>
        <div
          style={{
            ...styles.fill,
            width: `${progress}%`,
            transition: 'width 0.4s cubic-bezier(0.4, 0, 0.2, 1)',
          }}
        />
      </div>
    </div>
  );
}

const styles = {
  wrapper: {
    marginBottom: '2rem',
  },
  label: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '8px',
    fontSize: '0.8rem',
    color: 'var(--text-secondary)',
  },
  counter: {
    letterSpacing: '0.01em',
  },
  pct: {
    fontWeight: 600,
    color: 'var(--accent)',
  },
  track: {
    height: '6px',
    borderRadius: '999px',
    background: 'var(--accent-light)',
    overflow: 'hidden',
  },
  fill: {
    height: '100%',
    borderRadius: '999px',
    background: 'linear-gradient(90deg, var(--accent) 0%, var(--accent-dark) 100%)',
  },
};
