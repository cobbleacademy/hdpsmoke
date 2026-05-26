const PATTERN_ICONS = {
  Analytical: '🧠',
  Creative: '✨',
  Structured: '🏗️',
  Adaptive: '🧭',
};

export default function ResultCard({ result, onRestart }) {
  const { pattern, rankedPatterns, explanation } = result;
  const icon = PATTERN_ICONS[pattern.category] || '🎯';
  const maxScore = rankedPatterns[0].score;

  return (
    <div style={styles.wrapper}>
      <div style={styles.card}>
        {/* Header */}
        <div style={{ ...styles.patternBadge, background: `${pattern.color}18`, borderColor: `${pattern.color}40` }}>
          <span style={styles.patternIcon}>{icon}</span>
          <div>
            <p style={{ ...styles.patternLabel, color: pattern.color }}>Your Pattern</p>
            <h1 style={styles.patternName}>{pattern.name}</h1>
            <p style={styles.patternTagline}>{pattern.tagline}</p>
          </div>
        </div>

        {/* LLM Explanation */}
        <div style={styles.explanationSection}>
          <h3 style={styles.sectionTitle}>Why This Pattern Fits You</h3>
          <div style={styles.explanation}>
            {explanation.split('\n\n').map((para, i) => (
              <p key={i} style={styles.para}>
                {para}
              </p>
            ))}
          </div>
        </div>

        {/* Score Breakdown */}
        <div style={styles.scoresSection}>
          <h3 style={styles.sectionTitle}>Score Breakdown</h3>
          <div style={styles.scores}>
            {rankedPatterns.map((p, i) => {
              const barPct = maxScore > 0 ? (p.score / maxScore) * 100 : 0;
              return (
                <div key={p.category} style={styles.scoreRow}>
                  <div style={styles.scoreRowHeader}>
                    <span style={styles.scoreName}>
                      {PATTERN_ICONS[p.category]} {p.name}
                      {i === 0 && (
                        <span style={{ ...styles.winnerBadge, background: `${p.color}20`, color: p.color }}>
                          Winner
                        </span>
                      )}
                    </span>
                    <span style={styles.scoreValue}>{p.score.toFixed(1)} pts</span>
                  </div>
                  <div style={styles.barTrack}>
                    <div
                      style={{
                        ...styles.barFill,
                        width: `${barPct}%`,
                        background: p.color,
                        opacity: i === 0 ? 1 : 0.45,
                      }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <button style={styles.restartBtn} onClick={onRestart}>
          Retake Assessment
        </button>
      </div>
    </div>
  );
}

const styles = {
  wrapper: {
    width: '100%',
    display: 'flex',
    justifyContent: 'center',
    padding: '2rem 1rem',
  },
  card: {
    background: 'var(--surface)',
    borderRadius: 'var(--radius)',
    boxShadow: 'var(--shadow-lg)',
    border: '1px solid var(--border)',
    padding: '2.5rem',
    width: '100%',
    maxWidth: '600px',
    display: 'flex',
    flexDirection: 'column',
    gap: '2rem',
    animation: 'fadeIn 0.5s ease',
  },
  patternBadge: {
    display: 'flex',
    alignItems: 'center',
    gap: '1.25rem',
    padding: '1.5rem',
    borderRadius: '12px',
    border: '1.5px solid',
  },
  patternIcon: {
    fontSize: '2.5rem',
    lineHeight: 1,
    flexShrink: 0,
  },
  patternLabel: {
    fontSize: '0.72rem',
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    marginBottom: '0.25rem',
  },
  patternName: {
    fontSize: '1.6rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
    lineHeight: 1.1,
  },
  patternTagline: {
    fontSize: '0.8rem',
    color: 'var(--text-secondary)',
    marginTop: '0.3rem',
  },
  explanationSection: {},
  sectionTitle: {
    fontSize: '0.8rem',
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: '0.07em',
    color: 'var(--text-secondary)',
    marginBottom: '0.875rem',
  },
  explanation: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.875rem',
  },
  para: {
    fontSize: '0.9375rem',
    lineHeight: 1.7,
    color: 'var(--text-primary)',
  },
  scoresSection: {},
  scores: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.875rem',
  },
  scoreRow: {},
  scoreRowHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '5px',
  },
  scoreName: {
    fontSize: '0.875rem',
    fontWeight: 500,
    color: 'var(--text-primary)',
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
  },
  scoreValue: {
    fontSize: '0.8rem',
    fontWeight: 600,
    color: 'var(--text-secondary)',
  },
  winnerBadge: {
    fontSize: '0.65rem',
    fontWeight: 700,
    padding: '2px 7px',
    borderRadius: '999px',
    letterSpacing: '0.04em',
  },
  barTrack: {
    height: '6px',
    borderRadius: '999px',
    background: 'var(--border)',
    overflow: 'hidden',
  },
  barFill: {
    height: '100%',
    borderRadius: '999px',
    transition: 'width 0.6s cubic-bezier(0.4, 0, 0.2, 1)',
  },
  restartBtn: {
    padding: '0.875rem',
    borderRadius: '10px',
    border: '1.5px solid var(--border)',
    background: 'var(--surface)',
    color: 'var(--text-secondary)',
    fontSize: '0.9rem',
    fontWeight: 600,
    cursor: 'pointer',
    transition: 'all 0.18s ease',
    fontFamily: 'inherit',
  },
};
