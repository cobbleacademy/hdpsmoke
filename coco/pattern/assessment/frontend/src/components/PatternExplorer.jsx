const PATTERNS = [
  {
    category: 'Analytical',
    name: 'The Strategist',
    tagline: 'Logic-driven · Data-focused · Systematic',
    description:
      'Strategists thrive on evidence-based reasoning, structured decomposition of complex problems, and methodical decision-making. They are the people who ask "What does the data say?" before committing to a direction.',
    color: '#4F46E5',
    bg: '#EEF2FF',
    icon: '🧠',
    traits: ['Data-driven decisions', 'Root-cause analysis', 'Pattern recognition', 'Systematic planning'],
    questions: 3,
    maxScore: 24.0,
  },
  {
    category: 'Creative',
    name: 'The Innovator',
    tagline: 'Imaginative · Idea-rich · Boundary-pushing',
    description:
      'Innovators excel at generating novel ideas, making surprising conceptual connections, and challenging the status quo. They are energised by open-ended problems and see constraints as creative prompts.',
    color: '#D97706',
    bg: '#FFFBEB',
    icon: '✨',
    traits: ['Divergent thinking', 'Cross-domain connections', 'Rapid ideation', 'Embracing ambiguity'],
    questions: 3,
    maxScore: 21.6,
  },
  {
    category: 'Structured',
    name: 'The Architect',
    tagline: 'Process-oriented · Reliable · Precision-focused',
    description:
      'Architects build dependable systems and bring clarity to complexity. They deliver consistent, high-quality outcomes by designing robust processes and holding teams accountable to defined standards.',
    color: '#059669',
    bg: '#ECFDF5',
    icon: '🏗️',
    traits: ['Process design', 'Quality standards', 'Risk mitigation', 'Clear documentation'],
    questions: 2,
    maxScore: 17.2,
  },
  {
    category: 'Adaptive',
    name: 'The Navigator',
    tagline: 'Flexible · Resilient · Opportunity-oriented',
    description:
      'Navigators excel at reading shifting situations, pivoting quickly, and charting pragmatic paths through ambiguity. They treat change not as a threat but as a source of competitive advantage.',
    color: '#DC2626',
    bg: '#FEF2F2',
    icon: '🧭',
    traits: ['Situational awareness', 'Rapid pivoting', 'Calm under pressure', 'Opportunistic thinking'],
    questions: 2,
    maxScore: 15.2,
  },
];

export default function PatternExplorer() {
  return (
    <div style={styles.page}>
      <div style={styles.container}>
        <header style={styles.header}>
          <h1 style={styles.title}>Pattern Explorer</h1>
          <p style={styles.subtitle}>
            The assessment maps your answers to one of four work-style patterns. Browse them all below.
          </p>
        </header>

        <div style={styles.grid}>
          {PATTERNS.map((p) => (
            <PatternCard key={p.category} pattern={p} />
          ))}
        </div>

        <div style={styles.note}>
          <span style={styles.noteIcon}>💡</span>
          <p style={styles.noteText}>
            Most people have a <strong>dominant</strong> pattern and a strong <strong>secondary</strong> pattern.
            Your result card shows how close the runner-up was — a tight race means you draw from both styles depending on context.
          </p>
        </div>
      </div>
    </div>
  );
}

function PatternCard({ pattern: p }) {
  return (
    <div style={{ ...styles.card, borderTop: `4px solid ${p.color}` }}>
      <div style={{ ...styles.cardHeader, background: p.bg }}>
        <span style={styles.cardIcon}>{p.icon}</span>
        <div>
          <p style={{ ...styles.cardCategory, color: p.color }}>{p.category}</p>
          <h2 style={styles.cardName}>{p.name}</h2>
          <p style={styles.cardTagline}>{p.tagline}</p>
        </div>
      </div>

      <div style={styles.cardBody}>
        <p style={styles.cardDescription}>{p.description}</p>

        <div style={styles.traitsSection}>
          <p style={styles.traitsLabel}>Key traits</p>
          <div style={styles.traits}>
            {p.traits.map((trait) => (
              <span key={trait} style={{ ...styles.trait, background: p.bg, color: p.color, border: `1px solid ${p.color}30` }}>
                {trait}
              </span>
            ))}
          </div>
        </div>

        <div style={styles.statsRow}>
          <div style={styles.stat}>
            <span style={styles.statValue}>{p.questions}</span>
            <span style={styles.statLabel}>Questions</span>
          </div>
          <div style={styles.statDivider} />
          <div style={styles.stat}>
            <span style={styles.statValue}>{p.maxScore}</span>
            <span style={styles.statLabel}>Max score</span>
          </div>
        </div>
      </div>
    </div>
  );
}

const styles = {
  page: {
    padding: '2.5rem 2rem',
    maxWidth: 860,
    margin: '0 auto',
    animation: 'fadeIn 0.3s ease',
  },
  container: {},
  header: {
    marginBottom: '2rem',
  },
  title: {
    fontSize: '1.6rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
    letterSpacing: '-0.02em',
  },
  subtitle: {
    marginTop: '0.4rem',
    fontSize: '0.9375rem',
    color: 'var(--text-secondary)',
    lineHeight: 1.6,
  },
  grid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fill, minmax(340px, 1fr))',
    gap: '1.25rem',
  },
  card: {
    background: 'var(--surface)',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    overflow: 'hidden',
    boxShadow: 'var(--shadow)',
    display: 'flex',
    flexDirection: 'column',
  },
  cardHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: '1rem',
    padding: '1.25rem 1.5rem',
  },
  cardIcon: {
    fontSize: '2rem',
    lineHeight: 1,
    flexShrink: 0,
  },
  cardCategory: {
    fontSize: '0.65rem',
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    marginBottom: '0.2rem',
  },
  cardName: {
    fontSize: '1.125rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
    lineHeight: 1.2,
  },
  cardTagline: {
    fontSize: '0.72rem',
    color: 'var(--text-secondary)',
    marginTop: '0.2rem',
  },
  cardBody: {
    padding: '1.25rem 1.5rem',
    display: 'flex',
    flexDirection: 'column',
    gap: '1.125rem',
    flex: 1,
  },
  cardDescription: {
    fontSize: '0.875rem',
    color: 'var(--text-secondary)',
    lineHeight: 1.65,
  },
  traitsSection: {},
  traitsLabel: {
    fontSize: '0.65rem',
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    color: 'var(--text-secondary)',
    marginBottom: '0.5rem',
  },
  traits: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: '0.4rem',
  },
  trait: {
    fontSize: '0.72rem',
    fontWeight: 600,
    padding: '3px 10px',
    borderRadius: '999px',
  },
  statsRow: {
    display: 'flex',
    alignItems: 'center',
    gap: '1rem',
    paddingTop: '0.875rem',
    borderTop: '1px solid var(--border)',
    marginTop: 'auto',
  },
  stat: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
  },
  statValue: {
    fontSize: '1.1rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
  },
  statLabel: {
    fontSize: '0.65rem',
    color: 'var(--text-secondary)',
    marginTop: '1px',
  },
  statDivider: {
    width: 1,
    height: 28,
    background: 'var(--border)',
  },
  note: {
    display: 'flex',
    gap: '0.875rem',
    alignItems: 'flex-start',
    marginTop: '1.75rem',
    padding: '1rem 1.25rem',
    background: 'var(--accent-light)',
    borderRadius: '10px',
    border: '1px solid var(--border)',
  },
  noteIcon: {
    fontSize: '1.1rem',
    flexShrink: 0,
    marginTop: '1px',
  },
  noteText: {
    fontSize: '0.85rem',
    color: 'var(--text-secondary)',
    lineHeight: 1.65,
  },
};
