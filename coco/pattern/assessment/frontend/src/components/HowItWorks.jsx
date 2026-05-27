const STEPS = [
  {
    number: '01',
    title: 'Answer 10 questions',
    description:
      'Each question targets one of four cognitive categories: Analytical, Creative, Structured, or Adaptive. Questions are shown one at a time so you focus on each prompt without being anchored by later ones.',
    icon: '📝',
    detail: '10 questions · 4 options each (1–4 scale)',
  },
  {
    number: '02',
    title: 'Answers are weighted',
    description:
      'Not all questions carry equal importance. Each question has a weightage multiplier (e.g. 1.5×, 2.2×) that reflects how strongly it distinguishes between patterns. Your raw answer value is multiplied by this weight.',
    icon: '⚖️',
    detail: 'score[category] += answer.value × question.weightage',
    isCode: true,
  },
  {
    number: '03',
    title: 'Scores are accumulated per category',
    description:
      'Every answer adds points to one category\'s running total. By the end of the quiz, each of the four categories has a cumulative weighted score that reflects how strongly your answers aligned with that style.',
    icon: '📊',
    detail: 'Example: Analytical 24.0 · Creative 16.8 · Structured 12.1 · Adaptive 9.4',
  },
  {
    number: '04',
    title: 'Highest score wins the Pattern',
    description:
      'The category with the highest total score maps to your recommended Pattern. The runner-up score is also preserved — if it\'s within 20% of the winner, the AI explanation will acknowledge that secondary alignment.',
    icon: '🏆',
    detail: 'Winner = argmax(scores) → Pattern name + description',
    isCode: true,
  },
  {
    number: '05',
    title: 'AI generates your personalised explanation',
    description:
      'The full score leaderboard and every one of your specific answers are sent to an LLM (GPT-4o-mini). The model is instructed to write three paragraphs: why the pattern fits you, your key strengths, and one actionable tip — grounded in your actual responses, not generic text.',
    icon: '🤖',
    detail: 'Powered by OpenAI gpt-4o-mini · ~2–4 s response time',
  },
];

const SCORE_EXAMPLE = [
  { category: 'Analytical', score: 24.0, color: '#4F46E5', pct: 100 },
  { category: 'Creative', score: 16.8, color: '#D97706', pct: 70 },
  { category: 'Structured', score: 12.1, color: '#059669', pct: 50 },
  { category: 'Adaptive', score: 9.4, color: '#DC2626', pct: 39 },
];

export default function HowItWorks() {
  return (
    <div style={styles.page}>
      <div style={styles.container}>
        <header style={styles.header}>
          <h1 style={styles.title}>How It Works</h1>
          <p style={styles.subtitle}>
            The assessment uses a weighted scoring algorithm to match your answers to a cognitive work-style Pattern,
            then uses an LLM to explain the result in your specific context.
          </p>
        </header>

        {/* Steps */}
        <div style={styles.steps}>
          {STEPS.map((step, i) => (
            <div key={step.number} style={styles.step}>
              <div style={styles.stepLeft}>
                <div style={styles.stepNumber}>{step.number}</div>
                {i < STEPS.length - 1 && <div style={styles.stepLine} />}
              </div>
              <div style={styles.stepRight}>
                <div style={styles.stepHeader}>
                  <span style={styles.stepIcon}>{step.icon}</span>
                  <h3 style={styles.stepTitle}>{step.title}</h3>
                </div>
                <p style={styles.stepDescription}>{step.description}</p>
                {step.detail && (
                  <div style={{ ...styles.stepDetail, ...(step.isCode ? styles.stepDetailCode : {}) }}>
                    {step.detail}
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>

        {/* Score visualisation example */}
        <div style={styles.exampleCard}>
          <h3 style={styles.exampleTitle}>Example score breakdown</h3>
          <p style={styles.exampleSubtitle}>
            This is what the scoring looks like for someone whose answers strongly favour the Analytical pattern.
          </p>
          <div style={styles.bars}>
            {SCORE_EXAMPLE.map((item, i) => (
              <div key={item.category} style={styles.barRow}>
                <div style={styles.barRowHeader}>
                  <span style={styles.barLabel}>
                    {i === 0 && <span style={{ ...styles.winnerTag, color: item.color, background: `${item.color}18` }}>Winner</span>}
                    {item.category}
                  </span>
                  <span style={styles.barScore}>{item.score} pts</span>
                </div>
                <div style={styles.barTrack}>
                  <div
                    style={{
                      ...styles.barFill,
                      width: `${item.pct}%`,
                      background: item.color,
                      opacity: i === 0 ? 1 : 0.45,
                    }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Weightage table */}
        <div style={styles.tableCard}>
          <h3 style={styles.tableTitle}>Question weightage reference</h3>
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.th}>Q#</th>
                <th style={styles.th}>Category</th>
                <th style={styles.th}>Weightage</th>
                <th style={styles.th}>Max pts (val × 4)</th>
              </tr>
            </thead>
            <tbody>
              {[
                [1, 'Analytical', 2.0, 8.0],
                [2, 'Creative', 1.8, 7.2],
                [3, 'Structured', 1.5, 6.0],
                [4, 'Adaptive', 1.7, 6.8],
                [5, 'Analytical', 2.2, 8.8],
                [6, 'Creative', 1.6, 6.4],
                [7, 'Structured', 1.9, 7.6],
                [8, 'Adaptive', 2.0, 8.0],
                [9, 'Analytical', 1.8, 7.2],
                [10, 'Creative', 2.1, 8.4],
              ].map(([q, cat, w, max]) => (
                <tr key={q} style={styles.tr}>
                  <td style={styles.td}>Q{q}</td>
                  <td style={styles.td}>{cat}</td>
                  <td style={{ ...styles.td, fontWeight: 600 }}>{w}×</td>
                  <td style={{ ...styles.td, color: 'var(--accent)', fontWeight: 600 }}>{max}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

const styles = {
  page: {
    padding: '2.5rem 2rem',
    maxWidth: 720,
    margin: '0 auto',
    animation: 'fadeIn 0.3s ease',
  },
  container: {},
  header: { marginBottom: '2.5rem' },
  title: {
    fontSize: '1.6rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
    letterSpacing: '-0.02em',
  },
  subtitle: {
    marginTop: '0.5rem',
    fontSize: '0.9375rem',
    color: 'var(--text-secondary)',
    lineHeight: 1.65,
  },
  steps: {
    display: 'flex',
    flexDirection: 'column',
    marginBottom: '2.5rem',
  },
  step: {
    display: 'flex',
    gap: '1.25rem',
  },
  stepLeft: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    flexShrink: 0,
  },
  stepNumber: {
    width: 36,
    height: 36,
    borderRadius: '50%',
    background: 'var(--accent)',
    color: '#fff',
    fontSize: '0.72rem',
    fontWeight: 700,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    flexShrink: 0,
    letterSpacing: '0.02em',
  },
  stepLine: {
    width: 2,
    flex: 1,
    background: 'var(--border)',
    margin: '6px 0',
    minHeight: 24,
  },
  stepRight: {
    paddingBottom: '2rem',
    flex: 1,
  },
  stepHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.625rem',
    marginBottom: '0.5rem',
  },
  stepIcon: {
    fontSize: '1.1rem',
    lineHeight: 1,
  },
  stepTitle: {
    fontSize: '1rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
  },
  stepDescription: {
    fontSize: '0.875rem',
    color: 'var(--text-secondary)',
    lineHeight: 1.7,
    marginBottom: '0.625rem',
  },
  stepDetail: {
    display: 'inline-block',
    padding: '0.35rem 0.75rem',
    borderRadius: '6px',
    background: 'var(--accent-light)',
    color: 'var(--accent-dark)',
    fontSize: '0.8rem',
    fontWeight: 500,
  },
  stepDetailCode: {
    fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    fontSize: '0.78rem',
  },
  exampleCard: {
    background: 'var(--surface)',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    padding: '1.5rem',
    boxShadow: 'var(--shadow)',
    marginBottom: '1.25rem',
  },
  exampleTitle: {
    fontSize: '1rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
    marginBottom: '0.25rem',
  },
  exampleSubtitle: {
    fontSize: '0.8rem',
    color: 'var(--text-secondary)',
    marginBottom: '1.25rem',
    lineHeight: 1.6,
  },
  bars: { display: 'flex', flexDirection: 'column', gap: '0.875rem' },
  barRow: {},
  barRowHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '5px',
  },
  barLabel: {
    fontSize: '0.8rem',
    fontWeight: 600,
    color: 'var(--text-primary)',
    display: 'flex',
    alignItems: 'center',
    gap: '0.5rem',
  },
  winnerTag: {
    fontSize: '0.62rem',
    fontWeight: 700,
    padding: '2px 7px',
    borderRadius: '999px',
  },
  barScore: {
    fontSize: '0.78rem',
    fontWeight: 600,
    color: 'var(--text-secondary)',
  },
  barTrack: {
    height: '7px',
    borderRadius: '999px',
    background: 'var(--border)',
    overflow: 'hidden',
  },
  barFill: {
    height: '100%',
    borderRadius: '999px',
    transition: 'width 0.6s cubic-bezier(0.4,0,0.2,1)',
  },
  tableCard: {
    background: 'var(--surface)',
    borderRadius: 'var(--radius)',
    border: '1px solid var(--border)',
    padding: '1.5rem',
    boxShadow: 'var(--shadow)',
  },
  tableTitle: {
    fontSize: '1rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
    marginBottom: '1rem',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
  },
  th: {
    fontSize: '0.7rem',
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    color: 'var(--text-secondary)',
    padding: '0.5rem 0.75rem',
    textAlign: 'left',
    borderBottom: '2px solid var(--border)',
  },
  tr: {
    borderBottom: '1px solid var(--border)',
  },
  td: {
    padding: '0.5rem 0.75rem',
    fontSize: '0.85rem',
    color: 'var(--text-primary)',
  },
};
