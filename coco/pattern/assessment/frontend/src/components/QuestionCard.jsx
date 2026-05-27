export default function QuestionCard({ question, selectedValue, onAnswer, index }) {
  return (
    <div style={styles.card}>
      <p style={styles.category}>{question.category}</p>
      <h2 style={styles.text}>{question.text}</h2>
      <div style={styles.options}>
        {question.options.map((opt) => {
          const isSelected = selectedValue === opt.val;
          return (
            <button
              key={opt.val}
              onClick={() => onAnswer(question.id, opt.val)}
              style={{
                ...styles.option,
                ...(isSelected ? styles.optionSelected : {}),
              }}
            >
              <span
                style={{
                  ...styles.radio,
                  ...(isSelected ? styles.radioSelected : {}),
                }}
              >
                {isSelected && <span style={styles.radioDot} />}
              </span>
              <span style={styles.optionLabel}>{opt.label}</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

const styles = {
  card: {
    background: 'var(--surface)',
    borderRadius: 'var(--radius)',
    padding: '2rem 2.5rem',
    boxShadow: 'var(--shadow)',
    border: '1px solid var(--border)',
    animation: 'slideIn 0.3s ease',
  },
  category: {
    fontSize: '0.72rem',
    fontWeight: 600,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    color: 'var(--accent)',
    marginBottom: '0.75rem',
  },
  text: {
    fontSize: '1.15rem',
    fontWeight: 600,
    lineHeight: 1.5,
    color: 'var(--text-primary)',
    marginBottom: '1.75rem',
  },
  options: {
    display: 'flex',
    flexDirection: 'column',
    gap: '0.6rem',
  },
  option: {
    display: 'flex',
    alignItems: 'center',
    gap: '0.875rem',
    padding: '0.875rem 1.125rem',
    borderRadius: '10px',
    border: '1.5px solid var(--border)',
    background: 'var(--surface)',
    cursor: 'pointer',
    textAlign: 'left',
    transition: 'all 0.18s ease',
    outline: 'none',
    width: '100%',
  },
  optionSelected: {
    borderColor: 'var(--accent)',
    background: 'var(--accent-light)',
  },
  optionLabel: {
    fontSize: '0.9375rem',
    fontWeight: 500,
    color: 'var(--text-primary)',
  },
  radio: {
    width: '18px',
    height: '18px',
    borderRadius: '50%',
    border: '2px solid var(--border)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    flexShrink: 0,
    transition: 'border-color 0.18s ease',
  },
  radioSelected: {
    borderColor: 'var(--accent)',
  },
  radioDot: {
    width: '8px',
    height: '8px',
    borderRadius: '50%',
    background: 'var(--accent)',
  },
};
