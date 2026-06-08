import { useState, useEffect } from 'react';
import Sidebar from './components/Sidebar';
import QuestionCard from './components/QuestionCard';
import ProgressBar from './components/ProgressBar';
import ResultCard from './components/ResultCard';
import PatternExplorer from './components/PatternExplorer';
import HowItWorks from './components/HowItWorks';
import PayloadLibrary from './components/PayloadLibrary';
import OPAPolicyGenerator from './components/OPAPolicyGenerator';

// Detect mobile on mount and re-check on resize
function useIsMobile() {
  const [isMobile, setIsMobile] = useState(() => window.innerWidth < 768);
  useEffect(() => {
    const handler = () => setIsMobile(window.innerWidth < 768);
    window.addEventListener('resize', handler);
    return () => window.removeEventListener('resize', handler);
  }, []);
  return isMobile;
}

// Strip the trailing slash from BASE_URL so path joins don't produce double slashes.
// BASE_URL is set by Vite from vite.config.js `base` — single source of truth.
// Dev: '/'  →  APP_BASE = ''  →  paths become '/quiz', '/result', …
// K8s: '/api/pattern/assessment/'  →  APP_BASE = '/api/pattern/assessment'
const APP_BASE = import.meta.env.BASE_URL.replace(/\/$/, '');

const VIEW_PATHS = {
  quiz:           `${APP_BASE}/quiz`,
  result:         `${APP_BASE}/result`,
  explorer:       `${APP_BASE}/explorer`,
  howItWorks:     `${APP_BASE}/how-it-works`,
  payloadLibrary: `${APP_BASE}/payload-library`,
  opaPolicy:      `${APP_BASE}/opa-generator`,
};

const PATH_TO_VIEW = Object.fromEntries(
  Object.entries(VIEW_PATHS).map(([view, path]) => [path, view])
);

function viewFromPath(pathname) {
  return PATH_TO_VIEW[pathname] || 'quiz';
}

export default function App() {
  // ── Quiz state ──────────────────────────────────────────────────────────────
  const [questions, setQuestions] = useState([]);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [answers, setAnswers] = useState({});
  const [result, setResult] = useState(null);
  const [status, setStatus] = useState('loading'); // loading | ready | submitting | done | error
  const [error, setError] = useState(null);

  // ── Navigation state ────────────────────────────────────────────────────────
  const [currentView, setCurrentView] = useState(() => viewFromPath(window.location.pathname));
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const isMobile = useIsMobile();

  // ── Theme state — persisted in localStorage, applied as data-* on <html> ──
  // Migrate anyone who had the old 'dark' mode saved → 'dim'
  const [theme, setTheme] = useState(() => {
    const stored = localStorage.getItem('pa-theme') || 'light';
    return stored === 'dark' ? 'dim' : stored;
  });
  const [color, setColor] = useState(() => localStorage.getItem('pa-color') || 'violet');

  useEffect(() => {
    const root = document.documentElement;
    // 'light' = no attribute (default :root vars); any other value = set the attribute
    theme === 'light' ? root.removeAttribute('data-theme') : root.setAttribute('data-theme', theme);
    localStorage.setItem('pa-theme', theme);
  }, [theme]);

  useEffect(() => {
    const root = document.documentElement;
    color !== 'violet' ? root.setAttribute('data-color', color) : root.removeAttribute('data-color');
    localStorage.setItem('pa-color', color);
  }, [color]);

  // ── Sync URL on first load (redirect / → quiz path) ─────────────────────────
  useEffect(() => {
    if (!PATH_TO_VIEW[window.location.pathname]) {
      window.history.replaceState({}, '', VIEW_PATHS.quiz);
    }
  }, []);

  // ── Handle browser back/forward ──────────────────────────────────────────────
  useEffect(() => {
    function onPopState() {
      setCurrentView(viewFromPath(window.location.pathname));
    }
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, []);

  // ── Fetch questions on mount ─────────────────────────────────────────────────
  useEffect(() => {
    fetch(`${import.meta.env.BASE_URL}questions`)
      .then((res) => { if (!res.ok) throw new Error(); return res.json(); })
      .then((data) => { setQuestions(data); setStatus('ready'); })
      .catch(() => {
        setError('Could not load questions. Make sure the backend is running on port 3001.');
        setStatus('error');
      });
  }, []);

  // ── Auto-navigate to result when quiz is done ────────────────────────────────
  useEffect(() => {
    if (status === 'done') {
      window.history.pushState({}, '', VIEW_PATHS.result);
      setCurrentView('result');
    }
  }, [status]);

  // ── Navigation handler ───────────────────────────────────────────────────────
  function handleNavigate(view) {
    // Reset quiz if navigating back to it after completion
    if (view === 'quiz' && status === 'done') {
      setAnswers({});
      setCurrentIndex(0);
      setResult(null);
      setError(null);
      setStatus('ready');
    }
    window.history.pushState({}, '', VIEW_PATHS[view] || VIEW_PATHS.quiz);
    setCurrentView(view);
    setSidebarOpen(false);
  }

  // ── Quiz handlers ────────────────────────────────────────────────────────────
  const currentQuestion = questions[currentIndex];
  const currentAnswer = answers[currentQuestion?.id];
  const totalAnswered = Object.keys(answers).length;
  const allAnswered = totalAnswered === questions.length;
  const progress = questions.length > 0 ? ((currentIndex + 1) / questions.length) * 100 : 0;

  function handleAnswer(questionId, value) {
    setAnswers((prev) => ({ ...prev, [questionId]: value }));
  }

  function handleNext() {
    if (currentAnswer && currentIndex < questions.length - 1) setCurrentIndex((i) => i + 1);
  }

  function handlePrev() {
    if (currentIndex > 0) setCurrentIndex((i) => i - 1);
  }

  function handleKeyDown(e) {
    if (currentView !== 'quiz') return;
    if (e.key === 'ArrowRight' || e.key === 'Enter') handleNext();
    if (e.key === 'ArrowLeft') handlePrev();
  }

  async function handleSubmit() {
    setStatus('submitting');
    setError(null);
    const payload = Object.entries(answers).map(([questionId, value]) => ({
      questionId: Number(questionId),
      value,
    }));
    try {
      const res = await fetch(`${import.meta.env.BASE_URL}submit`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ answers: payload }),
      });
      if (!res.ok) throw new Error();
      const data = await res.json();
      setResult(data);
      setStatus('done');
    } catch {
      setError('Something went wrong. Please try again.');
      setStatus('ready');
    }
  }

  // ── Loading / error screens (no sidebar yet) ─────────────────────────────────
  if (status === 'loading') {
    return (
      <div style={styles.screen}>
        <div style={styles.spinner} />
        <p style={styles.loadingText}>Loading assessment…</p>
      </div>
    );
  }

  if (status === 'error' && !result) {
    return (
      <div style={styles.screen}>
        <p style={styles.errorText}>{error}</p>
      </div>
    );
  }

  // ── Main layout ──────────────────────────────────────────────────────────────
  return (
    <div style={styles.layout} onKeyDown={handleKeyDown} tabIndex={-1}>

      {/* Mobile hamburger */}
      {isMobile && (
        <button
          style={styles.hamburger}
          onClick={() => setSidebarOpen((o) => !o)}
          aria-label="Toggle menu"
        >
          {sidebarOpen ? '✕' : '☰'}
        </button>
      )}

      {/* Sidebar */}
      <Sidebar
        currentView={currentView}
        onNavigate={handleNavigate}
        hasResult={!!result}
        isOpen={sidebarOpen}
        onToggle={() => setSidebarOpen((o) => !o)}
        isMobile={isMobile}
        theme={theme}
        color={color}
        onThemeChange={setTheme}
        onColorChange={setColor}
      />

      {/* Main content area */}
      <main style={styles.main}>
        {/* ── Quiz view ── */}
        {currentView === 'quiz' && (
          <div style={styles.quizPage}>
            <div style={styles.quizContainer}>
              <header style={styles.quizHeader}>
                <h1 style={styles.quizTitle}>Pattern Assessment</h1>
                <p style={styles.quizSubtitle}>Discover your cognitive and work-style pattern</p>
              </header>

              <ProgressBar current={currentIndex + 1} total={questions.length} progress={progress} />

              {currentQuestion && (
                <QuestionCard
                  key={currentQuestion.id}
                  question={currentQuestion}
                  selectedValue={currentAnswer}
                  onAnswer={handleAnswer}
                  index={currentIndex}
                />
              )}

              {error && <p style={styles.errorBanner}>{error}</p>}

              <div style={styles.navBtns}>
                <button
                  style={{ ...styles.btn, ...styles.btnSecondary }}
                  onClick={handlePrev}
                  disabled={currentIndex === 0}
                >
                  ← Back
                </button>

                {currentIndex < questions.length - 1 ? (
                  <button
                    style={{
                      ...styles.btn,
                      ...styles.btnPrimary,
                      opacity: !currentAnswer ? 0.5 : 1,
                      cursor: !currentAnswer ? 'not-allowed' : 'pointer',
                    }}
                    onClick={handleNext}
                    disabled={!currentAnswer}
                  >
                    Next →
                  </button>
                ) : (
                  <button
                    style={{
                      ...styles.btn,
                      ...styles.btnSubmit,
                      opacity: !allAnswered || status === 'submitting' ? 0.6 : 1,
                      cursor: !allAnswered || status === 'submitting' ? 'not-allowed' : 'pointer',
                    }}
                    onClick={handleSubmit}
                    disabled={!allAnswered || status === 'submitting'}
                  >
                    {status === 'submitting' ? (
                      <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <span style={{ ...styles.spinner, width: 16, height: 16, borderWidth: 2 }} />
                        Analyzing…
                      </span>
                    ) : (
                      'Get My Pattern →'
                    )}
                  </button>
                )}
              </div>

              <p style={styles.hint}>
                {totalAnswered} / {questions.length} answered
                {currentAnswer && currentIndex < questions.length - 1 && ' · Press → to continue'}
              </p>
            </div>
          </div>
        )}

        {/* ── Result view ── */}
        {currentView === 'result' && result && (
          <ResultCard
            result={result}
            onRestart={() => handleNavigate('quiz')}
          />
        )}
        {currentView === 'result' && !result && (
          <div style={styles.screen}>
            <p style={styles.loadingText}>No result yet — take the assessment first.</p>
            <button style={{ ...styles.btn, ...styles.btnPrimary, marginTop: '1rem' }} onClick={() => handleNavigate('quiz')}>
              Take Assessment →
            </button>
          </div>
        )}

        {/* ── Pattern explorer view ── */}
        {currentView === 'explorer' && <PatternExplorer />}

        {/* ── How it works view ── */}
        {currentView === 'howItWorks' && <HowItWorks />}

        {/* ── Payload Library view ── */}
        {currentView === 'payloadLibrary' && <PayloadLibrary />}

        {/* ── OPA Policy Generator view ── */}
        {currentView === 'opaPolicy' && <OPAPolicyGenerator />}
      </main>
    </div>
  );
}

const styles = {
  layout: {
    display: 'flex',
    minHeight: '100vh',
    background: 'var(--bg)',
    position: 'relative',
  },
  main: {
    flex: 1,
    overflowY: 'auto',
    minWidth: 0,
  },
  hamburger: {
    position: 'fixed',
    top: '1rem',
    left: '1rem',
    zIndex: 100,
    width: 40,
    height: 40,
    borderRadius: '10px',
    border: '1.5px solid var(--border)',
    background: 'var(--surface)',
    fontSize: '1.1rem',
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontFamily: 'inherit',
    boxShadow: 'var(--shadow)',
    color: 'var(--text-primary)',
  },
  // ── Quiz layout ──
  quizPage: {
    minHeight: '100vh',
    display: 'flex',
    alignItems: 'flex-start',
    justifyContent: 'center',
    padding: '3rem 1.5rem 3rem',
  },
  quizContainer: {
    width: '100%',
    maxWidth: 560,
  },
  quizHeader: {
    textAlign: 'center',
    marginBottom: '2.5rem',
  },
  quizTitle: {
    fontSize: '1.75rem',
    fontWeight: 700,
    color: 'var(--text-primary)',
    letterSpacing: '-0.02em',
  },
  quizSubtitle: {
    marginTop: '0.4rem',
    fontSize: '0.9375rem',
    color: 'var(--text-secondary)',
  },
  navBtns: {
    display: 'flex',
    justifyContent: 'space-between',
    marginTop: '1.5rem',
    gap: '0.75rem',
  },
  btn: {
    padding: '0.8rem 1.5rem',
    borderRadius: '10px',
    fontSize: '0.9375rem',
    fontWeight: 600,
    cursor: 'pointer',
    border: 'none',
    fontFamily: 'inherit',
    transition: 'all 0.18s ease',
  },
  btnSecondary: {
    background: 'var(--surface)',
    color: 'var(--text-secondary)',
    border: '1.5px solid var(--border)',
  },
  btnPrimary: {
    background: 'var(--accent)',
    color: '#fff',
    flex: 1,
  },
  btnSubmit: {
    background: 'linear-gradient(135deg, var(--accent) 0%, var(--accent-dark) 100%)',
    color: '#fff',
    flex: 1,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },
  hint: {
    textAlign: 'center',
    marginTop: '1rem',
    fontSize: '0.78rem',
    color: 'var(--text-secondary)',
    minHeight: '1.2em',
  },
  // ── Shared screens ──
  screen: {
    minHeight: '100vh',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    gap: '1rem',
    padding: '2rem',
  },
  loadingText: {
    color: 'var(--text-secondary)',
    fontSize: '0.9375rem',
  },
  errorText: {
    color: 'var(--error)',
    fontSize: '0.9375rem',
    textAlign: 'center',
    maxWidth: 400,
  },
  errorBanner: {
    marginTop: '1rem',
    padding: '0.75rem 1rem',
    borderRadius: '8px',
    background: '#fef2f2',
    border: '1px solid #fecaca',
    color: 'var(--error)',
    fontSize: '0.875rem',
    textAlign: 'center',
  },
  spinner: {
    width: 32,
    height: 32,
    borderRadius: '50%',
    border: '3px solid var(--accent-light)',
    borderTopColor: 'var(--accent)',
    animation: 'spin 0.8s linear infinite',
  },
};
