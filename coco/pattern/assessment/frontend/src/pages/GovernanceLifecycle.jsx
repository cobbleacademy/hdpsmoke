import { useState, useEffect, useMemo } from 'react';

const BASE = import.meta.env.BASE_URL;

const TABS = [
  { key: 'swimlane', label: 'Swimlane Presentation' },
  { key: 'raci', label: 'RACI Matrix' },
  { key: 'sop', label: 'SOP Runbook' },
];

// ── Accent palette ────────────────────────────────────────────────────────────
// Per-team/per-phase identity colors, independent of the app's theme tokens —
// these are a stable visual index (so a team's color never shifts when the
// user switches app theme), unlike --accent/--success/etc. which are
// theme-reactive. 19 entries total: 8 in active use by the seed content
// (see governanceLifecycleConfigService.js's DEFAULT_CONFIG), 11 reserved
// for future teams/phases. Picking an unknown colorKey falls back to 'slate'.
const ACCENT_PALETTE = {
  blue:    { text: '#2563eb', tint: '#eff6ff', dot: '#3b82f6' },
  amber:   { text: '#b45309', tint: '#fffbeb', dot: '#f59e0b' },
  indigo:  { text: '#4338ca', tint: '#eef2ff', dot: '#6366f1' },
  emerald: { text: '#047857', tint: '#ecfdf5', dot: '#10b981' },
  rose:    { text: '#be123c', tint: '#fff1f2', dot: '#f43f5e' },
  violet:  { text: '#6d28d9', tint: '#f5f3ff', dot: '#8b5cf6' },
  sky:     { text: '#0369a1', tint: '#f0f9ff', dot: '#0ea5e9' },
  teal:    { text: '#0f766e', tint: '#f0fdfa', dot: '#14b8a6' },
  slate:   { text: '#475569', tint: '#f8fafc', dot: '#94a3b8' },
  red:     { text: '#b91c1c', tint: '#fef2f2', dot: '#ef4444' },
  orange:  { text: '#c2410c', tint: '#fff7ed', dot: '#f97316' },
  yellow:  { text: '#a16207', tint: '#fefce8', dot: '#eab308' },
  lime:    { text: '#4d7c0f', tint: '#f7fee7', dot: '#84cc16' },
  green:   { text: '#15803d', tint: '#f0fdf4', dot: '#22c55e' },
  cyan:    { text: '#0e7490', tint: '#ecfeff', dot: '#06b6d4' },
  purple:  { text: '#7e22ce', tint: '#faf5ff', dot: '#a855f7' },
  fuchsia: { text: '#a21caf', tint: '#fdf4ff', dot: '#d946ef' },
  pink:    { text: '#be185d', tint: '#fdf2f8', dot: '#ec4899' },
  gray:    { text: '#4b5563', tint: '#f9fafb', dot: '#9ca3af' },
};

// Converts a '#rrggbb' hex string + alpha (0-1) into an 'rgba(...)' string —
// used to derive translucent borders/rings from the solid `dot` hex above
// without hand-maintaining a third hex per palette entry.
function hexA(hex, alpha) {
  const h = hex.replace('#', '');
  const r = parseInt(h.substring(0, 2), 16);
  const g = parseInt(h.substring(2, 4), 16);
  const b = parseInt(h.substring(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function resolveColors(colorKey) {
  return ACCENT_PALETTE[colorKey] || ACCENT_PALETTE.slate;
}

// Badge color priority when a cell holds a combined role string (e.g. "RA",
// "CI") — colored by whichever role is most significant, in this order.
const ROLE_PRIORITY = ['A', 'R', 'C', 'I'];
const ROLE_COLOR_KEYS = { R: 'sky', A: 'rose', C: 'violet', I: 'slate' };
function dominantRole(letters) {
  return ROLE_PRIORITY.find((r) => letters.includes(r)) || null;
}

// ── Client-side mirror of the backend's validateConfig ────────────────────────
// Same invariants enforced server-side (governanceLifecycleConfigService.js) —
// checked here too so the editor can show errors before the network round
// trip, not instead of the backend check.
function validateConfig(config) {
  const errors = [];
  if (!config || typeof config !== 'object') return ['Config must be a JSON object'];
  if (!Array.isArray(config.teams) || config.teams.length === 0) {
    return ['"teams" must be a non-empty array'];
  }
  if (!Array.isArray(config.phases)) errors.push('"phases" must be an array');
  if (!config.raci || !Array.isArray(config.raci.rows)) errors.push('"raci.rows" must be an array');

  const teamIds = config.teams.map((t) => t.id);
  const teamIdSet = new Set(teamIds);
  if (teamIdSet.size !== teamIds.length) {
    errors.push('Duplicate team id found in "teams" — every team id must be unique');
  }

  (config.phases || []).forEach((phase, i) => {
    (phase.teamIds || []).forEach((id) => {
      if (!teamIdSet.has(id)) errors.push(`phases[${i}] ("${phase.title || phase.id}") references unknown team id "${id}"`);
    });
  });

  (config.raci?.rows || []).forEach((row, i) => {
    Object.keys(row.assignments || {}).forEach((id) => {
      if (!teamIdSet.has(id)) errors.push(`raci.rows[${i}] ("${row.activity}") references unknown team id "${id}"`);
    });
    const accountableCount = Object.values(row.assignments || {}).filter((v) => v.includes('A')).length;
    if (accountableCount !== 1) {
      errors.push(`raci.rows[${i}] ("${row.activity}") has ${accountableCount} Accountable owner(s) — expected exactly 1`);
    }
  });

  return errors;
}

// ── Responsive helper ──────────────────────────────────────────────────────────
// Mirrors the breakpoint-driven layout switches the source dashboard did via
// Tailwind's `lg:` prefix (1024px) — swimlane direction, connector arrow
// orientation, and the SOP nav/content grid all switch at this width.
function useIsNarrow(breakpoint = 1024) {
  const [narrow, setNarrow] = useState(() => window.innerWidth < breakpoint);
  useEffect(() => {
    function onResize() {
      setNarrow(window.innerWidth < breakpoint);
    }
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, [breakpoint]);
  return narrow;
}

// ── Small presentational pieces ────────────────────────────────────────────────

function StatusChip({ active, content }) {
  return (
    <span style={{ ...s.statusChip, ...(active ? s.statusChipActive : s.statusChipNominal) }}>
      <span style={{ ...s.statusDot, ...(active ? s.statusDotActive : s.statusDotNominal) }} />
      {active ? content.rejectionLoop.statusActiveLabel : content.rejectionLoop.statusNominalLabel}
    </span>
  );
}

function RejectionToggle({ active, onToggle, content }) {
  return (
    <button type="button" onClick={onToggle} style={{ ...s.toggleBtn, ...(active ? s.toggleBtnActive : {}) }}>
      <span style={{ ...s.toggleLabel, ...(active ? s.toggleLabelActive : {}) }}>
        {content.app.rejectionToggleLabel}
      </span>
      <span style={{ ...s.toggleTrack, ...(active ? s.toggleTrackActive : {}) }}>
        <span style={{ ...s.toggleThumb, ...(active ? s.toggleThumbActive : {}) }} />
      </span>
    </button>
  );
}

function Connector({ isNarrow }) {
  return (
    <div style={s.connector}>
      <svg
        style={{ width: 22, height: 22, color: 'var(--text-secondary)', transform: isNarrow ? 'rotate(90deg)' : 'none' }}
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
      >
        <path strokeLinecap="round" strokeLinejoin="round" d="M4 12h14m0 0l-5-5m5 5l-5 5" />
      </svg>
    </div>
  );
}

function PhaseCard({ phase, isSelected, isFlagged, onSelect, isNarrow, content }) {
  const c = phase.colors;
  return (
    <button
      type="button"
      onClick={onSelect}
      style={{
        ...s.phaseCard,
        borderLeftColor: c.dot,
        borderLeftWidth: isSelected ? 6 : 4,
        ...(isSelected ? { boxShadow: `0 0 0 1px ${hexA(c.dot, 0.3)}, var(--shadow)` } : {}),
        ...(isNarrow ? { minWidth: '100%' } : {}),
      }}
    >
      {isFlagged && <span style={s.phaseFlag}>{content.rejectionLoop.cardFlagLabel}</span>}
      <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
        <span style={{ ...s.badge, background: c.tint, color: c.text, borderColor: hexA(c.dot, 0.35) }}>
          {phase.team}
        </span>
      </div>
      <h3 style={s.phaseTitle}>
        <span style={{ fontFamily: 'ui-monospace, SFMono-Regular, monospace', color: c.text }}>{phase.code}.</span>{' '}
        {phase.title}
      </h3>
      <p style={{ ...s.phaseSubtitle, color: c.text }}>{phase.subtitle}</p>
      <p style={s.phaseTeaser}>{phase.teaser}</p>
      <span style={s.phaseViewMore}>
        View operational details
        <svg style={{ width: 12, height: 12 }} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
        </svg>
      </span>
    </button>
  );
}

function FieldLabel({ label, helper }) {
  return (
    <h4 style={s.fieldLabel}>
      {label} <span style={s.fieldLabelHelper}>— {helper}</span>
    </h4>
  );
}

function PhaseDrawer({ phase, rejectionLoop, content }) {
  if (!phase) {
    return (
      <div style={s.drawerEmpty}>
        <p style={s.mutedText}>
          Select a phase card above to inspect full operational scope, activities, and ownership detail.
        </p>
      </div>
    );
  }
  const c = phase.colors;
  const isFlagged = rejectionLoop && phase.receivesRemediationUpdates;
  return (
    <div style={{ ...s.drawer, borderLeftColor: c.dot }}>
      <div style={s.drawerHead}>
        <div>
          <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 10 }}>
            <span style={{ ...s.drawerCode, color: c.text }}>{phase.code}</span>
            <span style={{ ...s.badge, background: c.tint, color: c.text, borderColor: hexA(c.dot, 0.35) }}>
              {phase.team}
            </span>
            {isFlagged && <span style={s.drawerFlag}>{content.rejectionLoop.drawerFlagLabel}</span>}
          </div>
          <h2 style={s.drawerTitle}>{phase.title}</h2>
          <p style={{ ...s.drawerSubtitle, color: c.text }}>{phase.subtitle}</p>
        </div>
        <div style={s.drawerMeta}>
          <p>
            OWNER <span style={s.mutedItalic}>— accountable role, not the RACI A</span>
          </p>
          <p style={s.drawerMetaValue}>{phase.owner}</p>
          <p style={{ marginTop: 8 }}>
            SLA <span style={s.mutedItalic}>— expected turnaround, normal conditions</span>
          </p>
          <p style={s.drawerMetaValue}>{phase.sla}</p>
        </div>
      </div>

      <p style={s.drawerScope}>{phase.scope}</p>

      <div style={s.drawerGrid}>
        <div>
          <FieldLabel label="Key Activities" helper="steps this team executes before handoff" />
          <ul style={s.plainList}>
            {phase.activities.map((a, i) => (
              <li key={i} style={s.dotListItem}>
                <span style={{ ...s.dot, background: c.dot }} />
                <span>{a}</span>
              </li>
            ))}
          </ul>
        </div>
        <div>
          <FieldLabel label="Inputs" helper="artifacts and context this phase consumes" />
          <ul style={s.plainList}>
            {phase.inputs.map((a, i) => (
              <li key={i} style={s.bulletListItem}>• {a}</li>
            ))}
          </ul>
          <div style={{ marginTop: 20 }}>
            <FieldLabel label="Outputs" helper="artifacts handed to the next phase" />
          </div>
          <ul style={s.plainList}>
            {phase.outputs.map((a, i) => (
              <li key={i} style={s.bulletListItem}>• {a}</li>
            ))}
          </ul>
        </div>
        <div>
          <FieldLabel label="Systems & Tooling" helper="systems of record touched this phase" />
          <ul style={s.plainList}>
            {phase.systems.map((a, i) => (
              <li key={i} style={s.monoListItem}>{a}</li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}

function SwimlaneView({ phases, rejectionLoop, selectedPhaseId, setSelectedPhaseId, isNarrow, content }) {
  const selectedPhase = phases.find((p) => p.id === selectedPhaseId) || null;
  return (
    <section>
      <p style={s.intro}>{content.app.swimlaneIntro}</p>
      {rejectionLoop && (
        <div style={s.rejectionBanner}>
          <span style={{ fontSize: '1.3rem', lineHeight: 1 }}>🔄</span>
          <div>
            <p style={s.rejectionBannerTitle}>{content.rejectionLoop.swimlaneBannerTitle}</p>
            <p style={s.rejectionBannerBody}>{content.rejectionLoop.swimlaneBannerBody}</p>
          </div>
        </div>
      )}

      <div style={{ ...s.swimlaneRow, flexDirection: isNarrow ? 'column' : 'row' }}>
        {phases.map((phase, idx) => (
          <div key={phase.id} style={{ display: 'contents' }}>
            <PhaseCard
              phase={phase}
              isSelected={selectedPhaseId === phase.id}
              isFlagged={rejectionLoop && phase.receivesRemediationUpdates}
              onSelect={() => setSelectedPhaseId(phase.id)}
              isNarrow={isNarrow}
              content={content}
            />
            {idx < phases.length - 1 && <Connector isNarrow={isNarrow} />}
          </div>
        ))}
      </div>

      <PhaseDrawer phase={selectedPhase} rejectionLoop={rejectionLoop} content={content} />
    </section>
  );
}

function RaciBadge({ letters }) {
  if (!letters) {
    return <span style={s.raciBadgeEmpty}>—</span>;
  }
  const role = dominantRole(letters);
  const c = resolveColors(ROLE_COLOR_KEYS[role] || 'slate');
  return (
    <span style={{ ...s.raciBadge, background: c.tint, color: c.text, borderColor: hexA(c.dot, 0.35) }}>
      {letters}
    </span>
  );
}

function RaciView({ teams, raciRows, raciDefinitions, content }) {
  return (
    <section>
      <p style={s.intro}>{content.raci.intro}</p>

      <div style={s.raciTableWrap}>
        <table style={s.raciTable}>
          <thead>
            <tr style={s.raciHeadRow}>
              <th style={s.raciHeadCellLeft}>Governance Activity</th>
              {teams.map((team) => (
                <th key={team.id} style={{ ...s.raciHeadCell, color: team.colors.text }}>
                  {team.shortLabel}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {raciRows.map((row, i) => (
              <tr key={i} data-raci-row style={i % 2 === 0 ? s.raciRowEven : s.raciRowOdd}>
                <td style={s.raciActivityCell}>{row.activity}</td>
                {teams.map((team) => (
                  <td key={team.id} style={s.raciDataCell}>
                    <RaciBadge letters={row.assignments[team.id]} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={s.raciLegend}>
        {raciDefinitions.map((d) => {
          const c = resolveColors(ROLE_COLOR_KEYS[d.letter] || 'slate');
          return (
            <div key={d.letter} style={s.raciLegendItem}>
              <span style={{ ...s.raciBadge, background: c.tint, color: c.text, borderColor: hexA(c.dot, 0.35) }}>
                {d.letter}
              </span>
              <div>
                <p style={s.raciLegendLabel}>{d.label}</p>
                <p style={s.raciLegendDesc}>{d.desc}</p>
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function RejectionDirectiveCallout({ active, content }) {
  if (active) {
    const d = content.rejectionLoop.directiveActive;
    return (
      <div style={s.directiveActive}>
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: '1.1rem', lineHeight: 1 }}>⚠️</span>
          <h4 style={s.directiveTitleActive}>{d.title}</h4>
          <span style={{ ...s.directiveBadge, marginLeft: 'auto' }}>{d.badge}</span>
        </div>
        <p style={s.directiveBodyActive}>{d.body}</p>
        <ul style={s.directiveBullets}>
          {d.bullets.map((b, i) => <li key={i}>{b}</li>)}
        </ul>
      </div>
    );
  }
  const d = content.rejectionLoop.directiveStandby;
  return (
    <div style={s.directiveStandby}>
      <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8 }}>
        <span style={{ fontSize: '1.1rem', lineHeight: 1, opacity: 0.5 }}>⚠️</span>
        <h4 style={s.directiveTitleStandby}>{d.title}</h4>
        <span style={{ ...s.directiveBadgeStandby, marginLeft: 'auto' }}>{d.badge}</span>
      </div>
      <p style={s.directiveBodyStandby}>{d.body}</p>
    </div>
  );
}

function SopView({ sections, activeSection, setActiveSection, rejectionLoop, isNarrow, content }) {
  const section = sections.find((sec) => sec.id === activeSection) || sections[0];
  return (
    <section style={{ display: 'grid', gap: 20, gridTemplateColumns: isNarrow ? '1fr' : '260px 1fr' }}>
      <nav style={s.sopNav}>
        <p style={s.sopNavHeading}>Runbook Index</p>
        <ul style={{ listStyle: 'none', margin: '4px 0 0', padding: 0, display: 'flex', flexDirection: 'column', gap: 4 }}>
          {sections.map((sec) => (
            <li key={sec.id}>
              <button
                type="button"
                onClick={() => setActiveSection(sec.id)}
                style={{ ...s.sopNavItem, ...(sec.id === section.id ? s.sopNavItemActive : {}) }}
              >
                {sec.label}
              </button>
            </li>
          ))}
        </ul>
      </nav>

      <div style={s.sopContent}>
        <p style={s.sopKicker}>Trigger Condition</p>
        <p style={s.sopTrigger}>{section.trigger}</p>

        <h2 style={s.sopHeading}>{section.label}</h2>
        <p style={s.sopIntro}>{section.intro}</p>

        <h3 style={s.sopSubheading}>Operational Steps</h3>
        <ol style={{ margin: '10px 0 0', padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: 10 }}>
          {section.steps.map((step, i) => (
            <li key={i} style={s.sopStep}>
              <span style={s.sopStepNum}>{i + 1}</span>
              <span>{step}</span>
            </li>
          ))}
        </ol>

        <div style={{ marginTop: 24 }}>
          <FieldLabel label="Exit Criteria" helper="conditions that close this phase and hand control forward" />
        </div>
        <ul style={s.plainList}>
          {section.exit.map((e, i) => (
            <li key={i} style={s.dotListItem}>
              <span style={{ ...s.dot, background: 'var(--success)' }} />
              <span>{e}</span>
            </li>
          ))}
        </ul>

        <RejectionDirectiveCallout active={rejectionLoop} content={content} />
      </div>
    </section>
  );
}

// ─────────────────────────────────────────────────────────────────────────────

export default function GovernanceLifecycle() {
  const [loadStatus, setLoadStatus] = useState('loading'); // loading | ready | error
  const [loadError, setLoadError] = useState(null);
  const [content, setContent] = useState(null);
  const [usingDefault, setUsingDefault] = useState(false);
  const [writeAuthRequired, setWriteAuthRequired] = useState(false);
  const [adminToken, setAdminToken] = useState('');

  const [activeTab, setActiveTab] = useState('swimlane');
  const [selectedPhaseId, setSelectedPhaseId] = useState(null);
  const [rejectionLoop, setRejectionLoop] = useState(false);
  const [sopSection, setSopSection] = useState(null);
  const isNarrow = useIsNarrow();

  // ── Editor state ─────────────────────────────────────────────────────────
  const [editorOpen, setEditorOpen] = useState(false);
  const [editorText, setEditorText] = useState('');
  const [editorErrors, setEditorErrors] = useState([]);
  const [saveState, setSaveState] = useState('idle'); // idle | saving | saved | error
  const [saveError, setSaveError] = useState('');

  function loadConfig() {
    setLoadStatus('loading');
    setLoadError(null);
    fetch(`${BASE}governance-lifecycle-config`)
      .then((r) => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then((data) => {
        setContent(data.config);
        setUsingDefault(Boolean(data.usingDefault));
        setWriteAuthRequired(Boolean(data.writeAuthRequired));
        setSelectedPhaseId(data.config.phases?.[0]?.id ?? null);
        setSopSection(data.config.sop?.sections?.[0]?.id ?? null);
        setLoadStatus('ready');
      })
      .catch((err) => {
        setLoadError(err.message || 'Could not load Governance Lifecycle config — is the backend running?');
        setLoadStatus('error');
      });
  }

  useEffect(() => { loadConfig(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Derived data (recomputed only when content changes) ───────────────────
  const teamsById = useMemo(() => (
    content ? Object.fromEntries(content.teams.map((t) => [t.id, t])) : {}
  ), [content]);

  const teams = useMemo(() => (
    content ? content.teams.map((t) => ({ ...t, colors: resolveColors(t.colorKey) })) : []
  ), [content]);

  const phases = useMemo(() => (
    content
      ? content.phases.map((p) => ({
          ...p,
          team: p.teamIds.map((id) => teamsById[id]?.shortLabel || id.toUpperCase()).join(' + '),
          colors: resolveColors(p.colorKey),
        }))
      : []
  ), [content, teamsById]);

  function handleOpenEditor() {
    setEditorText(JSON.stringify(content, null, 2));
    setEditorErrors([]);
    setSaveState('idle');
    setSaveError('');
    setEditorOpen(true);
  }

  function handleEditorChange(val) {
    setEditorText(val);
    setSaveState('idle');
    try {
      const parsed = JSON.parse(val);
      setEditorErrors(validateConfig(parsed));
    } catch (e) {
      setEditorErrors([`Invalid JSON: ${e.message}`]);
    }
  }

  async function handleSaveEditor() {
    let parsed;
    try {
      parsed = JSON.parse(editorText);
    } catch (e) {
      setEditorErrors([`Invalid JSON: ${e.message}`]);
      return;
    }
    const errors = validateConfig(parsed);
    if (errors.length > 0) {
      setEditorErrors(errors);
      return;
    }

    setSaveState('saving');
    setSaveError('');
    try {
      const headers = { 'Content-Type': 'application/json' };
      if (writeAuthRequired && adminToken) headers['Authorization'] = `Bearer ${adminToken}`;
      const resp = await fetch(`${BASE}governance-lifecycle-config`, {
        method: 'PUT',
        headers,
        body: JSON.stringify({ config: parsed }),
      });
      const data = await resp.json();
      if (!resp.ok) {
        setSaveState('error');
        setSaveError(data.error || `HTTP ${resp.status}`);
        if (data.validationErrors) setEditorErrors(data.validationErrors);
        return;
      }
      setSaveState('saved');
      setEditorOpen(false);
      loadConfig();
    } catch (err) {
      setSaveState('error');
      setSaveError(err.message || 'Network error');
    }
  }

  if (loadStatus === 'loading') {
    return (
      <div style={s.container}>
        <div style={s.pageHeader}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <span style={s.headerIcon}>⚖️</span>
            <div style={s.headerTitle}>Governance Lifecycle</div>
          </div>
        </div>
        <div style={s.centerScreen}>
          <div style={s.spinner} />
          <p style={s.mutedText}>Loading governance lifecycle content…</p>
        </div>
      </div>
    );
  }

  if (loadStatus === 'error') {
    return (
      <div style={s.container}>
        <div style={s.pageHeader}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <span style={s.headerIcon}>⚖️</span>
            <div style={s.headerTitle}>Governance Lifecycle</div>
          </div>
        </div>
        <div style={s.centerScreen}>
          <p style={{ ...s.mutedText, color: 'var(--error)' }}>{loadError}</p>
        </div>
      </div>
    );
  }

  return (
    <div style={s.container}>
      <div style={s.pageHeader}>
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
          <div>
            <p style={s.kicker}>{content.app.kicker}</p>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flexWrap: 'wrap' }}>
              <span style={s.headerIcon}>⚖️</span>
              <div style={s.headerTitle}>{content.app.title}</div>
              {usingDefault && (
                <span style={s.defaultBadge} title="No saved config yet — showing the built-in seed content">
                  Using default content
                </span>
              )}
            </div>
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 10 }}>
            <StatusChip active={rejectionLoop} content={content} />
            <RejectionToggle active={rejectionLoop} onToggle={() => setRejectionLoop((v) => !v)} content={content} />
            <button
              onClick={editorOpen ? () => setEditorOpen(false) : handleOpenEditor}
              style={{ ...s.editBtn, ...(editorOpen ? s.editBtnActive : {}) }}
            >
              {editorOpen ? '✕ Close Editor' : '✎ Edit Content'}
            </button>
          </div>
        </div>
      </div>

      {editorOpen && (
        <div style={s.editorPanel}>
          <div style={s.editorHeadRow}>
            <span style={s.sectionLabel}>Governance Lifecycle JSON — edit and save without a rebuild</span>
            {writeAuthRequired && (
              <input
                type="password"
                placeholder="Admin token"
                value={adminToken}
                onChange={(e) => setAdminToken(e.target.value)}
                style={s.adminTokenInput}
              />
            )}
            <button
              onClick={handleSaveEditor}
              disabled={editorErrors.length > 0 || saveState === 'saving'}
              style={{ ...s.saveBtn, ...(editorErrors.length > 0 || saveState === 'saving' ? s.saveBtnDisabled : {}) }}
            >
              {saveState === 'saving' ? 'Saving…' : '💾 Save'}
            </button>
          </div>
          <textarea
            style={s.editorTextarea}
            value={editorText}
            onChange={(e) => handleEditorChange(e.target.value)}
            spellCheck={false}
          />
          {editorErrors.length > 0 && (
            <div style={s.editorErrors}>
              {editorErrors.map((e, i) => <div key={i}>• {e}</div>)}
            </div>
          )}
          {saveState === 'error' && <div style={s.editorErrors}>• {saveError}</div>}
        </div>
      )}

      <div style={s.tabStrip}>
        {TABS.map((tab) => (
          <button
            key={tab.key}
            style={{ ...s.tab, ...(activeTab === tab.key ? s.tabActive : {}) }}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
            {activeTab === tab.key && <span style={s.tabDot} />}
          </button>
        ))}
      </div>

      <div style={s.body}>
        {activeTab === 'swimlane' && (
          <SwimlaneView
            phases={phases}
            rejectionLoop={rejectionLoop}
            selectedPhaseId={selectedPhaseId}
            setSelectedPhaseId={setSelectedPhaseId}
            isNarrow={isNarrow}
            content={content}
          />
        )}
        {activeTab === 'raci' && (
          <RaciView teams={teams} raciRows={content.raci.rows} raciDefinitions={content.raci.definitions} content={content} />
        )}
        {activeTab === 'sop' && (
          <SopView
            sections={content.sop.sections}
            activeSection={sopSection}
            setActiveSection={setSopSection}
            rejectionLoop={rejectionLoop}
            isNarrow={isNarrow}
            content={content}
          />
        )}
      </div>

      <div style={s.footer}>{content.app.footer}</div>
    </div>
  );
}

const s = {
  container: { display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden', background: 'var(--bg)' },
  pageHeader: { padding: '1rem 1.25rem 0.75rem', borderBottom: '1px solid var(--border)', background: 'var(--surface)', flexShrink: 0 },
  kicker: { fontSize: '0.7rem', fontWeight: 700, letterSpacing: '0.06em', textTransform: 'uppercase', color: 'var(--text-secondary)', margin: '0 0 4px', fontFamily: 'ui-monospace, SFMono-Regular, monospace' },
  headerIcon: { fontSize: '1.5rem', lineHeight: 1, flexShrink: 0 },
  headerTitle: { fontSize: '1.25rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.02em' },
  defaultBadge: {
    display: 'inline-block', borderRadius: 999, border: '1px solid var(--border)', background: 'var(--surface-hover)',
    color: 'var(--text-secondary)', fontSize: '0.68rem', fontWeight: 600, padding: '0.2rem 0.6rem',
  },

  centerScreen: { flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 12 },
  spinner: {
    width: 32, height: 32, borderRadius: '50%', border: '3px solid var(--border)', borderTopColor: 'var(--accent)',
    animation: 'spin 0.8s linear infinite',
  },

  editBtn: {
    padding: '0.45rem 0.85rem', borderRadius: 8, border: '1px solid var(--border)', background: 'transparent',
    color: 'var(--text-secondary)', fontSize: '0.8rem', fontWeight: 600, cursor: 'pointer', fontFamily: 'inherit',
  },
  editBtnActive: { background: 'var(--surface-hover)', color: 'var(--text-primary)' },

  editorPanel: { borderBottom: '1px solid var(--border)', background: 'var(--surface)', padding: '1rem 1.25rem', flexShrink: 0 },
  editorHeadRow: { display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 10, marginBottom: 8 },
  sectionLabel: { fontSize: '0.8rem', fontWeight: 700, color: 'var(--text-primary)' },
  adminTokenInput: {
    marginLeft: 'auto', padding: '0.4rem 0.6rem', borderRadius: 8, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)', fontSize: '0.78rem', outline: 'none', fontFamily: 'inherit',
  },
  saveBtn: {
    padding: '0.45rem 0.9rem', borderRadius: 8, border: 'none', background: 'var(--accent)',
    color: '#fff', fontSize: '0.8rem', fontWeight: 700, cursor: 'pointer', fontFamily: 'inherit',
  },
  saveBtnDisabled: { opacity: 0.5, cursor: 'not-allowed' },
  editorTextarea: {
    width: '100%', minHeight: 260, padding: '0.75rem', borderRadius: 8, border: '1.5px solid var(--border)',
    background: 'var(--bg)', color: 'var(--text-primary)', fontFamily: 'ui-monospace, SFMono-Regular, monospace',
    fontSize: '0.78rem', outline: 'none', resize: 'vertical', boxSizing: 'border-box',
  },
  editorErrors: {
    marginTop: 8, padding: '0.6rem 0.75rem', borderRadius: 8, background: 'rgba(220,38,38,0.08)',
    border: '1px solid var(--error)', color: 'var(--error)', fontSize: '0.78rem', lineHeight: 1.6,
  },

  tabStrip: {
    display: 'flex', borderBottom: '1px solid var(--border)', background: 'var(--surface)',
    padding: '0 1rem', flexShrink: 0, overflowX: 'auto',
  },
  tab: {
    padding: '0.6rem 1.1rem', border: 'none', borderBottom: '2.5px solid transparent',
    background: 'transparent', color: 'var(--text-secondary)', fontSize: '0.85rem', fontWeight: 600,
    cursor: 'pointer', fontFamily: 'inherit', position: 'relative',
    display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap', transition: 'color 0.15s',
  },
  tabActive: { color: 'var(--accent)', borderBottom: '2.5px solid var(--accent)' },
  tabDot: { width: 6, height: 6, borderRadius: '50%', background: 'var(--accent)' },

  body: { flex: 1, overflowY: 'auto', padding: '1.25rem' },
  footer: { padding: '0.85rem', textAlign: 'center', fontSize: '0.72rem', color: 'var(--text-secondary)', borderTop: '1px solid var(--border)', fontFamily: 'ui-monospace, SFMono-Regular, monospace', flexShrink: 0 },

  intro: { maxWidth: '48rem', fontSize: '0.85rem', lineHeight: 1.6, color: 'var(--text-secondary)', margin: '0 0 1.25rem' },
  mutedText: { fontSize: '0.85rem', color: 'var(--text-secondary)' },
  mutedItalic: { fontStyle: 'italic', color: 'var(--text-secondary)' },

  // ── Status chip / toggle ────────────────────────────────────────────────────
  statusChip: {
    display: 'inline-flex', alignItems: 'center', gap: 8, borderRadius: 999,
    padding: '0.4rem 0.75rem', fontSize: '0.72rem', fontFamily: 'ui-monospace, SFMono-Regular, monospace', border: '1px solid transparent',
  },
  statusChipActive: { borderColor: hexA('#d97706', 0.4), background: hexA('#d97706', 0.1), color: 'var(--warning)' },
  statusChipNominal: { borderColor: hexA('#059669', 0.35), background: hexA('#059669', 0.1), color: 'var(--success)' },
  statusDot: { width: 8, height: 8, borderRadius: '50%', flexShrink: 0 },
  statusDotActive: { background: 'var(--warning)' },
  statusDotNominal: { background: 'var(--success)' },

  toggleBtn: {
    display: 'flex', alignItems: 'center', gap: 10, borderRadius: 999,
    border: '1px solid var(--border)', background: 'var(--surface-hover)', padding: '0.45rem 0.85rem',
    cursor: 'pointer', transition: 'border-color 0.15s, background 0.15s',
  },
  toggleBtnActive: { borderColor: hexA('#d97706', 0.4), background: hexA('#d97706', 0.12) },
  toggleLabel: { fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-secondary)' },
  toggleLabelActive: { color: 'var(--warning)' },
  toggleTrack: { position: 'relative', width: 36, height: 20, borderRadius: 999, background: 'var(--border)', flexShrink: 0, transition: 'background 0.15s' },
  toggleTrackActive: { background: 'var(--warning)' },
  toggleThumb: { position: 'absolute', left: 2, top: 2, width: 16, height: 16, borderRadius: '50%', background: 'var(--surface)', transition: 'left 0.15s' },
  toggleThumbActive: { left: 18, background: 'var(--text-primary)' },

  // ── Swimlane ─────────────────────────────────────────────────────────────────
  rejectionBanner: {
    display: 'flex', gap: 12, alignItems: 'flex-start', marginBottom: '1.25rem',
    borderRadius: 'var(--radius)', border: `1px solid ${hexA('#d97706', 0.4)}`, background: hexA('#d97706', 0.08), padding: '0.9rem 1.1rem',
  },
  rejectionBannerTitle: { fontSize: '0.78rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.03em', color: 'var(--warning)', margin: 0 },
  rejectionBannerBody: { fontSize: '0.83rem', lineHeight: 1.55, color: 'var(--text-primary)', margin: '4px 0 0' },

  swimlaneRow: { display: 'flex', gap: 16, alignItems: 'stretch', overflowX: 'auto', paddingBottom: 4 },
  connector: { flexShrink: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '4px 0' },

  phaseCard: {
    position: 'relative', minWidth: 220, flex: '1 1 220px', textAlign: 'left', cursor: 'pointer',
    borderRadius: 'var(--radius)', border: '1px solid var(--border)', borderLeftStyle: 'solid',
    background: 'var(--surface)', padding: '1.1rem', transition: 'box-shadow 0.15s, background 0.15s',
    fontFamily: 'inherit',
  },
  phaseFlag: {
    position: 'absolute', top: -12, right: 12, display: 'inline-flex', alignItems: 'center',
    borderRadius: 999, background: 'var(--warning)', color: '#fff', fontSize: '0.68rem', fontWeight: 700,
    padding: '0.3rem 0.6rem', boxShadow: 'var(--shadow)',
  },
  badge: { display: 'inline-block', borderRadius: 999, border: '1px solid', padding: '0.15rem 0.55rem', fontSize: '0.68rem', fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontWeight: 600 },
  phaseTitle: { marginTop: 8, fontSize: '0.95rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.01em' },
  phaseSubtitle: { marginTop: 4, fontSize: '0.8rem', fontWeight: 600 },
  phaseTeaser: { marginTop: 10, fontSize: '0.75rem', lineHeight: 1.55, color: 'var(--text-secondary)' },
  phaseViewMore: { marginTop: 14, display: 'inline-flex', alignItems: 'center', gap: 4, fontSize: '0.7rem', fontFamily: 'ui-monospace, SFMono-Regular, monospace', color: 'var(--text-secondary)' },

  drawerEmpty: { marginTop: '1.5rem', borderRadius: 'var(--radius)', border: '1px dashed var(--border)', background: 'var(--surface)', padding: '2rem', textAlign: 'center' },
  drawer: { marginTop: '1.5rem', borderRadius: 'var(--radius)', border: '1px solid var(--border)', borderLeftWidth: 4, borderLeftStyle: 'solid', background: 'var(--surface)', boxShadow: 'var(--shadow)', padding: '1.5rem' },
  drawerHead: { display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, borderBottom: '1px solid var(--border)', paddingBottom: 18 },
  drawerCode: { fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '0.72rem', letterSpacing: '0.08em' },
  drawerFlag: { display: 'inline-flex', alignItems: 'center', borderRadius: 999, border: `1px solid ${hexA('#d97706', 0.5)}`, background: hexA('#d97706', 0.1), color: 'var(--warning)', fontSize: '0.68rem', fontFamily: 'ui-monospace, SFMono-Regular, monospace', padding: '0.15rem 0.6rem' },
  drawerTitle: { marginTop: 8, fontSize: '1.15rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.01em' },
  drawerSubtitle: { fontSize: '0.82rem', fontWeight: 600 },
  drawerMeta: { textAlign: 'right', fontSize: '0.72rem', fontFamily: 'ui-monospace, SFMono-Regular, monospace', color: 'var(--text-secondary)' },
  drawerMetaValue: { color: 'var(--text-primary)' },
  drawerScope: { marginTop: 18, fontSize: '0.85rem', lineHeight: 1.65, color: 'var(--text-secondary)' },
  drawerGrid: { marginTop: 20, display: 'grid', gap: 22, gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' },

  fieldLabel: { fontSize: '0.68rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em', color: 'var(--text-secondary)', margin: 0 },
  fieldLabelHelper: { fontWeight: 400, fontStyle: 'italic', textTransform: 'none' },

  plainList: { listStyle: 'none', margin: '10px 0 0', padding: 0, display: 'flex', flexDirection: 'column', gap: 8 },
  dotListItem: { display: 'flex', gap: 8, fontSize: '0.82rem', lineHeight: 1.5, color: 'var(--text-primary)' },
  dot: { marginTop: 6, width: 6, height: 6, borderRadius: '50%', flexShrink: 0 },
  bulletListItem: { fontSize: '0.82rem', lineHeight: 1.55, color: 'var(--text-primary)' },
  monoListItem: { fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '0.78rem', lineHeight: 1.55, color: 'var(--text-primary)' },

  // ── RACI ─────────────────────────────────────────────────────────────────────
  // maxHeight + overflow: auto give this table its own bounded scrollport —
  // sticky header cells lock to the top of *this* box rather than the outer
  // page scroll (which the un-bounded overflowX-only wrapper couldn't do:
  // per the CSS overflow spec, setting only one axis to non-visible computes
  // the other axis to auto too, so the box silently became its own never-
  // scrolling viewport and sticky had nothing real to stick to).
  raciTableWrap: { overflow: 'auto', maxHeight: '65vh', borderRadius: 'var(--radius)', border: '1px solid var(--border)', background: 'var(--surface)' },
  raciTable: { width: '100%', minWidth: 640, borderCollapse: 'collapse', fontSize: '0.82rem' },
  raciHeadRow: { background: 'var(--surface-hover)' },
  raciHeadCellLeft: { position: 'sticky', top: 0, zIndex: 1, padding: '0.85rem 1.1rem', textAlign: 'left', fontSize: '0.7rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.03em', color: 'var(--text-secondary)', background: 'var(--surface-hover)', borderBottom: '1px solid var(--border)' },
  raciHeadCell: { position: 'sticky', top: 0, zIndex: 1, padding: '0.85rem 1.1rem', textAlign: 'center', fontSize: '0.7rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.03em', background: 'var(--surface-hover)', borderBottom: '1px solid var(--border)' },
  raciRowEven: { borderBottom: '1px solid var(--border)' },
  raciRowOdd: { borderBottom: '1px solid var(--border)', background: 'var(--surface-hover)' },
  raciActivityCell: { padding: '0.85rem 1.1rem', color: 'var(--text-primary)' },
  raciDataCell: { padding: '0.85rem 1.1rem', textAlign: 'center' },
  raciBadge: { display: 'inline-flex', alignItems: 'center', justifyContent: 'center', minWidth: 32, height: 28, borderRadius: 999, border: '1px solid', fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '0.75rem', fontWeight: 700, padding: '0 0.4rem' },
  raciBadgeEmpty: { display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: 28, height: 28, borderRadius: 999, border: '1px solid var(--border)', color: 'var(--text-secondary)', opacity: 0.5, fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '0.8rem' },
  raciLegend: { marginTop: '1.25rem', display: 'flex', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'center', gap: 20, borderRadius: 'var(--radius)', border: '1px solid var(--border)', background: 'var(--surface)', padding: '1.1rem 1.4rem' },
  raciLegendItem: { display: 'flex', alignItems: 'center', gap: 10 },
  raciLegendLabel: { fontSize: '0.82rem', fontWeight: 700, color: 'var(--text-primary)', margin: 0 },
  raciLegendDesc: { fontSize: '0.72rem', fontStyle: 'italic', color: 'var(--text-secondary)', margin: '2px 0 0' },

  // ── SOP ──────────────────────────────────────────────────────────────────────
  sopNav: { borderRadius: 'var(--radius)', border: '1px solid var(--border)', background: 'var(--surface)', padding: 10, alignSelf: 'start' },
  sopNavHeading: { padding: '0.4rem 0.6rem', fontSize: '0.68rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-secondary)', margin: 0 },
  sopNavItem: {
    display: 'block', width: '100%', textAlign: 'left', borderRadius: 8, border: '1px solid transparent',
    padding: '0.6rem 0.7rem', fontSize: '0.8rem', color: 'var(--text-secondary)', background: 'transparent',
    cursor: 'pointer', fontFamily: 'inherit', transition: 'background 0.15s, color 0.15s',
  },
  sopNavItemActive: { background: 'var(--accent)', color: '#fff', fontWeight: 600 },

  sopContent: { borderRadius: 'var(--radius)', border: '1px solid var(--border)', borderLeftWidth: 4, borderLeftColor: 'var(--border)', borderLeftStyle: 'solid', background: 'var(--surface)', padding: '1.5rem' },
  sopKicker: { fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '0.68rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-secondary)', margin: 0 },
  sopTrigger: { marginTop: 4, fontSize: '0.82rem', fontStyle: 'italic', color: 'var(--text-secondary)' },
  sopHeading: { marginTop: 18, fontSize: '1.3rem', fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.01em' },
  sopIntro: { marginTop: 8, fontSize: '0.85rem', lineHeight: 1.6, color: 'var(--text-secondary)' },
  sopSubheading: { marginTop: 26, fontSize: '0.7rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.04em', color: 'var(--text-secondary)' },
  sopStep: { display: 'flex', gap: 10, fontSize: '0.83rem', color: 'var(--text-primary)' },
  sopStepNum: { display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, width: 22, height: 22, borderRadius: '50%', border: '1px solid var(--border)', background: 'var(--surface-hover)', fontFamily: 'ui-monospace, SFMono-Regular, monospace', fontSize: '0.7rem', color: 'var(--text-secondary)' },

  directiveActive: { marginTop: 26, borderRadius: 'var(--radius)', border: `1px solid ${hexA('#d97706', 0.4)}`, background: hexA('#d97706', 0.08), padding: '1.1rem' },
  directiveTitleActive: { fontSize: '0.78rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.03em', color: 'var(--warning)', margin: 0 },
  directiveBadge: { display: 'inline-flex', alignItems: 'center', borderRadius: 999, border: `1px solid ${hexA('#d97706', 0.4)}`, background: hexA('#d97706', 0.15), color: 'var(--warning)', fontSize: '0.7rem', fontFamily: 'ui-monospace, SFMono-Regular, monospace', padding: '0.3rem 0.65rem' },
  directiveBodyActive: { marginTop: 10, fontSize: '0.83rem', lineHeight: 1.6, color: 'var(--text-primary)' },
  directiveBullets: { margin: '10px 0 0', paddingLeft: 18, display: 'flex', flexDirection: 'column', gap: 6, fontSize: '0.82rem', color: 'var(--text-primary)' },

  directiveStandby: { marginTop: 26, borderRadius: 'var(--radius)', border: '1px solid var(--border)', background: 'var(--surface-hover)', padding: '1.1rem' },
  directiveTitleStandby: { fontSize: '0.78rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.03em', color: 'var(--text-secondary)', margin: 0 },
  directiveBadgeStandby: { display: 'inline-flex', alignItems: 'center', borderRadius: 999, border: '1px solid var(--border)', background: 'var(--surface)', color: 'var(--text-secondary)', fontSize: '0.7rem', fontFamily: 'ui-monospace, SFMono-Regular, monospace', padding: '0.3rem 0.65rem' },
  directiveBodyStandby: { marginTop: 10, fontSize: '0.83rem', lineHeight: 1.6, color: 'var(--text-secondary)' },
};
