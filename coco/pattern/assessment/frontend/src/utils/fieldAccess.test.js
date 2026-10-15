import { describe, it, expect } from 'vitest';
import { resolveFields, collectDecisions, maskLast4, FULL, PARTIAL, DENY } from './fieldAccess.js';

const FIELDS = [
  { key: 'name',   label: 'Name',   sensitive: true, example: 'Jane Whitaker' },
  { key: 'email',  label: 'Email',  sensitive: true, example: 'jane@example.com' },
  { key: 'card',   label: 'Card',   sensitive: true, example: '4111111111114242' },
  { key: 'phone',  label: 'Phone',  sensitive: true, example: '415-555-0172' },
  { key: 'seq_id', label: 'Seq ID', example: '100245' },
];

const access = (...entries) => ({ response: [{ access: entries }] });
const entry  = (path, ...actions) => ({ path, actions: actions.map(action => ({ action })) });
const byKey  = rows => Object.fromEntries(rows.map(r => [r.key, r]));

describe('resolveFields', () => {
  it('applies FULL / PARTIAL / deny-by-default for the documented example', () => {
    const body = access(entry('name', 'FULL'), entry('email', 'FULL'), entry('card', 'PARTIAL'));
    const { rows, warnings } = resolveFields(FIELDS, body);
    const r = byKey(rows);
    expect(r.name).toMatchObject({ access: FULL, display: 'Jane Whitaker' });
    expect(r.email).toMatchObject({ access: FULL, display: 'jane@example.com' });
    expect(r.card).toMatchObject({ access: PARTIAL, display: '************4242' });
    expect(r.phone).toMatchObject({ access: DENY, display: null });
    expect(r.phone.reason).toMatch(/deny-by-default/);
    expect(warnings).toEqual([]);
  });

  it('merges decisions spread across several response[] elements (real provider layout)', () => {
    const body = { response: [
      { access: [entry('name', 'FULL')] },
      { access: [entry('email', 'FULL')] },
      { access: [entry('card', 'FULL')] },
    ] };
    const { rows, warnings } = resolveFields(FIELDS, body);
    const r = byKey(rows);
    expect(r.name).toMatchObject({ access: FULL, display: 'Jane Whitaker' });
    expect(r.email).toMatchObject({ access: FULL, display: 'jane@example.com' });
    expect(r.card).toMatchObject({ access: FULL, display: '4111111111114242' });
    expect(r.phone.access).toBe(DENY);
    expect(warnings).toEqual([]);
  });

  it('most restrictive wins for the same path across response[] elements', () => {
    const body = { response: [{ access: [entry('card', 'FULL')] }, { access: [entry('card', 'PARTIAL')] }] };
    expect(byKey(resolveFields(FIELDS, body).rows).card.access).toBe(PARTIAL);
  });

  it('ignores response[] elements without an access list, with a warning', () => {
    const body = { response: [{ foo: 1 }, { access: [entry('name', 'FULL')] }] };
    const { rows, warnings } = resolveFields(FIELDS, body);
    expect(byKey(rows).name.access).toBe(FULL);
    expect(warnings.some(w => w.includes('no "access" list'))).toBe(true);
  });

  it('most restrictive wins within one actions list', () => {
    const r = byKey(resolveFields(FIELDS, access(entry('name', 'FULL', 'PARTIAL'))).rows);
    expect(r.name.access).toBe(PARTIAL);
  });

  it('combines the same path across access entries', () => {
    const r = byKey(resolveFields(FIELDS, access(entry('name', 'FULL'), entry('name', 'PARTIAL'))).rows);
    expect(r.name.access).toBe(PARTIAL);
  });

  it('empty actions and unknown actions fail closed', () => {
    const body = access(entry('name'), entry('email', 'ALLOW'), entry('card', 'FULL', 'MAYBE'));
    const { rows, warnings } = resolveFields(FIELDS, body);
    const r = byKey(rows);
    expect(r.name.access).toBe(DENY);
    expect(r.email.access).toBe(DENY);
    expect(r.card.access).toBe(DENY);
    expect(warnings.some(w => w.includes('ALLOW'))).toBe(true);
    expect(warnings.some(w => w.includes('MAYBE'))).toBe(true);
  });

  it('unrecognised shape restricts every sensitive field and warns', () => {
    for (const body of [null, {}, { response: [] }, { response: [{}] }, 'not json', { access: [] }]) {
      const { rows, warnings } = resolveFields(FIELDS, body);
      rows.filter(x => x.sensitive).forEach(x => expect(x.access).toBe(DENY));
      expect(warnings[0]).toMatch(/not recognised/);
    }
  });

  it('accepts the body as a JSON string', () => {
    const r = byKey(resolveFields(FIELDS, JSON.stringify(access(entry('name', 'FULL')))).rows);
    expect(r.name.access).toBe(FULL);
  });

  it('non-sensitive fields are always shown, whatever the response says', () => {
    for (const body of [null, access(entry('seq_id', 'DENY'))]) {
      const r = byKey(resolveFields(FIELDS, body).rows);
      expect(r.seq_id).toMatchObject({ access: null, display: '100245' });
    }
  });

  it('warns about paths not declared in fields', () => {
    const { warnings } = resolveFields(FIELDS, access(entry('ssn', 'FULL')));
    expect(warnings.some(w => w.includes('ssn'))).toBe(true);
  });

  it('action strings are case-insensitive', () => {
    const r = byKey(resolveFields(FIELDS, access(entry('name', 'full'))).rows);
    expect(r.name.access).toBe(FULL);
  });
});

describe('collectDecisions', () => {
  it('ignores access entries with no path', () => {
    const { decisions, warnings } = collectDecisions(access({ actions: [{ action: 'FULL' }] }));
    expect(decisions.size).toBe(0);
    expect(warnings).toHaveLength(1);
  });
});

describe('maskLast4', () => {
  it('keeps the last 4 of long values', () => {
    expect(maskLast4('415-555-0172')).toBe('********0172');
  });
  it('fully masks values of 4 chars or fewer', () => {
    expect(maskLast4('Doe')).toBe('***');
    expect(maskLast4('1234')).toBe('****');
  });
  it('handles empty and nullish values', () => {
    expect(maskLast4('')).toBe('');
    expect(maskLast4(null)).toBe('');
    expect(maskLast4(undefined)).toBe('');
  });
});
