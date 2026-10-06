#!/usr/bin/env node
// Detects drift between hsm_bouncy's master sequence-diagram.svg and the
// `messages` entries of HsmDemo.jsx's FLOWS array.
//
// Unlike the architecture-diagram converter, this doesn't try to regenerate
// JSX wholesale -- the SVG's lines/paths/lifeline-x-coordinates would need a
// much heavier geometric parser to reliably re-derive from/to/dashed for
// every message, and FLOWS also carries hand-written narrative `steps` prose
// that has no literal SVG source. Instead this extracts every
// "<stepNum>. <label>" text pair from master and every {stepNum, label} pair
// already in HsmDemo.jsx's messages arrays, normalizes both, and reports:
//   - step numbers present in master but missing (or textually different)
//     from HsmDemo.jsx
//   - step numbers present in HsmDemo.jsx with no matching master step
// This is the same manual technique used mid-session to catch drift (the
// current_key model, the 26a/26b/26c split, the app_grants rename) --
// scripted so it doesn't have to be re-derived by hand every round.
//
// Usage: node scripts/sync-hsm-sequence.js <sequence-diagram.svg> <HsmDemo.jsx>

const fs = require('fs');

function normalize(text) {
  return text
    .replace(/&amp;/g, '&')
    .replace(/[{}]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

// Master step grammar (grew over rounds -- this was the blind spot that hid Sections 7-9 and
// the hsm-file-service flow for several rounds): "12." · "0a." · "15a." · "26c." · "4a2." ·
// "R1." (dotted) and "F11a <label>" (letter-prefixed, no dot, hsm-file-service flow).
const STEP_PREFIX_RE = /^(?:((?:\d+[a-z]?\d*)|(?:R\d+))\.\s*|(F\d+[ab]?)\s+)(.*)$/;

function extractMasterSteps(svgPath) {
  const src = fs.readFileSync(svgPath, 'utf8');
  const textRe = /<text\b[^>]*>([^<]*)<\/text>/g;
  const steps = new Map(); // stepNum -> [labels...]  (a step can legitimately appear twice: request + response)
  let m;
  while ((m = textRe.exec(src))) {
    const raw = normalize(m[1]);
    if (!raw) continue;
    const stepMatch = raw.match(STEP_PREFIX_RE);
    if (!stepMatch) continue;
    const stepNum = stepMatch[1] || stepMatch[2];
    const label = stepMatch[3];
    if (!steps.has(stepNum)) steps.set(stepNum, []);
    steps.get(stepNum).push(label);
  }
  return steps;
}

// Whole-flow check: numbered-step diffing can't see a flow whose lines carry no step number
// (Sections 8/9 are text-only) or a flow that doesn't exist in the replica at all. Master marks
// every flow with a `SECTION n` comment, plus the un-numbered STARTUP block and the hsm-file-service
// block; HsmDemo.jsx's FLOWS has one `title: 'N. ...'` entry per flow.
function checkFlowCount(svgPath, jsxPath) {
  const svg = fs.readFileSync(svgPath, 'utf8');
  const jsx = fs.readFileSync(jsxPath, 'utf8');
  const sections = (svg.match(/<!--\s*SECTION\s+\d+/g) || []).length;
  const startup = /STARTUP\s+\(at service init/.test(svg) ? 1 : 0;
  const fileSvc = /<!--[^>]*hsm-file-service:\s*GET/.test(svg) ? 1 : 0;
  const masterFlows = sections + startup + fileSvc;
  const jsxFlows = (jsx.match(/^\s{4}title: '\d+\.\s/gm) || []).length;
  const ok = masterFlows === jsxFlows;
  console.log(`[FLOW COUNT] master ${masterFlows} (${sections} SECTION + startup ${startup} + file-service ${fileSvc}) vs jsx FLOWS ${jsxFlows} -> ${ok ? 'ok' : 'MISMATCH -- a whole flow is missing or extra'}`);
  return ok ? 0 : 1;
}

// HsmDemo.jsx: every message is written on one line, e.g.
//   { from: 'x', to: 'y', label: 'POST /encrypt { plaintext, ... }', stepNum: 3 },
// A brace-matching object-literal regex breaks here because labels legitimately
// contain their own literal `{`/`}` (request/response shape text) -- so this
// scans line-by-line instead, which sidesteps the nesting problem entirely.
function extractJsxSteps(jsxPath) {
  const src = fs.readFileSync(jsxPath, 'utf8');
  const steps = new Map();
  const lineRe = /label:\s*'((?:[^'\\]|\\.)*)'.*?stepNum:\s*'?([0-9A-Za-z]+)'?/;
  for (const line of src.split('\n')) {
    const m = line.match(lineRe);
    if (!m) continue;
    const label = normalize(m[1].replace(/\\'/g, "'"));
    const stepNum = m[2];
    if (!steps.has(stepNum)) steps.set(stepNum, []);
    steps.get(stepNum).push(label);
  }
  return steps;
}

function labelsRoughlyMatch(masterLabel, jsxLabel) {
  if (masterLabel === jsxLabel) return true;
  // HsmDemo.jsx's own style deliberately expands on master's terser step text
  // (extra clauses, spelled-out consequences) rather than transcribing it
  // verbatim -- so this checks containment (most of master's significant
  // words show up somewhere in jsx's longer label), not symmetric overlap.
  // A generic paraphrase (e.g. jsx saying "slot" where master names a
  // literal "beta") can still legitimately fall short of this and needs a
  // human glance -- this script flags, it doesn't auto-decide.
  const words = (s) => new Set(s.toLowerCase().split(/[^a-z0-9_]+/).filter((w) => w.length > 2));
  const wm = words(masterLabel);
  const wj = words(jsxLabel);
  if (wm.size === 0) return true;
  const found = [...wm].filter((w) => wj.has(w)).length;
  return found / wm.size > 0.55;
}

function main() {
  const [svgPath, jsxPath] = process.argv.slice(2);
  if (!svgPath || !jsxPath) {
    console.error('Usage: node sync-hsm-sequence.js <sequence-diagram.svg> <HsmDemo.jsx>');
    process.exit(1);
  }

  const masterSteps = extractMasterSteps(svgPath);
  const jsxSteps = extractJsxSteps(jsxPath);

  const allStepNums = new Set([...masterSteps.keys(), ...jsxSteps.keys()]);
  const sorted = [...allStepNums].sort((a, b) => {
    const na = parseFloat(a.replace(/[^0-9.]/g, '')) || 0;
    const nb = parseFloat(b.replace(/[^0-9.]/g, '')) || 0;
    return na - nb;
  });

  let issues = checkFlowCount(svgPath, jsxPath);
  for (const stepNum of sorted) {
    const mLabels = masterSteps.get(stepNum) || [];
    const jLabels = jsxSteps.get(stepNum) || [];

    if (mLabels.length && !jLabels.length) {
      issues++;
      console.log(`[MISSING IN JSX] step ${stepNum}:`);
      mLabels.forEach((l) => console.log(`    master: ${l}`));
      continue;
    }
    if (!mLabels.length && jLabels.length) {
      issues++;
      console.log(`[NOT IN MASTER] step ${stepNum} (renumbered? removed upstream?):`);
      jLabels.forEach((l) => console.log(`    jsx: ${l}`));
      continue;
    }

    // Both have this step -- check each master label has a roughly-matching jsx label
    for (const mLabel of mLabels) {
      const hasMatch = jLabels.some((jLabel) => labelsRoughlyMatch(mLabel, jLabel));
      if (!hasMatch) {
        issues++;
        console.log(`[TEXT DRIFT] step ${stepNum}:`);
        console.log(`    master: ${mLabel}`);
        jLabels.forEach((jLabel) => console.log(`    jsx:    ${jLabel}`));
      }
    }
  }

  console.log(`\n${issues === 0 ? 'No drift detected.' : `${issues} potential drift item(s) above.`}`);
  console.log(`(master steps: ${masterSteps.size}, jsx steps: ${jsxSteps.size})`);
}

main();
