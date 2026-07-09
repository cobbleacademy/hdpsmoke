'use strict';

// Pattern Templates — runtime-editable Mermaid diagram library.
//
// Same tier-2 "runtime-editable storage" shape as Governance Lifecycle and
// Ranger Library: a manifest.json (list metadata — id, name, description,
// updatedAt) plus one file per template holding the actual content. Splitting
// the manifest from the content keeps the list-panel fetch (GET
// /pattern-templates) light even as the library grows, since it never has to
// read every template's full Mermaid text just to render the left-hand list.
//
// No encryption here (unlike Payload Library/OPA/Ranger) — Mermaid diagram
// text is not a secret, same reasoning as Governance Lifecycle's config.json.

const fs   = require('fs');
const path = require('path');
const crypto = require('crypto');

function storagePath() {
  return (
    process.env.PATTERN_TEMPLATES_STORAGE_PATH ||
    path.join(__dirname, '../../data/pattern-templates')
  );
}

function manifestPath() {
  return path.join(storagePath(), 'manifest.json');
}

function templateFilePath(id) {
  return path.join(storagePath(), `${id}.mmd`);
}

function ensureDir() {
  fs.mkdirSync(storagePath(), { recursive: true });
}

// Seed content served until the first template is saved — mirrors
// Governance Lifecycle's DEFAULT_CONFIG fallback convention.
const SEED_TEMPLATES = [
  {
    id: 'seed-flowchart',
    name: 'Simple Flowchart',
    description: 'A basic decision flowchart — start, decide, branch, end.',
    type: 'mermaid',
    mermaidText:
      'flowchart TD\n' +
      '  A[Start] --> B{Decision?}\n' +
      '  B -- Yes --> C[Do the thing]\n' +
      '  B -- No --> D[Skip it]\n' +
      '  C --> E[End]\n' +
      '  D --> E[End]\n',
  },
  {
    id: 'seed-sequence',
    name: 'Sequence Diagram',
    description: 'A request/response exchange between two participants.',
    type: 'mermaid',
    mermaidText:
      'sequenceDiagram\n' +
      '  participant User\n' +
      '  participant API\n' +
      '  User->>API: POST /submit\n' +
      '  API-->>User: 200 OK\n',
  },
  {
    id: 'seed-class',
    name: 'Class Diagram',
    description: 'A minimal class relationship example.',
    type: 'mermaid',
    mermaidText:
      'classDiagram\n' +
      '  class Animal {\n' +
      '    +String name\n' +
      '    +makeSound()\n' +
      '  }\n' +
      '  class Dog\n' +
      '  Animal <|-- Dog\n',
  },
  {
    id: 'seed-er',
    name: 'Entity Relationship Diagram',
    description: 'A basic two-table relationship.',
    type: 'mermaid',
    mermaidText:
      'erDiagram\n' +
      '  USER ||--o{ ORDER : places\n' +
      '  USER {\n' +
      '    string id\n' +
      '    string email\n' +
      '  }\n' +
      '  ORDER {\n' +
      '    string id\n' +
      '    string status\n' +
      '  }\n',
  },
  {
    // Demonstrates the 'svg' content type — hand-authored markup rendered
    // directly (no Mermaid layout engine involved). See ADR-0018 addendum on
    // why Mermaid's auto-layout can't match a hand-placed diagram's polish.
    id: 'seed-svg-example',
    name: 'Hand-Drawn SVG Example',
    description: 'Raw SVG markup rendered directly — for diagrams that need hand-placed polish Mermaid can\'t match.',
    type: 'svg',
    mermaidText:
      '<svg viewBox="0 0 420 160" xmlns="http://www.w3.org/2000/svg">\n' +
      '  <defs>\n' +
      '    <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">\n' +
      '      <path d="M0,0 L6,3 L0,6 Z" fill="#94a3b8" />\n' +
      '    </marker>\n' +
      '  </defs>\n' +
      '  <rect x="10" y="10" width="110" height="60" rx="8" fill="#22263a" stroke="#3b82f6" stroke-width="1.5" />\n' +
      '  <text x="65" y="45" text-anchor="middle" fill="#3b82f6" font-family="monospace" font-size="12">Client</text>\n' +
      '  <line x1="120" y1="40" x2="160" y2="40" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arrow)" />\n' +
      '  <rect x="160" y="10" width="110" height="60" rx="8" fill="#22263a" stroke="#f59e0b" stroke-width="1.5" />\n' +
      '  <text x="215" y="45" text-anchor="middle" fill="#f59e0b" font-family="monospace" font-size="12">Gateway</text>\n' +
      '  <line x1="270" y1="40" x2="310" y2="40" stroke="#94a3b8" stroke-width="1.5" marker-end="url(#arrow)" />\n' +
      '  <rect x="310" y="10" width="100" height="60" rx="8" fill="#1e1b4b" stroke="#a78bfa" stroke-width="2" />\n' +
      '  <text x="360" y="45" text-anchor="middle" fill="#a78bfa" font-family="monospace" font-size="12">Service</text>\n' +
      '</svg>\n',
  },
];

const VALID_TYPES = ['mermaid', 'svg'];

function toManifestEntry(t) {
  return {
    id: t.id, name: t.name, description: t.description,
    type: t.type || 'mermaid', updatedAt: t.updatedAt || null,
  };
}

function generateId(name) {
  const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
  const suffix = crypto.randomBytes(3).toString('hex');
  return `${slug || 'template'}-${suffix}`;
}

/** Returns the list for the left/right panel — metadata only, no Mermaid text. */
function listTemplates() {
  const file = manifestPath();
  if (!fs.existsSync(file)) {
    return SEED_TEMPLATES.map(toManifestEntry);
  }
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (err) {
    console.error('[patternTemplatesConfigService] Failed to parse manifest.json, serving seed list:', err.message);
    return SEED_TEMPLATES.map(toManifestEntry);
  }
}

/** Returns { id, name, description, mermaidText, updatedAt } for one template. */
function getTemplate(id) {
  const manifest = listTemplates();
  const entry = manifest.find((t) => t.id === id);
  if (!entry) {
    const seed = SEED_TEMPLATES.find((t) => t.id === id);
    return seed || null;
  }
  const filePath = templateFilePath(id);
  const mermaidText = fs.existsSync(filePath)
    ? fs.readFileSync(filePath, 'utf8')
    : (SEED_TEMPLATES.find((t) => t.id === id)?.mermaidText || '');
  return { ...entry, type: entry.type || 'mermaid', mermaidText };
}

function writeManifest(manifest) {
  ensureDir();
  fs.writeFileSync(manifestPath(), JSON.stringify(manifest, null, 2), 'utf8');
}

function validateInput({ name, mermaidText, type }) {
  const errors = [];
  if (!name || typeof name !== 'string' || !name.trim()) errors.push('"name" is required');
  if (!mermaidText || typeof mermaidText !== 'string' || !mermaidText.trim()) {
    errors.push('"mermaidText" is required');
  }
  if (mermaidText && mermaidText.length > 50_000) {
    errors.push('"mermaidText" exceeds the 50,000 character limit');
  }
  if (type !== undefined && !VALID_TYPES.includes(type)) {
    errors.push(`"type" must be one of: ${VALID_TYPES.join(', ')}`);
  }
  // 'svg' templates skip Mermaid parsing entirely (there's no Mermaid parser
  // on the backend to validate against either way) — just a basic sanity
  // check that the content is actually SVG markup, not e.g. leftover Mermaid text.
  if (type === 'svg' && mermaidText && !mermaidText.trim().startsWith('<svg')) {
    errors.push('"mermaidText" must start with <svg for type "svg"');
  }
  return errors;
}

/** Creates a new template. Returns the created manifest entry (with id). */
function createTemplate({ name, description, mermaidText, type }) {
  const errors = validateInput({ name, mermaidText, type });
  if (errors.length) {
    const err = new Error('Template failed validation');
    err.validationErrors = errors;
    throw err;
  }

  const manifest = listTemplates();
  if (manifest.some((t) => t.name === name)) {
    const err = new Error('Template failed validation');
    err.validationErrors = [`A template named "${name}" already exists — names must be unique`];
    throw err;
  }

  ensureDir();
  const id = generateId(name);
  const updatedAt = new Date().toISOString();
  fs.writeFileSync(templateFilePath(id), mermaidText, 'utf8');
  const entry = { id, name, description: description || '', type: type || 'mermaid', updatedAt };
  manifest.push(entry);
  writeManifest(manifest);
  return entry;
}

/** Edits an existing template's name/description/mermaidText/type. */
function updateTemplate(id, { name, description, mermaidText, type }) {
  const errors = validateInput({ name, mermaidText, type });
  if (errors.length) {
    const err = new Error('Template failed validation');
    err.validationErrors = errors;
    throw err;
  }

  const manifest = listTemplates();
  const idx = manifest.findIndex((t) => t.id === id);
  if (idx === -1) {
    const err = new Error(`Template "${id}" not found`);
    err.code = 'NOT_FOUND';
    throw err;
  }
  if (manifest.some((t) => t.name === name && t.id !== id)) {
    const err = new Error('Template failed validation');
    err.validationErrors = [`A template named "${name}" already exists — names must be unique`];
    throw err;
  }

  ensureDir();
  const updatedAt = new Date().toISOString();
  fs.writeFileSync(templateFilePath(id), mermaidText, 'utf8');
  manifest[idx] = { id, name, description: description || '', type: type || manifest[idx].type || 'mermaid', updatedAt };
  writeManifest(manifest);
  return manifest[idx];
}

function deleteTemplate(id) {
  const manifest = listTemplates();
  const idx = manifest.findIndex((t) => t.id === id);
  if (idx === -1) {
    const err = new Error(`Template "${id}" not found`);
    err.code = 'NOT_FOUND';
    throw err;
  }
  manifest.splice(idx, 1);
  writeManifest(manifest);
  const filePath = templateFilePath(id);
  if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
}

module.exports = { listTemplates, getTemplate, createTemplate, updateTemplate, deleteTemplate };
