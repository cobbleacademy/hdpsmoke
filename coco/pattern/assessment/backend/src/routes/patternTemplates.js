'use strict';

// Pattern Templates routes — runtime-editable Mermaid diagram library, full
// CRUD. See docs/adr/0018-pattern-templates-and-nav-flags.md.

const express = require('express');
const crypto  = require('crypto');
const {
  listTemplates,
  getTemplate,
  createTemplate,
  updateTemplate,
  deleteTemplate,
} = require('../services/patternTemplatesConfigService');

const router = express.Router();

/**
 * Middleware: verify the Bearer token when PAYLOAD_WRITE_AUTH_ENABLED=true.
 * Same shared gate as Payload Library / OPA / Ranger / Governance Lifecycle —
 * one secret reused everywhere a content editor needs write protection.
 */
function checkWriteAuth(req, res, next) {
  if (process.env.PAYLOAD_WRITE_AUTH_ENABLED !== 'true') return next();
  const adminToken = process.env.PAYLOAD_ADMIN_TOKEN || '';
  if (!adminToken) {
    console.warn(
      '[patternTemplates] PAYLOAD_WRITE_AUTH_ENABLED=true but PAYLOAD_ADMIN_TOKEN is not set — ' +
      'write auth is effectively disabled'
    );
    return next();
  }

  const authHeader = req.headers['authorization'] || '';
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';

  let valid = false;
  try {
    valid =
      token.length === adminToken.length &&
      crypto.timingSafeEqual(Buffer.from(token), Buffer.from(adminToken));
  } catch {
    valid = false;
  }

  if (!valid) {
    return res.status(401).json({
      error: 'Unauthorized: valid Bearer token required to write Pattern Templates content',
    });
  }
  next();
}

function writeAuthRequired() {
  return process.env.PAYLOAD_WRITE_AUTH_ENABLED === 'true' && Boolean(process.env.PAYLOAD_ADMIN_TOKEN);
}

// ── GET /pattern-templates ─────────────────────────────────────────────────────
// List metadata only (id, name, description, updatedAt) — the left-panel
// list. Full Mermaid text is fetched per-template via GET /pattern-templates/:id.
router.get('/pattern-templates', (req, res) => {
  try {
    res.json({ templates: listTemplates(), writeAuthRequired: writeAuthRequired() });
  } catch (err) {
    console.error('[patternTemplates] GET list failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /pattern-templates/:id ─────────────────────────────────────────────────
router.get('/pattern-templates/:id', (req, res) => {
  try {
    const template = getTemplate(req.params.id);
    if (!template) {
      return res.status(404).json({ error: `Template "${req.params.id}" not found` });
    }
    res.json(template);
  } catch (err) {
    console.error('[patternTemplates] GET one failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /pattern-templates ────────────────────────────────────────────────────
// Body: { name, description, mermaidText, type } — type: 'mermaid' | 'svg' (default 'mermaid')
router.post('/pattern-templates', checkWriteAuth, (req, res) => {
  const { name, description, mermaidText, type } = req.body || {};
  try {
    const created = createTemplate({ name, description, mermaidText, type });
    res.status(201).json(created);
  } catch (err) {
    if (err.validationErrors) {
      return res.status(400).json({ error: err.message, validationErrors: err.validationErrors });
    }
    console.error('[patternTemplates] POST failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── PUT /pattern-templates/:id ──────────────────────────────────────────────────
// Body: { name, description, mermaidText, type }
router.put('/pattern-templates/:id', checkWriteAuth, (req, res) => {
  const { name, description, mermaidText, type } = req.body || {};
  try {
    const updated = updateTemplate(req.params.id, { name, description, mermaidText, type });
    res.json(updated);
  } catch (err) {
    if (err.validationErrors) {
      return res.status(400).json({ error: err.message, validationErrors: err.validationErrors });
    }
    if (err.code === 'NOT_FOUND') {
      return res.status(404).json({ error: err.message });
    }
    console.error('[patternTemplates] PUT failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── DELETE /pattern-templates/:id ───────────────────────────────────────────────
router.delete('/pattern-templates/:id', checkWriteAuth, (req, res) => {
  try {
    deleteTemplate(req.params.id);
    res.json({ ok: true });
  } catch (err) {
    if (err.code === 'NOT_FOUND') {
      return res.status(404).json({ error: err.message });
    }
    console.error('[patternTemplates] DELETE failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
