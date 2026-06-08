'use strict';

const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');

// ── Key — reuses the same PAYLOAD_ENCRYPTION_KEY as payloadService ────────────

function getEncryptionKey() {
  const raw = process.env.PAYLOAD_ENCRYPTION_KEY || '';
  if (!raw) return null;
  if (/^[0-9a-f]{64}$/i.test(raw)) return Buffer.from(raw, 'hex');
  return crypto.scryptSync(raw, 'pattern-payload-v1', 32);
}

function storagePath() {
  return (
    process.env.OPA_POLICY_STORAGE_PATH ||
    path.join(__dirname, '../../data/opa-policies')
  );
}

// Safe file name: lowercase, non-alphanumeric → underscore
function toFileKey(name) {
  return name.toLowerCase().replace(/[^a-z0-9]/g, '_');
}

// ── AES-256-GCM helpers (same envelope format as payloadService) ──────────────

function encrypt(text, key) {
  const iv     = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ct     = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
  return {
    v:          1,
    iv:         iv.toString('hex'),
    tag:        cipher.getAuthTag().toString('hex'),
    ciphertext: ct.toString('hex'),
  };
}

function decrypt(envelope, key) {
  if (envelope.v !== 1) throw new Error(`Unknown OPA policy envelope version: ${envelope.v}`);
  const decipher = crypto.createDecipheriv(
    'aes-256-gcm',
    key,
    Buffer.from(envelope.iv, 'hex')
  );
  decipher.setAuthTag(Buffer.from(envelope.tag, 'hex'));
  return Buffer.concat([
    decipher.update(Buffer.from(envelope.ciphertext, 'hex')),
    decipher.final(),
  ]).toString('utf8');
}

// ── Validation ────────────────────────────────────────────────────────────────

/**
 * Validates that a string is non-empty Rego with a package declaration
 * and at least one recognisable OPA rule head.
 * Throws a descriptive error (safe to surface to the client) on failure.
 */
function validateRego(rego) {
  if (typeof rego !== 'string' || !rego.trim()) {
    throw new Error('Rego content must be a non-empty string');
  }
  if (!rego.includes('package ')) {
    throw new Error('Rego must contain a package declaration (e.g. "package databricks.abac")');
  }
  // At least one rule head: allow, row_visible, column_masked, deny
  if (!/\b(allow|row_visible|column_masked|deny)\s*[\[{=]/.test(rego)) {
    throw new Error(
      'Rego must contain at least one allow, row_visible, column_masked, or deny rule'
    );
  }
}

// ── Read ──────────────────────────────────────────────────────────────────────

/**
 * Read and (if encrypted) decrypt the stored Rego for a policy name.
 *
 * Resolution order (first found wins):
 *  1. {storagePath}/{fileKey}.enc   — AES-256-GCM encrypted
 *  2. {storagePath}/{fileKey}.rego  — plain text (no encryption key set)
 *
 * @param {string} name  Policy name (e.g. "customers-abac")
 * @returns {{ rego: string, encrypted: boolean } | null}
 */
function readPolicy(name) {
  const fileKey = toFileKey(name);
  const key     = getEncryptionKey();
  const dir     = storagePath();

  const encFile = path.join(dir, `${fileKey}.enc`);
  if (fs.existsSync(encFile)) {
    if (!key) {
      throw new Error(
        `Encrypted OPA policy found for "${name}" but PAYLOAD_ENCRYPTION_KEY is not set. ` +
        'Set the env var to decrypt, or delete the .enc file to start fresh.'
      );
    }
    const envelope = JSON.parse(fs.readFileSync(encFile, 'utf8'));
    return { rego: decrypt(envelope, key), encrypted: true };
  }

  const plainFile = path.join(dir, `${fileKey}.rego`);
  if (fs.existsSync(plainFile)) {
    return { rego: fs.readFileSync(plainFile, 'utf8'), encrypted: false };
  }

  return null;
}

// ── Write ─────────────────────────────────────────────────────────────────────

/**
 * Validate, then encrypt and persist Rego content for a policy name.
 * Writes {fileKey}.enc when a key is configured; {fileKey}.rego otherwise.
 *
 * @param {string} name  Policy name
 * @param {string} rego  Rego content to store
 */
function writePolicy(name, rego) {
  validateRego(rego);

  const fileKey = toFileKey(name);
  const dir     = storagePath();
  fs.mkdirSync(dir, { recursive: true });

  const key = getEncryptionKey();
  if (key) {
    const envelope = encrypt(rego, key);
    fs.writeFileSync(
      path.join(dir, `${fileKey}.enc`),
      JSON.stringify(envelope, null, 2),
      'utf8'
    );
  } else {
    console.warn(
      `[opaPolicyPersistService] PAYLOAD_ENCRYPTION_KEY not set — ` +
      `storing OPA policy "${name}" as plain text. Set PAYLOAD_ENCRYPTION_KEY for encrypted storage.`
    );
    fs.writeFileSync(path.join(dir, `${fileKey}.rego`), rego, 'utf8');
  }
}

// ── List ──────────────────────────────────────────────────────────────────────

/**
 * List saved policy names (file stems, without extension).
 * @returns {string[]}
 */
function listPolicies() {
  const dir = storagePath();
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir)
    .filter(f => f.endsWith('.enc') || f.endsWith('.rego'))
    .map(f => f.replace(/\.(enc|rego)$/, ''))
    .sort();
}

// ── Env + policyKey-aware read/write (for tree-based library) ────────────────

/**
 * Read the Rego for a specific env + policyKey.
 * Storage: {baseDir}/{envId}/{safeKey}.enc  or  .rego
 *
 * @param {string} envId
 * @param {string} policyKey  e.g. "demos__customers___region_row_filter"
 */
function readPolicyByKey(envId, policyKey) {
  const dir  = path.join(storagePath(), envId.toLowerCase());
  const safe = policyKey.toLowerCase().replace(/[^a-z0-9_]/g, '_');
  const key  = getEncryptionKey();

  const encFile   = path.join(dir, `${safe}.enc`);
  const plainFile = path.join(dir, `${safe}.rego`);

  if (fs.existsSync(encFile)) {
    if (!key) throw new Error(`Encrypted policy found for "${policyKey}" but PAYLOAD_ENCRYPTION_KEY is not set.`);
    const envelope = JSON.parse(fs.readFileSync(encFile, 'utf8'));
    return { rego: decrypt(envelope, key), encrypted: true };
  }

  if (fs.existsSync(plainFile)) {
    return { rego: fs.readFileSync(plainFile, 'utf8'), encrypted: false };
  }

  return null;
}

/**
 * Validate, then encrypt and persist Rego for a specific env + policyKey.
 *
 * @param {string} envId
 * @param {string} policyKey
 * @param {string} rego
 */
function writePolicyByKey(envId, policyKey, rego) {
  validateRego(rego);

  const dir  = path.join(storagePath(), envId.toLowerCase());
  fs.mkdirSync(dir, { recursive: true });
  const safe = policyKey.toLowerCase().replace(/[^a-z0-9_]/g, '_');
  const key  = getEncryptionKey();

  if (key) {
    const envelope = encrypt(rego, key);
    fs.writeFileSync(path.join(dir, `${safe}.enc`), JSON.stringify(envelope, null, 2), 'utf8');
  } else {
    console.warn(
      `[opaPolicyPersistService] PAYLOAD_ENCRYPTION_KEY not set — ` +
      `storing "${policyKey}" (env: ${envId}) as plain text.`
    );
    fs.writeFileSync(path.join(dir, `${safe}.rego`), rego, 'utf8');
  }
}

module.exports = {
  readPolicy, writePolicy, listPolicies, validateRego, getEncryptionKey,
  readPolicyByKey, writePolicyByKey,
};
