'use strict';

const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');
const jsYaml = require('js-yaml');

// ── Key derivation ────────────────────────────────────────────────────────────

/**
 * Derive a 32-byte AES-256 key from PAYLOAD_ENCRYPTION_KEY.
 *
 * Accepts two formats:
 *   - 64 lowercase hex chars  → used directly as the key bytes
 *   - any other string        → scrypt-derived (supports human-readable passphrases)
 *
 * Returns null when PAYLOAD_ENCRYPTION_KEY is unset — callers fall back to
 * plain-text mode and log a warning.
 *
 * @returns {Buffer|null}
 */
function getEncryptionKey() {
  const raw = process.env.PAYLOAD_ENCRYPTION_KEY || '';
  if (!raw) return null;
  if (/^[0-9a-f]{64}$/i.test(raw)) return Buffer.from(raw, 'hex');
  // Passphrase → scrypt key derivation (salt is fixed and domain-separated)
  return crypto.scryptSync(raw, 'pattern-payload-v1', 32);
}

/**
 * Return the directory where encrypted (or plain) payload files are stored.
 * Defaults to ./data/payloads relative to the backend root in development,
 * or /app/data/payloads in a container when PAYLOAD_STORAGE_PATH is set.
 */
function storagePath() {
  return (
    process.env.PAYLOAD_STORAGE_PATH ||
    path.join(__dirname, '../../data/payloads')
  );
}

/**
 * Paths for the plain-text fallback chain (steps 2 & 3 of readPayload).
 *
 * Local dev  — resolved relative to __dirname so no env vars are needed.
 * Docker     — PAYLOAD_FALLBACK_DIR / PAYLOAD_LEGACY_YAML env vars point
 *              to bind-mounted copies of the frontend public/ files.
 * K8s / Helm — set the same two env vars to a ConfigMap-mounted path.
 */
const FRONTEND_PAYLOADS = (
  process.env.PAYLOAD_FALLBACK_DIR ||
  path.join(__dirname, '..', '..', '..', 'frontend', 'public', 'payloads')
);
const LEGACY_YAML = (
  process.env.PAYLOAD_LEGACY_YAML ||
  path.join(__dirname, '..', '..', '..', 'frontend', 'public', 'payloads.yaml')
);

// ── AES-256-GCM encryption helpers ───────────────────────────────────────────

/**
 * Encrypt a UTF-8 string with AES-256-GCM.
 * A fresh random 12-byte IV is generated for every call.
 *
 * @param {string} text   Plain text to encrypt (YAML content)
 * @param {Buffer} key    32-byte key from getEncryptionKey()
 * @returns {{ v: number, iv: string, tag: string, ciphertext: string }}
 *   All binary values are hex-encoded strings.
 *   v=1 identifies the envelope format version for future key-rotation support.
 */
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

/**
 * Decrypt an envelope produced by encrypt().
 * Throws if the auth tag does not match (tampered or wrong key).
 *
 * @param {{ v, iv, tag, ciphertext }} envelope
 * @param {Buffer} key  32-byte key
 * @returns {string}    Plain text (YAML content)
 */
function decrypt(envelope, key) {
  if (envelope.v !== 1) {
    throw new Error(`Unknown payload envelope version: ${envelope.v}`);
  }
  const iv       = Buffer.from(envelope.iv, 'hex');
  const tag      = Buffer.from(envelope.tag, 'hex');
  const ct       = Buffer.from(envelope.ciphertext, 'hex');
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(ct), decipher.final()]).toString('utf8');
}

// ── Read ──────────────────────────────────────────────────────────────────────

/**
 * Read and decrypt the payload YAML for a given environment.
 *
 * Resolution order (first found wins):
 *   1. {PAYLOAD_STORAGE_PATH}/{envId}.enc   — encrypted, preferred
 *   2. frontend/public/payloads/{envId}.yaml — plain-text migration fallback
 *   3. frontend/public/payloads.yaml          — legacy flat-file fallback
 *
 * @param {string} envId  Raw environment ID (e.g. "ADM-DEV", "PROD")
 * @returns {{ yaml: string, payloads: Array, encrypted: boolean } | null}
 * @throws Error if an encrypted file is found but no key is configured
 */
function readPayload(envId) {
  const key    = getEncryptionKey();
  const encDir = storagePath();
  const encFile = path.join(encDir, `${envId}.enc`);

  // ── Step 1: encrypted file ───────────────────────────────────────────────
  if (fs.existsSync(encFile)) {
    if (!key) {
      throw new Error(
        `Encrypted payload found for "${envId}" but PAYLOAD_ENCRYPTION_KEY is not set. ` +
        'Set the env var to decrypt, or delete the .enc file to use the plain-text fallback.'
      );
    }
    const envelope  = JSON.parse(fs.readFileSync(encFile, 'utf8'));
    const yaml      = decrypt(envelope, key);
    const payloads  = parsePayloads(yaml, envId);
    return { yaml, payloads, encrypted: true };
  }

  // ── Step 1b: plain-text saved file in storage dir ────────────────────────
  // writePayload() writes here when PAYLOAD_ENCRYPTION_KEY is not set.
  // Must be checked BEFORE the read-only fallback dirs so that edits saved
  // via the UI take precedence over the original static files.
  const savedPlainFile = path.join(encDir, `${envId}.yaml`);
  if (fs.existsSync(savedPlainFile)) {
    const yaml     = fs.readFileSync(savedPlainFile, 'utf8');
    const payloads = parsePayloads(yaml, envId);
    return { yaml, payloads, encrypted: false };
  }

  // ── Step 2: plain per-env YAML (migration / dev fallback, read-only) ────
  const plainFile = path.join(FRONTEND_PAYLOADS, `${envId}.yaml`);
  if (fs.existsSync(plainFile)) {
    const yaml     = fs.readFileSync(plainFile, 'utf8');
    const payloads = parsePayloads(yaml, envId);
    return { yaml, payloads, encrypted: false };
  }

  // ── Step 3: legacy flat payloads.yaml ───────────────────────────────────
  if (fs.existsSync(LEGACY_YAML)) {
    const yaml     = fs.readFileSync(LEGACY_YAML, 'utf8');
    const payloads = parsePayloads(yaml, envId);
    return { yaml, payloads, encrypted: false };
  }

  return null;
}

// ── Write ─────────────────────────────────────────────────────────────────────

/**
 * Validate YAML structure, then encrypt and persist payload content.
 *
 * When PAYLOAD_ENCRYPTION_KEY is set the file is written as {envId}.enc.
 * When the key is absent the content is stored as plain {envId}.yaml with
 * a startup-level warning (acceptable in local dev, not for production).
 *
 * @param {string} envId  Raw environment ID
 * @param {string} yaml   YAML content to store
 * @throws Error on invalid YAML or write failure
 */
function writePayload(envId, yaml) {
  // Validate YAML structure before touching disk
  validateYaml(yaml);

  const dir = storagePath();
  fs.mkdirSync(dir, { recursive: true });

  const key = getEncryptionKey();
  if (key) {
    const envelope = encrypt(yaml, key);
    fs.writeFileSync(
      path.join(dir, `${envId}.enc`),
      JSON.stringify(envelope, null, 2),
      'utf8'
    );
  } else {
    console.warn(
      `[payloadService] PAYLOAD_ENCRYPTION_KEY not set — ` +
      `storing payload for "${envId}" as plain text. ` +
      'Set PAYLOAD_ENCRYPTION_KEY for encrypted storage.'
    );
    fs.writeFileSync(path.join(dir, `${envId}.yaml`), yaml, 'utf8');
  }
}

// ── YAML helpers ──────────────────────────────────────────────────────────────

/**
 * Parse a YAML string and return the payloads array.
 * Throws a descriptive error if parsing fails or the structure is invalid.
 */
function parsePayloads(yaml, envId) {
  let parsed;
  try {
    parsed = jsYaml.load(yaml);
  } catch (e) {
    throw new Error(`YAML parse error in "${envId}" payload file: ${e.message}`);
  }
  if (!parsed?.payloads || !Array.isArray(parsed.payloads)) {
    throw new Error(`Payload file for "${envId}" must contain a top-level "payloads:" array`);
  }
  return parsed.payloads;
}

/**
 * Validate that a YAML string is well-formed and has the required structure.
 * Throws a descriptive error (safe to surface to the client) on failure.
 */
function validateYaml(yaml) {
  let parsed;
  try {
    parsed = jsYaml.load(yaml);
  } catch (e) {
    throw new Error(`Invalid YAML: ${e.message}`);
  }
  if (!parsed?.payloads || !Array.isArray(parsed.payloads)) {
    throw new Error('YAML must contain a top-level "payloads:" array');
  }
  if (parsed.payloads.length === 0) {
    throw new Error('"payloads:" array must not be empty');
  }
  for (const [i, entry] of parsed.payloads.entries()) {
    if (typeof entry.name !== 'string' || !entry.name.trim()) {
      throw new Error(`payloads[${i}] is missing a required "name" field`);
    }
    if (typeof entry.payload !== 'string' || !entry.payload.trim()) {
      throw new Error(`payloads[${i}] ("${entry.name}") is missing a required "payload" field`);
    }
  }
}

module.exports = { readPayload, writePayload, encrypt, decrypt, getEncryptionKey, validateYaml };
