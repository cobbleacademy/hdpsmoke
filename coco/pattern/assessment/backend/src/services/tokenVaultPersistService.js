'use strict';

const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');

// ── Key — reuses the same PAYLOAD_ENCRYPTION_KEY as payloadService / rangerPersistService ──

function getEncryptionKey() {
  const raw = process.env.PAYLOAD_ENCRYPTION_KEY || '';
  if (!raw) return null;
  if (/^[0-9a-f]{64}$/i.test(raw)) return Buffer.from(raw, 'hex');
  return crypto.scryptSync(raw, 'pattern-payload-v1', 32);
}

function storagePath() {
  return (
    process.env.TOKEN_VAULT_STORAGE_PATH ||
    path.join(__dirname, '../../data/token-vault')
  );
}

function envDir(envId) {
  const dir = path.join(storagePath(), envId.toLowerCase());
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function credentialsPath(envId, ext) {
  return path.join(envDir(envId), `credentials.${ext}`);
}

// ── AES-256-GCM helpers (same envelope format as payloadService/rangerPersistService) ──

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
  if (envelope.v !== 1) throw new Error(`Unknown Token Vault envelope version: ${envelope.v}`);
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

// ── Credential-set CRUD ─────────────────────────────────────────────────────────
// One file per environment holding the full flat list — same "whole-file"
// granularity as Payload Library, simpler than Ranger's per-item files since
// there's no per-item generation step to keep separate.

/**
 * Read the full credential-set list for an environment. Returns [] if none
 * stored yet. Throws if an encrypted file exists but no key is configured.
 */
function readCredentials(envId) {
  const dir = envDir(envId);
  const encFile   = credentialsPath(envId, 'enc');
  const plainFile = credentialsPath(envId, 'json');
  const key = getEncryptionKey();

  if (fs.existsSync(encFile)) {
    if (!key) {
      throw new Error(
        `Encrypted Token Vault credentials found for env "${envId}" but PAYLOAD_ENCRYPTION_KEY is not set.`
      );
    }
    const envelope = JSON.parse(fs.readFileSync(encFile, 'utf8'));
    return JSON.parse(decrypt(envelope, key));
  }

  if (fs.existsSync(plainFile)) {
    return JSON.parse(fs.readFileSync(plainFile, 'utf8'));
  }

  void dir; // envDir() already created it as a side effect
  return [];
}

/**
 * Encrypt and persist the full credential-set list for an environment.
 * Writes .enc when a key is configured, .json (with a console.warn) otherwise.
 */
function writeCredentials(envId, credentials) {
  if (!Array.isArray(credentials)) {
    throw new Error('Token Vault credentials must be an array');
  }

  const key  = getEncryptionKey();
  const text = JSON.stringify(credentials, null, 2);

  if (key) {
    const envelope = encrypt(text, key);
    fs.writeFileSync(credentialsPath(envId, 'enc'), JSON.stringify(envelope, null, 2), 'utf8');
    // Remove a stale plaintext file from before a key was configured.
    const plainFile = credentialsPath(envId, 'json');
    if (fs.existsSync(plainFile)) fs.unlinkSync(plainFile);
  } else {
    console.warn(
      `[tokenVaultPersistService] PAYLOAD_ENCRYPTION_KEY not set — ` +
      `storing Token Vault credentials for env "${envId}" as plain text.`
    );
    fs.writeFileSync(credentialsPath(envId, 'json'), text, 'utf8');
  }
}

/** Redacts clientSecret for any response that leaves the server. */
function redact(credential) {
  const { clientSecret, ...rest } = credential;
  return { ...rest, clientSecretSet: Boolean(clientSecret) };
}

/**
 * Look up one credential set by id within an env, including its real
 * clientSecret — server-side use only (token generation), never returned
 * to the browser as-is.
 */
function findCredential(envId, credentialId) {
  return readCredentials(envId).find((c) => c.id === credentialId) || null;
}

// ── Environments ──────────────────────────────────────────────────────────────

/**
 * Parse TOKEN_VAULT_ENVS into an array of { id, label } objects.
 * Falls back to [{ id: 'DEFAULT', label: 'Default' }] when not set.
 */
function getTokenVaultEnvironments() {
  const raw = process.env.TOKEN_VAULT_ENVS || '';
  if (!raw.trim()) {
    return [{ id: 'DEFAULT', label: 'Default' }];
  }
  return raw.split(',').map((s) => {
    const id = s.trim().toUpperCase();
    return { id, label: id };
  });
}

module.exports = {
  readCredentials,
  writeCredentials,
  redact,
  findCredential,
  getTokenVaultEnvironments,
  getEncryptionKey,
};
