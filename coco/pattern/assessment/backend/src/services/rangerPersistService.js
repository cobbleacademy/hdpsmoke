'use strict';

const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');

// ── Key — reuses the same PAYLOAD_ENCRYPTION_KEY as payloadService / opaPolicyPersistService ──

function getEncryptionKey() {
  const raw = process.env.PAYLOAD_ENCRYPTION_KEY || '';
  if (!raw) return null;
  if (/^[0-9a-f]{64}$/i.test(raw)) return Buffer.from(raw, 'hex');
  return crypto.scryptSync(raw, 'pattern-payload-v1', 32);
}

function storagePath() {
  return (
    process.env.RANGER_POLICY_STORAGE_PATH ||
    path.join(__dirname, '../../data/ranger-policies')
  );
}

function envDir(envId) {
  const dir = path.join(storagePath(), envId.toLowerCase());
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function manifestPath(envId) {
  return path.join(envDir(envId), 'manifest.json');
}

// Safe file key: lowercase, non-alphanumeric → underscore
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
  if (envelope.v !== 1) throw new Error(`Unknown Ranger policy envelope version: ${envelope.v}`);
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

// ── Manifest CRUD ─────────────────────────────────────────────────────────────

function readManifest(envId) {
  const p = manifestPath(envId);
  if (!fs.existsSync(p)) return { policies: [] };
  try {
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch {
    return { policies: [] };
  }
}

function writeManifest(envId, manifest) {
  fs.writeFileSync(manifestPath(envId), JSON.stringify(manifest, null, 2), 'utf8');
}

/**
 * Upsert a policy entry in the manifest.
 * Matched by policyKey.
 */
function upsertManifestEntry(envId, entry) {
  const manifest = readManifest(envId);
  const idx = manifest.policies.findIndex((p) => p.policyKey === entry.policyKey);
  const record = {
    policyKey:     entry.policyKey,
    name:          entry.name          ?? entry.policyKey,
    serviceType:   entry.serviceType   ?? 'hive',
    service:       entry.service       ?? '',
    lastGenerated: entry.lastGenerated ?? new Date().toISOString(),
  };
  if (idx >= 0) {
    manifest.policies[idx] = record;
  } else {
    manifest.policies.push(record);
  }
  writeManifest(envId, manifest);
  return manifest;
}

/**
 * Remove a policy entry from the manifest and delete its storage file.
 */
function removeManifestEntry(envId, policyKey) {
  const manifest = readManifest(envId);
  manifest.policies = manifest.policies.filter((p) => p.policyKey !== policyKey);
  writeManifest(envId, manifest);

  // Remove encrypted / plain file
  const dir  = envDir(envId);
  const safe = toFileKey(policyKey);
  const enc  = path.join(dir, `${safe}.enc`);
  const plain = path.join(dir, `${safe}.json`);
  if (fs.existsSync(enc))   fs.unlinkSync(enc);
  if (fs.existsSync(plain)) fs.unlinkSync(plain);
}

// ── Policy read / write ───────────────────────────────────────────────────────

/**
 * Read and decrypt (if encrypted) a stored Ranger policy JSON for envId + policyKey.
 * Returns { policy: object, encrypted: boolean } or null if not found.
 */
function readPolicy(envId, policyKey) {
  const dir  = envDir(envId);
  const safe = toFileKey(policyKey);
  const key  = getEncryptionKey();

  const encFile   = path.join(dir, `${safe}.enc`);
  const plainFile = path.join(dir, `${safe}.json`);

  if (fs.existsSync(encFile)) {
    if (!key) {
      throw new Error(
        `Encrypted Ranger policy found for "${policyKey}" but PAYLOAD_ENCRYPTION_KEY is not set.`
      );
    }
    const envelope = JSON.parse(fs.readFileSync(encFile, 'utf8'));
    return { policy: JSON.parse(decrypt(envelope, key)), encrypted: true };
  }

  if (fs.existsSync(plainFile)) {
    return { policy: JSON.parse(fs.readFileSync(plainFile, 'utf8')), encrypted: false };
  }

  return null;
}

/**
 * Encrypt and persist a Ranger policy JSON object for envId + policyKey.
 * Writes .enc when a key is configured, .json otherwise.
 */
function writePolicy(envId, policyKey, policyObj) {
  if (typeof policyObj !== 'object' || policyObj === null) {
    throw new Error('Ranger policy must be a non-null JSON object or array');
  }

  const dir  = envDir(envId);
  const safe = toFileKey(policyKey);
  const key  = getEncryptionKey();
  const text = JSON.stringify(policyObj, null, 2);

  if (key) {
    const envelope = encrypt(text, key);
    fs.writeFileSync(
      path.join(dir, `${safe}.enc`),
      JSON.stringify(envelope, null, 2),
      'utf8'
    );
  } else {
    console.warn(
      `[rangerPersistService] PAYLOAD_ENCRYPTION_KEY not set — ` +
      `storing Ranger policy "${policyKey}" (env: ${envId}) as plain text.`
    );
    fs.writeFileSync(path.join(dir, `${safe}.json`), text, 'utf8');
  }
}

// ── Environments ──────────────────────────────────────────────────────────────

/**
 * Parse RANGER_ENVS into an array of { id, label } objects.
 * Falls back to [{ id: 'DEFAULT', label: 'Default' }] when not set.
 */
function getRangerEnvironments() {
  const raw = process.env.RANGER_ENVS || '';
  if (!raw.trim()) {
    const showPrompt = process.env.RANGER_DEFAULT_SHOW_PROMPT !== 'false';
    return [{ id: 'DEFAULT', label: 'Default', showPrompt }];
  }
  return raw.split(',').map((s) => {
    const id = s.trim().toUpperCase();
    const envKey = id.replace(/-/g, '_');
    // Default true — set RANGER_{ENV}_SHOW_PROMPT=false to hide per environment
    const showPrompt = process.env[`RANGER_${envKey}_SHOW_PROMPT`] !== 'false';
    return { id, label: id, showPrompt };
  });
}

module.exports = {
  readManifest,
  upsertManifestEntry,
  removeManifestEntry,
  readPolicy,
  writePolicy,
  getRangerEnvironments,
  getEncryptionKey,
};
