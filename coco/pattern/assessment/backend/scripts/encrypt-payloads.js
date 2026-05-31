#!/usr/bin/env node
'use strict';

/**
 * Payload Encryption Migration Script
 * ─────────────────────────────────────────────────────────────────────────────
 * Encrypts plain YAML payload files with AES-256-GCM, or re-keys already-
 * encrypted files from an old key to a new one.
 *
 * USAGE
 * ─────
 * Initial encryption (plain YAML → .enc):
 *
 *   PAYLOAD_ENCRYPTION_KEY=<new-hex-key> \
 *   node backend/scripts/encrypt-payloads.js
 *
 * Re-key (old .enc → new .enc):
 *
 *   PAYLOAD_ENCRYPTION_KEY=<new-hex-key> \
 *   PAYLOAD_OLD_KEY=<old-hex-key> \
 *   node backend/scripts/encrypt-payloads.js --rekey
 *
 * Dry run (print what would happen, make no changes):
 *
 *   PAYLOAD_ENCRYPTION_KEY=<key> \
 *   node backend/scripts/encrypt-payloads.js --dry-run
 *
 * OPTIONS
 * ───────
 *   --rekey      Re-encrypt existing .enc files with PAYLOAD_ENCRYPTION_KEY.
 *                Requires PAYLOAD_OLD_KEY env var (the key used to encrypt them).
 *   --dry-run    Print planned operations without writing any files.
 *   --source     Source directory for plain YAML files.
 *                Default: frontend/public/payloads/ (relative to project root)
 *   --dest       Destination directory for .enc files.
 *                Default: PAYLOAD_STORAGE_PATH || backend/data/payloads/
 *
 * KEY FORMATS
 * ───────────
 *   PAYLOAD_ENCRYPTION_KEY and PAYLOAD_OLD_KEY accept:
 *   - 64 lowercase hex chars (32 bytes, used directly)
 *   - Any other string (passphrase, derived via scrypt)
 *
 * Generate a new random key:
 *   openssl rand -hex 32
 *
 * SECURITY
 * ────────
 *   Run this script on a trusted machine, not in CI/CD pipelines.
 *   The old key is only read from PAYLOAD_OLD_KEY during --rekey.
 *   After re-keying, update PAYLOAD_ENCRYPTION_KEY in all deployments
 *   before restarting the backend.
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const crypto  = require('crypto');
const fs      = require('fs');
const path    = require('path');

// ── Parse CLI args ────────────────────────────────────────────────────────────
const args    = process.argv.slice(2);
const isRekey = args.includes('--rekey');
const isDry   = args.includes('--dry-run');

function argValue(flag) {
  const idx = args.findIndex(a => a === flag);
  return idx !== -1 ? args[idx + 1] : null;
}

const PROJECT_ROOT  = path.join(__dirname, '..', '..');
const DEFAULT_SRC   = path.join(PROJECT_ROOT, 'frontend', 'public', 'payloads');
const DEFAULT_DEST  = process.env.PAYLOAD_STORAGE_PATH ||
                      path.join(PROJECT_ROOT, 'backend', 'data', 'payloads');

const srcDir  = argValue('--source') || DEFAULT_SRC;
const destDir = argValue('--dest')   || DEFAULT_DEST;

// ── Key helpers ───────────────────────────────────────────────────────────────
function deriveKey(raw, label) {
  if (!raw) die(`${label} is required but not set`);
  if (/^[0-9a-f]{64}$/i.test(raw)) return Buffer.from(raw, 'hex');
  console.log(`  [key] Deriving ${label} via scrypt (passphrase mode)...`);
  return crypto.scryptSync(raw, 'pattern-payload-v1', 32);
}

function encrypt(text, key) {
  const iv     = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ct     = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
  return { v: 1, iv: iv.toString('hex'), tag: cipher.getAuthTag().toString('hex'), ciphertext: ct.toString('hex') };
}

function decrypt(envelope, key) {
  if (envelope.v !== 1) die(`Unknown envelope version: ${envelope.v}`);
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

// ── Utilities ─────────────────────────────────────────────────────────────────
function die(msg) { console.error(`\nERROR: ${msg}\n`); process.exit(1); }

function banner(title) {
  console.log('\n' + '─'.repeat(60));
  console.log(`  ${title}`);
  console.log('─'.repeat(60));
}

function writeFile(filePath, content) {
  if (isDry) {
    console.log(`  [dry-run] Would write: ${filePath}`);
    return;
  }
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content, 'utf8');
  console.log(`  ✅ Written: ${filePath}`);
}

// ── Main ──────────────────────────────────────────────────────────────────────
function main() {
  const newKey = deriveKey(process.env.PAYLOAD_ENCRYPTION_KEY, 'PAYLOAD_ENCRYPTION_KEY');

  if (isRekey) {
    // ── Re-key: decrypt .enc files with old key, re-encrypt with new key ──
    banner('Re-key: re-encrypting payload files with new key');
    console.log(`  Source (enc files): ${destDir}`);
    console.log(`  Dest   (enc files): ${destDir}  (in-place)`);
    if (isDry) console.log('  [DRY RUN — no files will be written]\n');

    const oldKey = deriveKey(process.env.PAYLOAD_OLD_KEY, 'PAYLOAD_OLD_KEY');

    if (!fs.existsSync(destDir)) die(`Source directory not found: ${destDir}`);
    const encFiles = fs.readdirSync(destDir).filter(f => f.endsWith('.enc'));
    if (encFiles.length === 0) die(`No .enc files found in ${destDir}`);

    let ok = 0, failed = 0;
    for (const file of encFiles) {
      const encPath = path.join(destDir, file);
      try {
        const envelope = JSON.parse(fs.readFileSync(encPath, 'utf8'));
        const plainText = decrypt(envelope, oldKey);
        const newEnvelope = encrypt(plainText, newKey);
        writeFile(encPath, JSON.stringify(newEnvelope, null, 2));
        ok++;
      } catch (e) {
        console.error(`  ❌ Failed to re-key ${file}: ${e.message}`);
        failed++;
      }
    }

    console.log(`\n  Done: ${ok} re-keyed, ${failed} failed.`);
    if (failed > 0) process.exit(1);

  } else {
    // ── Initial encryption: plain YAML → .enc ─────────────────────────────
    banner('Encrypting plain YAML payload files');
    console.log(`  Source (plain YAML): ${srcDir}`);
    console.log(`  Dest   (enc files):  ${destDir}`);
    if (isDry) console.log('  [DRY RUN — no files will be written]\n');

    if (!fs.existsSync(srcDir)) die(`Source directory not found: ${srcDir}`);

    const yamlFiles = fs.readdirSync(srcDir).filter(f => f.endsWith('.yaml') || f.endsWith('.yml'));
    if (yamlFiles.length === 0) die(`No YAML files found in ${srcDir}`);

    let ok = 0, skipped = 0, failed = 0;
    for (const file of yamlFiles) {
      const envId   = file.replace(/\.ya?ml$/, '');
      const srcPath = path.join(srcDir, file);
      const dstPath = path.join(destDir, `${envId}.enc`);

      // Skip if an encrypted file already exists (don't overwrite silently)
      if (fs.existsSync(dstPath) && !isDry) {
        console.log(`  ⏭️  Skipped (already encrypted): ${envId}`);
        skipped++;
        continue;
      }

      try {
        const yaml     = fs.readFileSync(srcPath, 'utf8');
        const envelope = encrypt(yaml, newKey);
        writeFile(dstPath, JSON.stringify(envelope, null, 2));
        ok++;
      } catch (e) {
        console.error(`  ❌ Failed to encrypt ${file}: ${e.message}`);
        failed++;
      }
    }

    console.log(`\n  Done: ${ok} encrypted, ${skipped} skipped (already exist), ${failed} failed.`);
    if (ok > 0 && !isDry) {
      console.log('\n  Next steps:');
      console.log('  1. Set PAYLOAD_ENCRYPTION_KEY in your backend env / Helm values / docker-compose');
      console.log('  2. Set PAYLOAD_STORAGE_PATH to the directory containing the .enc files');
      console.log(`  3. Mount that directory as a volume in your container: ${destDir}`);
      console.log('  4. Restart the backend — it will now serve decrypted content via the API');
    }
    if (failed > 0) process.exit(1);
  }
}

main();
