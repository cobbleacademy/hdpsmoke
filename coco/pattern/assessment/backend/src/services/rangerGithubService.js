'use strict';

// ── Error helpers ─────────────────────────────────────────────────────────────

function makeGithubError(message, code) {
  const e = new Error(message);
  e.code = code;
  return e;
}

async function safeFetch(url, options = {}) {
  let resp;
  try {
    resp = await fetch(url, options);
  } catch (err) {
    throw makeGithubError(`Network error reaching GitHub: ${err.message}`, 'FETCH_ERROR');
  }

  if (resp.status === 404) {
    throw makeGithubError(
      'File not found on GitHub. Check the owner, repo, branch and file path.',
      'FILE_NOT_FOUND'
    );
  }
  if (resp.status === 401) {
    throw makeGithubError(
      'GitHub authentication failed. Check your GITHUB_TOKEN value.',
      'UNAUTHORIZED'
    );
  }
  if (resp.status === 403) {
    throw makeGithubError(
      'GitHub access denied or rate limit exceeded. Set GITHUB_TOKEN for private repos and higher rate limits (60 → 5000 req/hour).',
      'RATE_LIMITED'
    );
  }
  if (!resp.ok) {
    throw makeGithubError(`GitHub returned HTTP ${resp.status}.`, 'FETCH_ERROR');
  }

  return resp;
}

// ── Main fetch function ───────────────────────────────────────────────────────

/**
 * Fetch a Rego policy file from a GitHub repository.
 *
 * Two fetch modes:
 *  - 'api' (default): GitHub Contents API — handles private repos, returns
 *                     base64-encoded content, 1 MB file size limit.
 *  - 'raw':           raw.githubusercontent.com — no size limit, public repos
 *                     or any repo with a valid token.
 *
 * @param {{ owner, repo, branch, filePath, fetchMode?, token? }} opts
 * @returns {{
 *   content: string,
 *   sha: string|null,
 *   sizeBytes: number,
 *   fetchMode: string,
 *   warning: string|null
 * }}
 */
async function fetchRegoFile({ owner, repo, branch, filePath, fetchMode = 'api', token }) {
  const baseHeaders = { 'User-Agent': 'pattern-assessment-ranger-library' };
  if (token) baseHeaders['Authorization'] = `Bearer ${token}`;

  let rawContent, sha, sizeBytes;

  if (fetchMode === 'raw') {
    const rawUrl =
      `https://raw.githubusercontent.com/` +
      `${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/` +
      `${encodeURIComponent(branch)}/${filePath}`;
    const resp = await safeFetch(rawUrl, { headers: baseHeaders });
    rawContent = await resp.text();
    sha = null;
    sizeBytes = Buffer.byteLength(rawContent, 'utf8');
  } else {
    const apiUrl =
      `https://api.github.com/repos/` +
      `${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/` +
      `contents/${filePath}?ref=${encodeURIComponent(branch)}`;
    const apiHeaders = {
      ...baseHeaders,
      Accept: 'application/vnd.github.v3+json',
    };
    const resp = await safeFetch(apiUrl, { headers: apiHeaders });
    const data = await resp.json();
    rawContent = Buffer.from(data.content.replace(/\n/g, ''), 'base64').toString('utf8');
    sha = data.sha || null;
    sizeBytes = data.size || Buffer.byteLength(rawContent, 'utf8');
  }

  const warnings = [];

  if (!rawContent.includes('package ')) {
    warnings.push(
      'File does not appear to contain a Rego package declaration. ' +
      'Verify the path points to a .rego file.'
    );
  }

  if (sizeBytes > 50_000) {
    warnings.push('File is large (> 50 KB). Generation may be slower or truncated.');
  }

  return {
    content: rawContent,
    sha,
    sizeBytes,
    fetchMode,
    warning: warnings.length > 0 ? warnings.join(' | ') : null,
  };
}

module.exports = { fetchRegoFile };
