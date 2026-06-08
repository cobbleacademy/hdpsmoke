'use strict';

// ── Notebook SQL extractor ────────────────────────────────────────────────────

/**
 * Extract SQL from a Databricks Python notebook (.py).
 * Notebooks interleave # MAGIC %sql cells with Python code and # MAGIC %md markdown.
 *
 * Rules:
 *  - Lines starting with "# MAGIC %sql"  → begin a SQL block
 *  - Lines starting with "# MAGIC %md"   → end the current block (markdown)
 *  - Lines starting with "# MAGIC %python" → end the current block
 *  - "# COMMAND ----------"               → cell separator, ends current block
 *  - Inside a SQL block, lines starting "# MAGIC " → strip that prefix, keep rest
 *  - Python lines (no "# MAGIC" prefix) while inSqlBlock → exit SQL block
 *  - spark.sql(f"""...""") style SQL      → detected and counted as warning
 *
 * @param {string} content  Raw .py file content
 * @returns {{ sql: string, sqlBlockCount: number, fStringCount: number }}
 */
function extractNotebookSql(content) {
  const lines = content.split('\n');
  const sqlLines = [];
  let inSqlBlock = false;
  let sqlBlockCount = 0;
  let fStringCount = 0;

  for (const line of lines) {
    const trimmed = line.trim();

    if (trimmed === '# COMMAND ----------') {
      inSqlBlock = false;
      continue;
    }

    if (trimmed === '# MAGIC %sql') {
      if (!inSqlBlock) {
        inSqlBlock = true;
        sqlBlockCount++;
        if (sqlLines.length > 0) sqlLines.push('');
      }
      continue;
    }

    if (
      trimmed.startsWith('# MAGIC %md') ||
      trimmed.startsWith('# MAGIC %python') ||
      trimmed.startsWith('# MAGIC %sh')
    ) {
      inSqlBlock = false;
      continue;
    }

    if (inSqlBlock) {
      if (line.startsWith('# MAGIC ')) {
        sqlLines.push(line.slice('# MAGIC '.length));
      } else if (trimmed.startsWith('#')) {
        // comment line — keep in SQL context
        sqlLines.push(trimmed);
      } else {
        // Python code — exit SQL block
        inSqlBlock = false;
      }
      continue;
    }

    // Detect f-string SQL outside magic blocks
    if (
      line.includes('spark.sql(f"""') ||
      line.includes("spark.sql(f'''") ||
      line.includes('spark.sql(f"') ||
      line.includes("spark.sql(f'")
    ) {
      fStringCount++;
    }
  }

  return {
    sql: sqlLines.join('\n').trim(),
    sqlBlockCount,
    fStringCount,
  };
}

// ── Error helper ──────────────────────────────────────────────────────────────

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
      `File not found on GitHub. Check the owner, repo, branch and file path.`,
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
 * Fetch ABAC policy content from a GitHub repository.
 *
 * Two fetch modes:
 *  - 'api' (default): GitHub Contents API — handles private repos, returns base64,
 *                     has a 1 MB file size limit.
 *  - 'raw':           raw.githubusercontent.com — simpler, no size limit,
 *                     works for public repos or with a token.
 *
 * Notebook handling:
 *  Files ending in .py are treated as Databricks notebooks. SQL is extracted
 *  from # MAGIC %sql blocks. f-string SQL blocks (spark.sql(f"""...""")) are
 *  skipped and counted as a warning.
 *
 * @param {{ owner, repo, branch, filePath, fetchMode?, token? }} opts
 * @returns {{
 *   content: string,
 *   sha: string|null,
 *   sizeBytes: number,
 *   fetchMode: string,
 *   extractedFromNotebook: boolean,
 *   sqlBlockCount: number,
 *   warning: string|null
 * }}
 */
async function fetchAbacPolicy({ owner, repo, branch, filePath, fetchMode = 'api', token }) {
  const baseHeaders = { 'User-Agent': 'pattern-assessment-opa-generator' };
  if (token) baseHeaders['Authorization'] = `Bearer ${token}`;

  let rawContent, sha, sizeBytes;

  if (fetchMode === 'raw') {
    const rawUrl = `https://raw.githubusercontent.com/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/${encodeURIComponent(branch)}/${filePath}`;
    const resp = await safeFetch(rawUrl, { headers: baseHeaders });
    rawContent = await resp.text();
    sha = null;
    sizeBytes = Buffer.byteLength(rawContent, 'utf8');
  } else {
    // API mode — file path must NOT be encoded as a whole; each segment is encoded
    const apiUrl =
      `https://api.github.com/repos/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/contents/${filePath}` +
      `?ref=${encodeURIComponent(branch)}`;
    const apiHeaders = { ...baseHeaders, Accept: 'application/vnd.github.v3+json' };
    const resp = await safeFetch(apiUrl, { headers: apiHeaders });
    const data = await resp.json();
    // data.content is base64 with possible newlines
    rawContent = Buffer.from(data.content.replace(/\n/g, ''), 'base64').toString('utf8');
    sha = data.sha || null;
    sizeBytes = data.size || Buffer.byteLength(rawContent, 'utf8');
  }

  // ── Notebook extraction ───────────────────────────────────────────────────
  const isNotebook = filePath.endsWith('.py') || filePath.endsWith('.ipynb');
  let content = rawContent;
  let extractedFromNotebook = false;
  let sqlBlockCount = 0;
  const warnings = [];

  if (isNotebook) {
    const extracted = extractNotebookSql(rawContent);
    content = extracted.sql;
    extractedFromNotebook = true;
    sqlBlockCount = extracted.sqlBlockCount;

    if (extracted.fStringCount > 0) {
      warnings.push(`${extracted.fStringCount} f-string SQL block(s) were skipped (contain Python variables). Paste them manually in Direct Input mode.`);
    }
    if (sqlBlockCount < 1) {
      warnings.push('No # MAGIC %sql blocks found. Use Direct Input mode to paste the policy manually.');
    }
  }

  if (sizeBytes > 50_000) {
    warnings.push('File is large (> 50 KB). Generation may be slower or truncated.');
  }

  return {
    content,
    sha,
    sizeBytes,
    fetchMode,
    extractedFromNotebook,
    sqlBlockCount,
    warning: warnings.length > 0 ? warnings.join(' | ') : null,
  };
}

module.exports = { fetchAbacPolicy };
