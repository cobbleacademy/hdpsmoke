'use strict';

// ── Variable resolver ─────────────────────────────────────────────────────────

/**
 * Extract Databricks widget default values from notebook.
 * Parses: dbutils.widgets.text("varname", "default_value", "label")
 *
 * @param {string} content  Raw notebook content
 * @returns {Object} Map of variable_name → default_value
 */
function extractWidgetDefaults(content) {
  const variables = {};
  const widgetRegex = /dbutils\.widgets\.text\s*\(\s*"([^"]+)"\s*,\s*"([^"]*)"\s*,/g;

  let match;
  while ((match = widgetRegex.exec(content)) !== null) {
    const varName = match[1];
    const defaultValue = match[2];
    variables[varName] = defaultValue;
  }

  return variables;
}

/**
 * Resolve Python variables in SQL strings.
 * Replaces {variable_name} with values from the variables map.
 * Also handles common transformations like {var_name} → {var_name} with underscore conversion.
 *
 * @param {string} sql       SQL with {variable} placeholders
 * @param {Object} variables Map of variable → value
 * @returns {string} SQL with variables resolved
 */
function resolveSqlVariables(sql, variables) {
  let resolved = sql;

  // Find all {variable} patterns
  const varPattern = /\{([a-zA-Z_][a-zA-Z0-9_]*)\}/g;
  const foundVars = new Set();

  let match;
  while ((match = varPattern.exec(sql)) !== null) {
    foundVars.add(match[1]);
  }

  // Resolve each variable
  for (const varName of foundVars) {
    let value = variables[varName];

    // If not found, try variations (e.g., group_pii_readers might be accessed as pii_readers)
    if (!value) {
      // Try looking for a widget variable that uses this as suffix
      for (const [widgetVar, widgetVal] of Object.entries(variables)) {
        if (widgetVar.includes(varName) || varName.includes(widgetVar.replace('group_', ''))) {
          value = widgetVal;
          break;
        }
      }
    }

    if (value) {
      // Use backticks if value contains special chars, otherwise keep as-is
      const quoted = value.includes('-') || value.includes('.') ? `\`${value}\`` : value;
      resolved = resolved.replace(new RegExp(`\\{${varName}\\}`, 'g'), quoted);
    }
  }

  return resolved;
}

// ── Notebook SQL extractor ────────────────────────────────────────────────────

/**
 * Extract SQL from a Databricks Python notebook (.py).
 * Handles both:
 *  1. # MAGIC %sql cells (Databricks standard)
 *  2. f-string SQL assignments (policy_sql = f"""...""") — including multi-line
 *
 * Rules:
 *  - Lines starting with "# MAGIC %sql"  → begin a SQL block
 *  - Lines starting with "# MAGIC %md"   → end the current block
 *  - "# COMMAND ----------"               → cell separator
 *  - Inside a SQL block, "# MAGIC " prefix is stripped
 *  - Python lines without "# MAGIC" while inSqlBlock → exit SQL block
 *  - f-string assignments (= f"""...""" or = f'''...''') → extract SQL content
 *  - spark.sql(f"""...""") → extract SQL content (both single and multi-line)
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
  let inFString = false;
  let fStringQuote = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();

    // ── Handle # MAGIC %sql blocks ────────────────────────────────────────────
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

    // ── Handle f-string SQL (multi-line) ───────────────────────────────────
    // Look for: var_name = f"""...""" or spark.sql(f"""...""")
    if (!inFString && !inSqlBlock) {
      // Detect start of f-string with proper quote detection
      let fStringStart = -1;
      let quoteType = null;

      if (line.includes('= f"""')) {
        fStringStart = line.indexOf('= f"""') + '= f'.length;
        quoteType = '"""';
      } else if (line.includes("= f'''")) {
        fStringStart = line.indexOf("= f'''") + "= f".length;
        quoteType = "'''";
      } else if (line.includes('spark.sql(f"""')) {
        fStringStart = line.indexOf('spark.sql(f"""') + 'spark.sql(f'.length;
        quoteType = '"""';
      } else if (line.includes("spark.sql(f'''")) {
        fStringStart = line.indexOf("spark.sql(f'''") + "spark.sql(f".length;
        quoteType = "'''";
      }

      if (fStringStart >= 0 && quoteType) {
        fStringQuote = quoteType;
        inFString = true;
        fStringCount++;

        // Find the content between opening and closing quotes
        const afterOpen = line.substring(fStringStart + quoteType.length);
        const closeIdx = afterOpen.indexOf(quoteType);

        if (closeIdx >= 0) {
          // Single-line f-string
          const sqlContent = afterOpen.substring(0, closeIdx).trim();
          if (sqlContent) {
            if (sqlLines.length > 0) sqlLines.push('');
            sqlLines.push(sqlContent);
          }
          inFString = false;
          sqlBlockCount++;
        } else {
          // Multi-line f-string starts
          if (afterOpen.trim()) {
            if (sqlLines.length > 0) sqlLines.push('');
            sqlLines.push(afterOpen);
          }
        }
        continue;
      }
    }

    // ── Continue collecting multi-line f-string content ────────────────────
    if (inFString) {
      if (line.includes(fStringQuote)) {
        // Found closing quote
        const idx = line.indexOf(fStringQuote);
        const beforeClose = line.substring(0, idx).trim();
        if (beforeClose) {
          sqlLines.push(beforeClose);
        }
        inFString = false;
        sqlBlockCount++;
      } else {
        // Still inside f-string — collect this line
        const trimmedLine = line.trim();
        if (trimmedLine && !trimmedLine.startsWith('#')) {
          sqlLines.push(trimmedLine);
        }
      }
      continue;
    }
  }

  return {
    sql: sqlLines.join('\n').trim(),
    sqlBlockCount,
    fStringCount: fStringCount, // Count only f-strings we actually found
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
  let varsResolved = 0;

  if (isNotebook) {
    const extracted = extractNotebookSql(rawContent);
    content = extracted.sql;
    extractedFromNotebook = true;
    sqlBlockCount = extracted.sqlBlockCount;

    // ── Resolve Python variables in SQL ────────────────────────────────────
    const widgetDefaults = extractWidgetDefaults(rawContent);
    if (Object.keys(widgetDefaults).length > 0 && content.includes('{')) {
      const resolvedContent = resolveSqlVariables(content, widgetDefaults);
      const originalVarCount = (content.match(/\{[a-zA-Z_][a-zA-Z0-9_]*\}/g) || []).length;
      const resolvedVarCount = (resolvedContent.match(/\{[a-zA-Z_][a-zA-Z0-9_]*\}/g) || []).length;
      varsResolved = originalVarCount - resolvedVarCount;

      if (varsResolved > 0) {
        content = resolvedContent;
        warnings.push(`${varsResolved} Python variable(s) resolved from widget defaults (${Object.keys(widgetDefaults).join(', ')})`);
      }
    }

    if (extracted.fStringCount > 0) {
      warnings.push(`${extracted.fStringCount} f-string SQL block(s) extracted and processed.`);
    }
    if (sqlBlockCount < 1 && extracted.fStringCount === 0) {
      warnings.push('No SQL blocks found. Use Direct Input mode to paste the policy manually.');
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
