#!/usr/bin/env node
// Converts hsm_bouncy's master architecture-diagram.svg into the JSX body used
// by frontend/src/pages/HsmDemo.jsx's ArchitectureDiagram(). Master's SVG is
// hand-authored, one element per line, with the exact same colors/coordinates
// our replica already mirrors — so this is a mechanical attribute-renaming
// pass (kebab-case -> camelCase, self-closing tags, {/{ }/} escaping for JSX
// text), not a content rewrite. Output is a JSX fragment to eyeball-diff
// against the current <svg>...</svg> body in HsmDemo.jsx, not an auto-apply.
//
// Usage: node scripts/sync-hsm-architecture.js <path-to-architecture-diagram.svg>

const fs = require('fs');

const ATTR_RENAME = {
  'stroke-width': 'strokeWidth',
  'stroke-dasharray': 'strokeDasharray',
  'stroke-linecap': 'strokeLinecap',
  'marker-end': 'markerEnd',
  'marker-start': 'markerStart',
  'text-anchor': 'textAnchor',
  'font-size': 'fontSize',
  'font-family': 'fontFamily',
  'font-weight': 'fontWeight',
  'letter-spacing': 'letterSpacing',
  'stop-color': 'stopColor',
  'clip-path': 'clipPath',
};

function renameAttrs(attrString) {
  // attrString: 'x="10" y="20" stroke-width="1.5"'
  return attrString.replace(/([a-zA-Z-]+)="([^"]*)"/g, (_, name, value) => {
    const jsxName = ATTR_RENAME[name] || name;
    if (jsxName === 'transform') {
      // master: rotate(-90,931,833)  ->  JSX convention used in this file: rotate(-90 931 833)
      value = value.replace(/rotate\(([^)]+)\)/, (_m, inner) => `rotate(${inner.replace(/,/g, ' ')})`);
    }
    if (jsxName === 'd') {
      // master: "M 640,700 L 640,750"  ->  this file's own convention: "M 640 700 L 640 750"
      value = value.replace(/,/g, ' ');
    }
    return `${jsxName}="${value}"`;
  });
}

function escapeJsxText(text) {
  // Curly braces aren't valid raw JSX text -- this file's own convention
  // wraps each one as a JS-expression child: {'{'}  /  {'}'}
  return text.replace(/[{}]/g, (ch) => `{'${ch}'}`);
}

function convertLine(line) {
  const trimmed = line.trim();
  if (!trimmed) return null;

  // Comments pass through as JSX comments
  const commentMatch = trimmed.match(/^<!--(.*)-->$/s);
  if (commentMatch) return `{/*${commentMatch[1]}*/}`;

  // <text ...>content</text>
  const textMatch = trimmed.match(/^<text\s+([^>]*)>([^<]*)<\/text>$/);
  if (textMatch) {
    const [, attrs, content] = textMatch;
    return `<text ${renameAttrs(attrs)}>${escapeJsxText(content)}</text>`;
  }

  // Self-closing elements: rect, line, path, polygon, circle, marker (single-line only), stop
  const selfCloseMatch = trimmed.match(/^<(rect|line|path|polygon|circle|stop)\s+([^>]*?)\/?>$/);
  if (selfCloseMatch) {
    const [, tag, attrs] = selfCloseMatch;
    return `<${tag} ${renameAttrs(attrs)} />`;
  }

  // <g ...> opening / </g> closing / <defs> / </defs>
  if (/^<g[\s>]/.test(trimmed)) {
    const attrs = trimmed.match(/^<g\s*([^>]*)>$/);
    return attrs && attrs[1] ? `<g ${renameAttrs(attrs[1])}>` : '<g>';
  }
  if (trimmed === '</g>') return '</g>';
  if (trimmed === '<defs>') return '<defs>';
  if (trimmed === '</defs>') return '</defs>';

  // marker with nested polygon on one line -- handled separately below
  return { unhandled: trimmed };
}

function convertMarkerLine(line) {
  // <marker id="arr-blue" ...><polygon .../></marker>  (all on one line in master)
  const m = line.trim().match(/^<marker\s+([^>]*)>(.*)<\/marker>$/);
  if (!m) return null;
  const [, attrs, inner] = m;
  const innerMatch = inner.trim().match(/^<polygon\s+([^>]*?)\/?>$/);
  const innerJsx = innerMatch ? `<polygon ${renameAttrs(innerMatch[1])} />` : inner;
  // no space before the closing </marker> -- matches this file's existing style
  return `<marker ${renameAttrs(attrs)}>${innerJsx}</marker>`;
}

function main() {
  const svgPath = process.argv[2];
  if (!svgPath) {
    console.error('Usage: node sync-hsm-architecture.js <architecture-diagram.svg>');
    process.exit(1);
  }
  const src = fs.readFileSync(svgPath, 'utf8');
  // Join multi-line HTML comments onto one logical line before per-line processing.
  const joined = src.replace(/<!--[\s\S]*?-->/g, (m) => m.replace(/\s*\n\s*/g, ' '));
  const lines = joined.split('\n');

  const out = [];
  const unhandled = [];

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    if (/^<svg[\s>]/.test(trimmed)) continue; // outer wrapper -- HsmDemo.jsx supplies its own
    if (/^<\/svg>$/.test(trimmed)) continue;
    if (/^<title>/.test(trimmed)) continue; // HsmDemo.jsx keeps its own <title> wording
    if (/^<desc>/.test(trimmed)) continue; // <desc> is hand-curated prose, not mechanically portable
    if (/^<rect width=/.test(trimmed)) { out.push(convertLine(trimmed)); continue; } // background rect

    if (/^<marker\s/.test(trimmed)) {
      const converted = convertMarkerLine(trimmed);
      out.push(converted || `/* UNHANDLED MARKER: ${trimmed} */`);
      continue;
    }

    const converted = convertLine(trimmed);
    if (converted === null) continue;
    if (typeof converted === 'object' && converted.unhandled) {
      unhandled.push(converted.unhandled);
      out.push(`/* UNHANDLED: ${converted.unhandled} */`);
    } else {
      out.push(converted);
    }
  }

  console.log(out.join('\n'));
  if (unhandled.length) {
    console.error(`\n--- ${unhandled.length} unhandled line(s), see /* UNHANDLED */ markers above ---`);
  }
}

main();
