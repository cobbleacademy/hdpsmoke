import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  // base must match the Istio VirtualService prefix so Vite generates
  // asset paths the gateway can route to the frontend pod.
  // Istio: match prefix /api/pattern/assessment, rewrite uri /api/pattern/assessment (no-op)
  // → nginx receives the full original path; assets must live under the same prefix.
  base: '/api/pattern/assessment/',
  plugins: [react()],
  // Two build entries, one repo, one Vite config:
  //   main   — the full 11-feature app-user experience (index.html)
  //   access — the end-user "Access Control" bundle: just Permission Checker
  //            + Identity Audit (access-control/index.html). A genuinely
  //            separate JS/CSS output, not a runtime-filtered view of the
  //            main bundle — the end-user audience never downloads code for
  //            the quiz/Payload Library/OPA/Ranger/HSM/Governance features.
  build: {
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
        access: resolve(__dirname, 'access-control/index.html'),
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api/pattern/assessment/questions': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/submit': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/provider-config': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/run-payload': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/payload-content': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/opa-config': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/opa-generate': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/opa-policies': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/opa-policy': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/opa-manifest': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/opa-parse': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/opa-stale': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/ranger-config': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/ranger-fetch': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/ranger-generate': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/ranger-manifest': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/ranger-policy': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      // ── Sensec HSM encryption service — LOCAL DEV ONLY ──────────────────────
      // In any real environment Istio routes /api/sensec/hsm/v1/* directly to
      // that service's own pod, independent of this app's /api/pattern/assessment
      // route. This proxy only exists so the dev server can reach it locally.
      '/api/sensec/hsm': {
        target: 'http://localhost:3005',
        changeOrigin: true,
      },
      '/api/pattern/assessment/permission-config': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/check-permission': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/identity-audit-config': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/identity-audit': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/governance-lifecycle-config': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/pattern/assessment/app-config': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      // Prefix match (not exact) — covers /pattern-templates and
      // /pattern-templates/:id alike, same treatment as payload-content/:envId.
      '/api/pattern/assessment/pattern-templates': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      // ── Access Control (end-user app) — same routers, second mount point ──────
      '/api/access/control/permission-config': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/access/control/check-permission': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/access/control/identity-audit-config': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/api/access/control/identity-audit': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
    },
  },
});
