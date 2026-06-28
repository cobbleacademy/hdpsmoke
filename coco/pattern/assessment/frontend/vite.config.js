import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  // base must match the Istio VirtualService prefix so Vite generates
  // asset paths the gateway can route to the frontend pod.
  // Istio: match prefix /api/pattern/assessment, rewrite uri /api/pattern/assessment (no-op)
  // → nginx receives the full original path; assets must live under the same prefix.
  base: '/api/pattern/assessment/',
  plugins: [react()],
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
    },
  },
});
