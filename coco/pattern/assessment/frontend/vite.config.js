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
    },
  },
});
