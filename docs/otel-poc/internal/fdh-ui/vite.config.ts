import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    proxy: {
      '/jaeger': { target: 'http://127.0.0.1:16686', rewrite: path => path.replace(/^\/jaeger/, '') },
      '/prometheus': { target: 'http://127.0.0.1:9090', rewrite: path => path.replace(/^\/prometheus/, '') },
    },
  },
  build: { outDir: 'dist', sourcemap: false },
});