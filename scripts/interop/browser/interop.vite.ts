import {defineConfig} from 'vite';
import {workletInline} from './js/common/vite-plugin-worklet.ts';
import {crossOriginIsolation} from './js/common/vite-plugin-isolate.ts';
export default defineConfig({
  root: 'interop',
  plugins: [workletInline(), crossOriginIsolation()],
  server: {host: '127.0.0.1', port: 5179, strictPort: true},
  optimizeDeps: {exclude: ['@libav.js/variant-opus-af']}
});
