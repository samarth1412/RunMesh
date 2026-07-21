import { configDefaults, defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: { proxy: { '/v1': 'http://control-plane:8080', '/health': 'http://control-plane:8080' } },
  test: {
    environment: 'jsdom',
    globals: true,
    exclude: [...configDefaults.exclude, 'e2e/**', 'demo/**'],
    coverage: {
      provider: 'v8',
      include: ['src/**/*.{ts,tsx}'],
      exclude: ['src/vite-env.d.ts', 'src/main.tsx'],
      reporter: ['text', 'json-summary'],
      thresholds: { lines: 50, functions: 50, statements: 50, branches: 45 },
    },
  },
})
