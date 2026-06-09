import { defineConfig } from 'tsup';

export default defineConfig([
    {
        entry: ['src/index.ts'],
        format: ['esm', 'cjs'],
        dts: true,
        outDir: 'dist',
        target: 'node16',
        clean: true,
        sourcemap: false,
        splitting: false,
        treeshake: true,
    },
]);
