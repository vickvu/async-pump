// Strict, type-aware ESLint config (flat config — ESLint 9/10 + typescript-eslint v8).
// Lints the TypeScript sources in src/ and test/ with full type information.
//
// Formatting is left to Prettier; this config only enforces correctness and
// code-quality rules. Run with `npm run lint` (or `npm run lint:fix`).
import { defineConfig } from 'eslint/config';
import js from '@eslint/js';
import ts from 'typescript-eslint';
import mocha from 'eslint-plugin-mocha';
import prettier from 'eslint-config-prettier';

export default defineConfig(
    // Generated output, coverage, and tooling that isn't part of the TS program.
    { ignores: ['dist/', 'coverage/', '*.config.ts', 'eslint.config.mjs'] },

    // Strict, type-checked linting for the sources. Everything is scoped to the
    // TS files so the type-aware parser never runs on non-program files.
    {
        files: ['src/**/*.ts', 'test/**/*.ts'],
        extends: [js.configs.recommended, ...ts.configs.strictTypeChecked, ...ts.configs.stylisticTypeChecked],
        languageOptions: {
            parserOptions: {
                projectService: true,
                tsconfigRootDir: import.meta.dirname,
            },
        },
    },

    // Mocha rules for the test specs (composes with the type-checked block above).
    {
        files: ['test/**/*.spec.ts'],
        extends: [mocha.configs.recommended],
    },

    // Keep ESLint out of Prettier's lane (must stay last to win).
    prettier,
);
