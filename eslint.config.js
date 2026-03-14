import js from '@eslint/js'
import tseslint from 'typescript-eslint'
import eslintPluginPrettierRecommended from 'eslint-plugin-prettier/recommended'

export default tseslint.config(
  {
    ignores: ['__temp/', 'build/', 'node_modules/']
  },

  js.configs.recommended,
  ...tseslint.configs.recommended,

  {
    rules: {
      'array-callback-return': 'error',
      'block-scoped-var': 'error',
      'eqeqeq': ['error', 'always', { null: 'ignore' }],
      'no-var': 'error',
      'prefer-const': 'error',
      'prefer-arrow-callback': 'error',
      'no-constant-condition': ['error', { checkLoops: false }],
      'no-restricted-properties': [
        'error',
        { object: 'describe', property: 'only' },
        { object: 'it', property: 'only' }
      ]
    }
  },

  {
    files: ['**/*.ts', '**/*.tsx'],
    languageOptions: {
      parserOptions: {
        projectService: true
      }
    },
    rules: {
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-use-before-define': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-empty-function': 'off',
      '@typescript-eslint/explicit-function-return-type': 'off',
      '@typescript-eslint/explicit-module-boundary-types': 'off',
      '@typescript-eslint/ban-ts-comment': 'warn',
      '@typescript-eslint/no-wrapper-object-types': 'off',
      '@typescript-eslint/no-unused-expressions': 'off',
      '@typescript-eslint/strict-boolean-expressions': 'warn',
      'no-dupe-class-members': 'off',
      'require-atomic-updates': 'off'
    }
  },

  eslintPluginPrettierRecommended
)
