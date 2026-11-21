#!/usr/bin/env node
// Keeps engine words out of the notebook, pipeline and run pages (nova#516 decision 1).
// A member there reads four nouns: Notebook, Version, Schedule, Run.
// See docs: reference/glossary.

import { readFileSync } from 'node:fs'
import { dirname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')

const FILES = [
  'src/routes/Notebooks.tsx',
  'src/routes/NotebookDetail.tsx',
  'src/routes/Pipelines.tsx',
  'src/routes/PipelineDetail.tsx',
  'src/routes/Runs.tsx',
]

// Whole word, case-insensitive, plural allowed.
const BANNED = ['bundle', 'digest', 'target', 'object version', 'DAG', 'CRD'].map(
  (word) => new RegExp(`\\b${word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?:e?s)?\\b`, 'gi'),
)

const JSX_TEXT = /(?<![=-])>([^<>{}]+)</g
const STRING_LITERAL = /'([^'\\\n]*)'|"([^"\\\n]*)"/g
const TEMPLATE_LITERAL = /`([^`]*)`/g

/**
 * Every text a person reads in `source` and where it starts: JSX text — which may span lines, so
 * the match begins at the `>` of the tag before it — quoted strings and templates without `${`.
 * `[^<>{}]` stops at the next tag or expression, so the text is the text and not the markup; the
 * lookbehind keeps an arrow's `=>` from starting one.
 */
function* textsOf(source) {
  for (const match of source.matchAll(JSX_TEXT)) {
    const text = match[1]
    const lead = text.search(/\S/)
    yield { text, start: match.index + 1 + Math.max(lead, 0) }
  }
  for (const match of source.matchAll(STRING_LITERAL)) yield { text: match[1] ?? match[2], start: match.index }
  for (const match of source.matchAll(TEMPLATE_LITERAL)) {
    if (!match[1].includes('${')) yield { text: match[1], start: match.index }
  }
}

let hits = 0
for (const file of FILES) {
  const source = readFileSync(join(root, file), 'utf8')
  const lines = source.split('\n')
  const lineAt = (index) => source.slice(0, index).split('\n').length
  const shown = relative(process.cwd(), join(root, file))
  for (const { text, start } of textsOf(source)) {
    const line = lineAt(start)
    const trimmed = lines[line - 1].trimStart()
    if (trimmed.startsWith('//') || trimmed.startsWith('*') || trimmed.startsWith('/*')) continue
    for (const re of BANNED) {
      for (const match of text.matchAll(re)) {
        hits += 1
        console.log(
          `${shown}:${line}: "${match[0]}" is an engine word; use Notebook, Version, Schedule or Run (docs: reference/glossary)`,
        )
      }
    }
  }
}

if (hits > 0) process.exit(1)
console.log(`vocab-lint: ${FILES.length} files clean`)
