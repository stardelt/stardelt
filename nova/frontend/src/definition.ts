import { parseDocument } from 'yaml'
import type { CreatePipelineRequest, PipelineDocument, PipelineOptions, UpdatePipelineRequest } from './api'

/**
 * Reading a pipeline's definition as the person edited it (stardelt-nova#351). The text is parsed
 * here only to show errors early and to build the request; Nova parses nothing and checks every
 * rule again (`pipelines_validate`, `pipelines_update`), so nothing in this file decides what is
 * allowed.
 */

/** The top-level keys of a pipeline document, the body of `POST /api/pipelines`. */
export const DEFINITION_KEYS = ['name', 'ownerGroup', 'description', 'sql', 'notebook', 'tasks', 'schedule', 'triggers', 'params', 'options', 'notify', 'viewers', 'publish'] as const

/** A definition far larger than any statement Nova accepts is refused without being parsed. */
export const MAX_DEFINITION_BYTES = 64 * 1024

/** What an update can change, in words (the `changes` of `pipelines_validate`). */
export const CHANGE_WORDS: Record<string, string> = {
  statement: 'the statement',
  notebook: 'the notebook',
  tasks: 'the tasks',
  schedule: 'the schedule',
  description: 'the description',
  triggers: 'the trigger',
  params: 'the parameters',
  options: 'the options',
  deployment: 'the deployment',
}

export type Parsed =
  | { ok: true; document: PipelineDocument }
  | { ok: false; message: string; line: number | null }

function lineOf(text: string, key: string): number | null {
  const index = text.split('\n').findIndex((line) => line.startsWith(`${key}:`))
  return index < 0 ? null : index + 1
}

/** YAML text to a document: syntax errors with their line, no anchors or aliases, known keys only. */
export function parseDefinition(text: string): Parsed {
  if (new TextEncoder().encode(text).length > MAX_DEFINITION_BYTES) {
    return { ok: false, message: 'The definition is larger than 64 KiB.', line: null }
  }
  const doc = parseDocument(text, { uniqueKeys: true })
  const first = doc.errors[0]
  if (first) {
    const message = first.message.split('\n')[0].replace(/ at line \d+, column \d+:?$/, '')
    return { ok: false, message, line: first.linePos?.[0].line ?? null }
  }
  let value: unknown
  try {
    value = doc.toJS({ maxAliasCount: 0 })
  } catch {
    return { ok: false, message: 'Anchors and aliases (& and *) are not allowed in a definition.', line: null }
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return { ok: false, message: `The definition must be a mapping with the keys ${DEFINITION_KEYS.join(', ')}.`, line: null }
  }
  const unknown = Object.keys(value).find((key) => !(DEFINITION_KEYS as readonly string[]).includes(key))
  if (unknown !== undefined) {
    return {
      ok: false,
      message: `Unknown key ${JSON.stringify(unknown)}; the keys are ${DEFINITION_KEYS.join(', ')}.`,
      line: lineOf(text, unknown),
    }
  }
  return { ok: true, document: value as PipelineDocument }
}

/**
 * The rules this editor adds to Nova's: the name and the group are what identify the pipeline, so
 * a definition that changes them would be about another pipeline. `null` when nothing is wrong.
 */
export function editorRule(document: PipelineDocument, original: PipelineDocument): string | null {
  if (typeof document.name !== 'string' || typeof document.ownerGroup !== 'string') {
    return 'The definition needs a name and an ownerGroup.'
  }
  if (document.name !== original.name) return 'The name cannot change here: it would be another pipeline.'
  if (document.ownerGroup !== original.ownerGroup) return 'The group of a pipeline cannot change.'
  return null
}

/** The body of `POST /api/pipelines/validate`: the document, a schedule or `null`. */
export function toValidateRequest(document: PipelineDocument): CreatePipelineRequest {
  return {
    name: document.name,
    ownerGroup: document.ownerGroup ?? '',
    description: document.description,
    sql: document.sql,
    notebook: document.notebook,
    ...(document.tasks ? { tasks: document.tasks } : {}),
    schedule: document.schedule ?? null,
    triggers: document.triggers,
    params: document.params,
    options: document.options as PipelineOptions | undefined,
    notify: document.notify ?? undefined,
    viewers: document.viewers,
    publish: document.publish ?? undefined,
  }
}

/** The body of `PUT /api/pipelines/{namespace}/{name}`, with the version the editor read. */
export function toUpdateRequest(document: PipelineDocument, resourceVersion: string | null): UpdatePipelineRequest {
  return {
    description: document.description,
    sql: document.sql,
    notebook: document.notebook,
    ...(document.tasks ? { tasks: document.tasks } : {}),
    schedule: document.schedule ?? null,
    triggers: document.triggers ?? null,
    params: document.params ?? {},
    options: document.options as PipelineOptions | undefined,
    notify: document.notify ?? undefined,
    viewers: document.viewers,
    publish: document.publish ?? undefined,
    ...(resourceVersion ? { resourceVersion } : {}),
  }
}
