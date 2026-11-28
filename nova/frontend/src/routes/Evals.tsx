import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useState, type FormEvent } from 'react'
import { useSearchParams } from 'react-router-dom'
import { ApiError, api, errorMessage, type EvalWithLastRun, type NewEval, type NewEvalRun } from '../api'
import CatalogSelect from '../components/CatalogSelect'
import ErrorBox from '../components/ErrorBox'
import { formatTime, relativeTime } from '../format'
import { useMe } from '../hooks'

/** The expected answer the form starts with: the smallest of the three shapes. */
const DEFAULT_EXPECTED = '{"kind":"value","value":0}'
/** What the textarea holds, in words, before a person sees the API refuse a shape. */
const EXPECTED_HINT =
  'A JSON object: kind value with a value, kind uses with namespace.table names, or kind rows with a list of rows.'
/** The sentence the page shows itself, without asking the API. */
const BAD_OBJECT = 'the expected answer must be a JSON object'

/**
 * The expected answer in words (stardelt/stardelt#125 slice 4): the value, the tables a
 * statement must use, or the rows. Never blank, and never executed — the API is the one
 * that compares it with an answer.
 */
function describe(expected: unknown): string {
  if (typeof expected === 'object' && expected !== null) {
    const e = expected as { kind?: unknown; value?: unknown; tables?: unknown; rows?: unknown }
    if (e.kind === 'value') return `the value ${JSON.stringify(e.value)}`
    if (e.kind === 'uses' && Array.isArray(e.tables) && e.tables.every((t) => typeof t === 'string'))
      return `uses ${(e.tables as string[]).join(', ')}`
    if (e.kind === 'rows') return `the rows ${JSON.stringify(e.rows)}`
  }
  return 'the stored answer'
}

/**
 * Evals (#125 slices 4 and 5): the questions a catalog must always answer correctly, the form
 * that adds one, and each question's newest verdict with the catalog's score.
 */
export default function Evals() {
  const [params] = useSearchParams()
  const catalog = params.get('catalog') ?? undefined
  const queryKey = ['evals', catalog ?? '']
  const qc = useQueryClient()
  const me = useMe()
  const groups = me.data?.groups ?? []
  const list = useQuery({ queryKey, queryFn: () => api.evals(catalog) })
  const rows = list.data ?? []

  const [adding, setAdding] = useState(false)
  const [question, setQuestion] = useState('')
  const [ownerGroup, setOwnerGroup] = useState('')
  const [expectedText, setExpectedText] = useState(DEFAULT_EXPECTED)
  const [formError, setFormError] = useState<unknown>(null)
  const [removeError, setRemoveError] = useState<unknown>(null)
  const [saving, setSaving] = useState(false)
  const [removing, setRemoving] = useState<number | null>(null)
  const [running, setRunning] = useState<number | 'all' | null>(null)
  const [runError, setRunError] = useState<unknown>(null)
  const mayRun = (row: EvalWithLastRun) => me.data?.is_admin === true || groups.includes(row.ownerGroup)
  const runnable = rows.filter(mayRun)

  /** The CLI's three calls for one row (#125 point 2): Ask, execute, post the answer. */
  async function runOne(row: EvalWithLastRun) {
    const answer = await api.ask(row.question, catalog)
    const meta = { model: answer.model, endpoint: answer.endpoint }
    let body: NewEvalRun
    if (!answer.sql) {
      body = { ...meta, error: (answer.sentence || 'Ask answered no SQL').slice(0, 2000) }
    } else {
      const sql = answer.sql
      try {
        const result = await api.query(sql)
        body = { ...meta, sql, rows: result.rows as NewEvalRun['rows'] }
      } catch (err) {
        body = { ...meta, sql, error: errorMessage(err as unknown).slice(0, 2000) }
      }
    }
    await api.runEval(row.id, body)
  }

  async function run(row: EvalWithLastRun) {
    setRunError(null)
    setRunning(row.id)
    try {
      await runOne(row)
    } catch (err) {
      setRunError(err)
    } finally {
      setRunning(null)
      await qc.invalidateQueries({ queryKey })
    }
  }

  async function runAll() {
    setRunError(null)
    setRunning('all')
    try {
      for (const row of rows.filter(mayRun)) await runOne(row)
    } catch (err) {
      setRunError(err)
    } finally {
      setRunning(null)
      await qc.invalidateQueries({ queryKey })
    }
  }

  async function submit(event: FormEvent) {
    event.preventDefault()
    setFormError(null)
    let parsed: unknown
    try {
      parsed = JSON.parse(expectedText)
    } catch {
      setFormError(new ApiError(BAD_OBJECT, 400))
      return
    }
    if (typeof parsed !== 'object' || parsed === null) {
      // The page answers this one itself: nothing leaves the browser.
      setFormError(new ApiError(BAD_OBJECT, 400))
      return
    }
    setSaving(true)
    try {
      await api.createEval(
        { ownerGroup, question: question.trim(), expected: parsed as NewEval['expected'] },
        catalog,
      )
      setQuestion('')
      setExpectedText(DEFAULT_EXPECTED)
      setAdding(false)
      await qc.invalidateQueries({ queryKey })
    } catch (e) {
      setFormError(e)
    } finally {
      setSaving(false)
    }
  }

  async function remove(id: number) {
    setRemoveError(null)
    setRemoving(id)
    try {
      await api.deleteEval(id)
      await qc.invalidateQueries({ queryKey })
    } catch (e) {
      setRemoveError(e)
    } finally {
      setRemoving(null)
    }
  }

  return (
    <div className="max-w-6xl space-y-5">
      <header className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <h1 className="text-2xl font-bold tracking-tight">Evals</h1>
          <p className="mt-1 text-sm text-text-dim">
            Questions with a known answer. After a model, a guide or a schema changes, an eval
            run asks them again and says what changed.
          </p>
          {(() => {
            const ran = rows.filter((row) => row.lastRun !== null)
            if (ran.length === 0) return null
            const passed = ran.filter((row) => row.lastRun?.passed).length
            return (
              <p className="text-sm text-slate-600">
                {passed} of {ran.length} passed
              </p>
            )
          })()}
        </div>
        <div className="flex items-center gap-2">
          <CatalogSelect />
          {runnable.length > 0 && (
            <button
              type="button"
              className="btn-ghost py-1.5 px-3 text-sm"
              disabled={running !== null}
              onClick={() => void runAll()}
            >
              {running === 'all' ? 'Running…' : 'Run all'}
            </button>
          )}
          {!adding && (
            <button
              type="button"
              className="btn-primary py-1.5 px-3 text-sm"
              onClick={() => {
                setFormError(null)
                setAdding(true)
              }}
            >
              Add eval
            </button>
          )}
        </div>
      </header>

      {adding && (
        <form className="card space-y-4 p-5" onSubmit={submit}>
          <div className="space-y-1.5">
            <label htmlFor="eval-question" className="label">
              Question
            </label>
            <input
              id="eval-question"
              className="input w-full"
              maxLength={500}
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              required
            />
          </div>

          <div className="space-y-1.5">
            <label htmlFor="eval-owner" className="label">
              Owner group
            </label>
            <input
              id="eval-owner"
              className="input w-full"
              list="eval-groups"
              value={ownerGroup}
              onChange={(e) => setOwnerGroup(e.target.value)}
              required
            />
            <datalist id="eval-groups">
              {groups.map((g) => (
                <option key={g} value={g} />
              ))}
            </datalist>
          </div>

          <div className="space-y-1.5">
            <label htmlFor="eval-expected" className="label">
              Expected answer
            </label>
            <textarea
              id="eval-expected"
              className="input w-full font-mono text-sm"
              rows={4}
              value={expectedText}
              onChange={(e) => setExpectedText(e.target.value)}
            />
            <p className="text-xs text-text-mute">{EXPECTED_HINT}</p>
          </div>

          {formError != null && <ErrorBox error={formError} />}

          <div className="flex items-center gap-2">
            <button type="submit" className="btn-primary py-1.5 px-3 text-sm" disabled={saving}>
              {saving ? 'Saving…' : 'Add eval'}
            </button>
            <button
              type="button"
              className="btn-ghost py-1.5 px-3 text-sm"
              onClick={() => {
                setFormError(null)
                setAdding(false)
              }}
            >
              Cancel
            </button>
          </div>
        </form>
      )}

      {removeError != null && <ErrorBox error={removeError} />}
      {runError != null && <ErrorBox error={runError} />}

      {list.isLoading && !list.data && <p className="text-sm text-text-mute">Loading…</p>}
      {list.isError && <ErrorBox error={list.error} onRetry={() => void list.refetch()} />}
      {list.data && rows.length === 0 && (
        <p className="text-sm text-text-dim">
          No evals yet. Add the questions this catalog must always answer correctly.
        </p>
      )}

      {rows.length > 0 && (
        <section className="card min-w-0 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-bg-soft/50">
                <tr className="text-left text-xs text-text-mute">
                  <th scope="col" className="th px-5 py-2.5">Question</th>
                  <th scope="col" className="th px-3 py-2.5">Owner group</th>
                  <th scope="col" className="th px-3 py-2.5">Expected answer</th>
                  <th scope="col" className="th px-3 py-2.5">Last run</th>
                  <th scope="col" className="th px-3 py-2.5">Written by</th>
                  <th scope="col" className="th px-3 py-2.5">Written</th>
                  <th scope="col" className="th px-3 py-2.5">
                    <span className="sr-only">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={row.id} className="border-t border-border align-top">
                    <td className="td px-5 py-2.5 break-words">{row.question}</td>
                    <td className="td px-3 py-2.5 break-words">{row.ownerGroup}</td>
                    <td className="td px-3 py-2.5 break-all font-mono text-xs">
                      {describe(row.expected)}
                    </td>
                    <td className="td px-3 py-2.5">
                      {row.lastRun === null ? (
                        <span className="text-slate-500">not run yet</span>
                      ) : row.lastRun.passed ? (
                        <span className="text-green-700">passed</span>
                      ) : (
                        <span className="text-red-700">
                          failed — {row.lastRun.reason ?? 'no reason'}
                        </span>
                      )}
                    </td>
                    <td className="td px-3 py-2.5 break-all text-text-dim">{row.createdBy}</td>
                    <td
                      className="td px-3 py-2.5 whitespace-nowrap text-text-dim"
                      title={formatTime(row.createdAt)}
                    >
                      {relativeTime(Date.parse(row.createdAt))}
                    </td>
                    <td className="td px-3 py-2.5 text-right">
                      {mayRun(row) && (
                        <button
                          type="button"
                          className="btn-ghost mr-1 py-1 px-2.5 text-xs"
                          aria-label={`Run the eval: ${row.question}`}
                          disabled={running !== null}
                          onClick={() => void run(row)}
                        >
                          {running === row.id ? 'Running…' : 'Run'}
                        </button>
                      )}
                      <button
                        type="button"
                        className="btn-ghost py-1 px-2.5 text-xs"
                        disabled={removing === row.id}
                        onClick={() => void remove(row.id)}
                      >
                        Remove
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  )
}
