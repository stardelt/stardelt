import { useState, type FormEvent } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '../api'
import { formatTime, relativeTime } from '../format'
import ErrorBox from './ErrorBox'

/** Most characters a question may carry; the API's own limit (#1018). */
const MAX_QUESTION = 500
/** Most characters the SQL may carry; the API's own limit (#1018). */
const MAX_SQL = 20000
/** Most characters of a failed check's message shown inline; the rest sits in `title`. */
const MAX_CHECK_MESSAGE = 160

/** The instant a check ran as milliseconds, or `null` when it never ran. */
function checkedAt(item: { lastCheckAt?: string | null }): number | null {
  if (!item.lastCheckAt) return null
  const ms = Date.parse(item.lastCheckAt)
  return Number.isNaN(ms) ? null : ms
}

/**
 * "Verified queries": the questions a table's owners answered with SQL, read by anyone who
 * may see the table and added or removed by whoever may edit it (#911 slice 2b).
 *
 * The list is a query of its own, so a failed load leaves the rest of the About card
 * working: while it is loading or failed the section renders nothing at all. Nothing here
 * runs the SQL or renders Markdown — a verified query is text plus the statement.
 *
 * Each query shows the outcome of its last check, and anyone who may see the list may run it
 * again: the check reads only and runs as the caller (#912). With two or more queries a
 * `Check all` runs them in one call and says how many passed (#912 slice 1b). The statement
 * opens in the SQL editor, which is what a reader wants next.
 */
export default function VerifiedQueries({
  ns,
  tbl,
  catalog,
  canEdit,
}: {
  ns: string
  tbl: string
  /** The catalog the table was browsed in; a write must land in the same one. */
  catalog?: string
  /** The documentation's `canEdit`: an administrator or the table's owner group. */
  canEdit: boolean
}) {
  const qc = useQueryClient()
  const queryKey = ['verified-queries', catalog, ns, tbl]
  const list = useQuery({
    queryKey,
    queryFn: () => api.verifiedQueries(ns, tbl, catalog),
    retry: false,
  })

  const [question, setQuestion] = useState('')
  const [sql, setSql] = useState('')
  const [formOpen, setFormOpen] = useState(false)
  const [adding, setAdding] = useState(false)
  const [addError, setAddError] = useState<unknown>(null)
  const [deleting, setDeleting] = useState<number | null>(null)
  const [deleteError, setDeleteError] = useState<unknown>(null)
  const [checking, setChecking] = useState<number | null>(null)
  const [checkError, setCheckError] = useState<unknown>(null)
  const [checkingAll, setCheckingAll] = useState(false)
  /** The totals of the last "Check all", or `null` before there was one. */
  const [bulk, setBulk] = useState<{ checked: number; passed: number; failed: number } | null>(null)

  const items = list.data?.items ?? []

  // A failed or unfinished list is not the card's business: render nothing rather than an
  // error, so a backend that does not answer this path leaves every screen as it was.
  if (list.isLoading || list.isError) return null
  if (items.length === 0 && !canEdit) return null

  function closeForm() {
    setQuestion('')
    setSql('')
    setAddError(null)
    setFormOpen(false)
  }

  async function add(event: FormEvent) {
    event.preventDefault()
    const nextQuestion = question.trim()
    const nextSql = sql.trim()
    if (nextQuestion === '' || nextSql === '' || adding) return
    setAdding(true)
    setAddError(null)
    try {
      await api.createVerifiedQuery(ns, tbl, { question: nextQuestion, sql: nextSql }, catalog)
      setQuestion('')
      setSql('')
      setFormOpen(false)
      await qc.invalidateQueries({ queryKey })
    } catch (error) {
      setAddError(error)
    } finally {
      setAdding(false)
    }
  }

  async function remove(id: number) {
    if (deleting !== null) return
    setDeleting(id)
    setDeleteError(null)
    try {
      await api.deleteVerifiedQuery(id)
      await qc.invalidateQueries({ queryKey })
    } catch (error) {
      setDeleteError(error)
    } finally {
      setDeleting(null)
    }
  }

  async function check(id: number) {
    if (checking !== null) return
    setChecking(id)
    setCheckError(null)
    try {
      await api.checkVerifiedQuery(id)
      await qc.invalidateQueries({ queryKey })
    } catch (error) {
      setCheckError(error)
    } finally {
      setChecking(null)
    }
  }

  async function checkAll() {
    if (checkingAll) return
    setCheckingAll(true)
    setCheckError(null)
    try {
      const answer = await api.checkAllVerifiedQueries(ns, tbl, catalog)
      setBulk({ checked: answer.checked, passed: answer.passed, failed: answer.failed })
      await qc.invalidateQueries({ queryKey })
    } catch (error) {
      setCheckError(error)
    } finally {
      setCheckingAll(false)
    }
  }

  return (
    <section className="mt-4" aria-label="Verified queries">
      <div className="mb-1 flex flex-wrap items-center gap-x-2 gap-y-1">
        <span className="label">Verified queries</span>
        {items.length >= 2 && (
          <button
            type="button"
            className="btn-ghost py-0.5 px-2 text-xs"
            disabled={checkingAll}
            onClick={() => void checkAll()}
          >
            {checkingAll ? 'Checking all…' : 'Check all'}
          </button>
        )}
        {bulk !== null && (
          <span className={bulk.failed === 0 ? 'text-xs text-ok' : 'text-xs text-bad'}>
            {`${bulk.passed} of ${bulk.checked} passed`}
            {bulk.failed === 0 ? '' : ` · ${bulk.failed} failed`}
          </span>
        )}
      </div>

      {items.length > 0 ? (
        <ul className="space-y-3">
          {items.map((item) => {
            const at = checkedAt(item)
            const message = item.lastCheckMessage ?? ''
            return (
              <li key={item.id} className="min-w-0">
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <p className="min-w-0 text-sm text-text whitespace-normal break-words">{item.question}</p>
                  {canEdit && (
                    <button
                      type="button"
                      className="btn-ghost py-1 px-2.5 text-xs"
                      aria-label={`Delete the verified query: ${item.question}`}
                      disabled={deleting !== null}
                      onClick={() => void remove(item.id)}
                    >
                      Delete
                    </button>
                  )}
                </div>
                <pre className="mt-1.5 max-h-40 overflow-auto rounded-[8px] bg-bg-soft px-2.5 py-2 font-mono text-xs text-text-dim">
                  {item.sql}
                </pre>
                <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-text-mute">
                  <span>
                    Verified by {item.verifiedBy} · {formatTime(item.verifiedAt)}
                  </span>
                  {item.lastCheckPassed === true && at !== null && (
                    <span className="text-ok">✓ Checked {relativeTime(at)}</span>
                  )}
                  {item.lastCheckPassed === false && (
                    <span className="text-bad" title={message}>
                      ✕ {message.slice(0, MAX_CHECK_MESSAGE)}
                    </span>
                  )}
                  {at === null && <span>Not checked yet</span>}
                  <button
                    type="button"
                    className="btn-ghost py-0.5 px-2 text-xs"
                    disabled={checking !== null}
                    onClick={() => void check(item.id)}
                  >
                    {checking === item.id ? 'Checking…' : 'Check now'}
                  </button>
                  <Link className="btn-ghost py-0.5 px-2 text-xs" to={`/query?sql=${encodeURIComponent(item.sql)}`}>
                    Open in SQL editor
                  </Link>
                </div>
              </li>
            )
          })}
        </ul>
      ) : (
        <p className="text-sm text-text-mute">
          No verified queries yet. Add the question the table answers, and the SQL that answers it.
        </p>
      )}

      {checkError !== null && (
        <div className="mt-2">
          <ErrorBox error={checkError} />
        </div>
      )}

      {deleteError !== null && (
        <div className="mt-2">
          <ErrorBox error={deleteError} />
        </div>
      )}

      {canEdit && !formOpen && (
        <button type="button" className="btn-ghost mt-3 py-1 px-2.5 text-xs" onClick={() => setFormOpen(true)}>
          + Add a verified query
        </button>
      )}

      {canEdit && formOpen && (
        <form className="mt-3 space-y-2" onSubmit={add}>
          <input
            className="input"
            maxLength={MAX_QUESTION}
            value={question}
            onChange={(event) => setQuestion(event.target.value)}
            placeholder="What did revenue last month come to?"
            aria-label="Question"
          />
          <textarea
            className="input min-h-[6rem] resize-y font-mono text-xs"
            maxLength={MAX_SQL}
            value={sql}
            onChange={(event) => setSql(event.target.value)}
            placeholder="SELECT sum(revenue) FROM … WHERE …"
            aria-label="SQL"
          />
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="submit"
              className="btn-primary"
              disabled={adding || question.trim() === '' || sql.trim() === ''}
            >
              {adding ? 'Adding…' : 'Add verified query'}
            </button>
            <button type="button" className="btn-ghost" onClick={closeForm}>
              Cancel
            </button>
          </div>
          {addError !== null && <ErrorBox error={addError} />}
        </form>
      )}
    </section>
  )
}
