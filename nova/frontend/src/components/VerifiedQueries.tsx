import { useState, type FormEvent } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '../api'
import { formatTime } from '../format'
import ErrorBox from './ErrorBox'

/** Most characters a question may carry; the API's own limit (#1018). */
const MAX_QUESTION = 500
/** Most characters the SQL may carry; the API's own limit (#1018). */
const MAX_SQL = 20000

/**
 * "Verified queries": the questions a table's owners answered with SQL, read by anyone who
 * may see the table and added or removed by whoever may edit it (#911 slice 2b).
 *
 * The list is a query of its own, so a failed load leaves the rest of the About card
 * working: while it is loading or failed the section renders nothing at all. Nothing here
 * runs the SQL or renders Markdown — a verified query is text plus the statement.
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
  const [adding, setAdding] = useState(false)
  const [addError, setAddError] = useState<unknown>(null)
  const [deleting, setDeleting] = useState<number | null>(null)
  const [deleteError, setDeleteError] = useState<unknown>(null)

  const items = list.data?.items ?? []

  // A failed or unfinished list is not the card's business: render nothing rather than an
  // error, so a backend that does not answer this path leaves every screen as it was.
  if (list.isLoading || list.isError) return null
  if (items.length === 0 && !canEdit) return null

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

  return (
    <section className="mt-4" aria-label="Verified queries">
      <div className="label mb-1">Verified queries</div>

      {items.length > 0 ? (
        <ul className="space-y-3">
          {items.map((item) => (
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
              <pre className="mt-1.5 overflow-x-auto rounded-[8px] bg-bg-soft px-2.5 py-2 font-mono text-xs text-text-dim">
                {item.sql}
              </pre>
              <div className="mt-1 text-xs text-text-mute">
                Verified by {item.verifiedBy} · {formatTime(item.verifiedAt)}
              </div>
            </li>
          ))}
        </ul>
      ) : (
        <p className="text-sm text-text-mute">
          No verified queries yet. Add the question the table answers, and the SQL that answers it.
        </p>
      )}

      {deleteError !== null && (
        <div className="mt-2">
          <ErrorBox error={deleteError} />
        </div>
      )}

      {canEdit && (
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
            <button type="submit" className="btn" disabled={adding || question.trim() === '' || sql.trim() === ''}>
              {adding ? 'Adding…' : 'Add'}
            </button>
          </div>
          {addError !== null && <ErrorBox error={addError} />}
        </form>
      )}
    </section>
  )
}
