import { useState, type FormEvent } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '../api'
import { formatTime } from '../format'
import ErrorBox from './ErrorBox'

/** The API's name rule for a metric: lower-case, digits and underscore, a letter first. */
const NAME_RE = /^[a-z][a-z0-9_]*$/

/** `country, channel` → `['country', 'channel']`; empty entries drop, so an empty field is `[]`. */
function splitDimensions(raw: string): string[] {
  return raw
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part !== '')
}

/**
 * "Metrics": the named numbers a table's owners declared over it (#124), read by anyone
 * who may see the table and added or removed by whoever may edit it.
 *
 * The list is a query of its own, so a failed load leaves the rest of the About card
 * working: while it is loading or failed the section renders nothing at all. The
 * expression is shown as text — a metric is a definition, never run from here.
 */
export default function MetricDefinitions({
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
  const queryKey = ['metric-definitions', catalog, ns, tbl]
  const list = useQuery({
    queryKey,
    queryFn: () => api.metricDefinitions(ns, tbl, catalog),
    retry: false,
  })

  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [expression, setExpression] = useState('')
  const [timeColumn, setTimeColumn] = useState('')
  const [dimensions, setDimensions] = useState('')
  const [formOpen, setFormOpen] = useState(false)
  const [adding, setAdding] = useState(false)
  const [addError, setAddError] = useState<unknown>(null)
  const [confirming, setConfirming] = useState<number | null>(null)
  const [deleting, setDeleting] = useState<number | null>(null)
  const [deleteError, setDeleteError] = useState<unknown>(null)

  // A body that is not a list (an older backend, or a route that answers something else)
  // is no list: leave the card as it was.
  const items = Array.isArray(list.data) ? list.data : []

  // A failed or unfinished list is not the card's business: render nothing rather than an
  // error, so a backend that predates #2058 leaves every screen as it was.
  if (list.isLoading || list.isError) return null
  if (items.length === 0 && !canEdit) return null

  function closeForm() {
    setName('')
    setDescription('')
    setExpression('')
    setTimeColumn('')
    setDimensions('')
    setAddError(null)
    setFormOpen(false)
  }

  async function add(event: FormEvent) {
    event.preventDefault()
    const nextName = name.trim()
    const nextExpression = expression.trim()
    if (!NAME_RE.test(nextName) || nextExpression === '' || adding) return
    setAdding(true)
    setAddError(null)
    try {
      await api.createMetricDefinition(
        ns,
        tbl,
        {
          name: nextName,
          description: description.trim(),
          expression: nextExpression,
          timeColumn: timeColumn.trim() === '' ? null : timeColumn.trim(),
          dimensions: splitDimensions(dimensions),
        },
        catalog,
      )
      setName('')
      setDescription('')
      setExpression('')
      setTimeColumn('')
      setDimensions('')
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
      await api.deleteMetricDefinition(id)
      setConfirming(null)
      await qc.invalidateQueries({ queryKey })
    } catch (error) {
      setDeleteError(error)
    } finally {
      setDeleting(null)
    }
  }

  return (
    <section className="mt-4" aria-label="Metrics">
      <h3 className="label mb-1">Metrics</h3>
      <p className="mb-2 text-xs text-text-mute">
        A named number over this table: its expression, the time column it is counted over and the columns it
        can be grouped by.
      </p>

      {items.length > 0 && (
        <ul className="space-y-3">
          {items.map((item) => (
            <li key={item.id} className="min-w-0">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <p className="min-w-0 text-sm font-medium text-text whitespace-normal break-words">{item.name}</p>
                {canEdit && (
                  <button
                    type="button"
                    className="btn-ghost py-1 px-2.5 text-xs"
                    aria-label={`Delete the metric ${item.name}`}
                    disabled={deleting !== null}
                    onClick={() => {
                      if (confirming === item.id) void remove(item.id)
                      else setConfirming(item.id)
                    }}
                  >
                    {confirming === item.id ? 'Confirm delete' : 'Delete'}
                  </button>
                )}
              </div>
              {item.description !== '' && (
                <p className="text-sm text-text-dim whitespace-normal break-words">{item.description}</p>
              )}
              <code className="mt-1.5 block overflow-x-auto rounded bg-bg-soft px-2 py-1 text-xs">
                {item.expression}
              </code>
              <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-text-mute">
                {item.timeColumn !== null && item.timeColumn !== '' && (
                  <span>
                    Counted over <code>{item.timeColumn}</code>
                  </span>
                )}
                {item.dimensions.map((dimension) => (
                  <span key={dimension} className="badge border-accent/40 bg-accent/10 text-accent">
                    {dimension}
                  </span>
                ))}
                <span>
                  Verified by {item.verifiedBy} · {formatTime(item.verifiedAt)}
                </span>
              </div>
            </li>
          ))}
        </ul>
      )}

      {items.length === 0 && canEdit && (
        <p className="text-sm text-text-mute">
          No metrics yet. A metric is read by people and by agents, so say what the number means.
        </p>
      )}

      {deleteError !== null && (
        <div className="mt-2">
          <ErrorBox error={deleteError} />
        </div>
      )}

      {canEdit && !formOpen && (
        <button type="button" className="btn-ghost mt-3 py-1 px-2.5 text-xs" onClick={() => setFormOpen(true)}>
          + Add a metric
        </button>
      )}

      {canEdit && formOpen && (
        <form className="mt-3 space-y-2" onSubmit={add}>
          <input
            className="input"
            value={name}
            pattern="[a-z][a-z0-9_]*"
            onChange={(event) => setName(event.target.value)}
            placeholder="revenue"
            aria-label="Name"
          />
          <input
            className="input"
            value={description}
            onChange={(event) => setDescription(event.target.value)}
            placeholder="Revenue in dollars, net of returns"
            aria-label="Description"
          />
          <textarea
            className="input resize-y font-mono text-xs"
            rows={2}
            value={expression}
            onChange={(event) => setExpression(event.target.value)}
            placeholder="sum(total_cents) / 100.0"
            aria-label="Expression"
          />
          <input
            className="input"
            value={timeColumn}
            onChange={(event) => setTimeColumn(event.target.value)}
            placeholder="ordered_at (optional)"
            aria-label="Time column"
          />
          <input
            className="input"
            value={dimensions}
            onChange={(event) => setDimensions(event.target.value)}
            placeholder="country, channel"
            aria-label="Dimensions"
          />
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="submit"
              className="btn-primary"
              disabled={adding || !NAME_RE.test(name.trim()) || expression.trim() === ''}
            >
              {adding ? 'Adding…' : 'Add metric'}
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
