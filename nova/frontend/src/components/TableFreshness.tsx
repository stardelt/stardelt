import { useEffect, useState, type FormEvent } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, errorMessage, type FreshnessUpdate, type TableFreshness as Freshness } from '../api'
import { formatTime, relativeTime } from '../format'

/**
 * "Freshness": the promise the table's owner made — a wall-clock deadline in a zone plus a
 * grace period — and the status it yields right now (#402 slice 2a).
 *
 * This card is the **promise and status**. The maintenance numbers (files, small files,
 * snapshots) are `TableHealth`; that is a different card. The badge and the sentence under
 * it are the API's own words (`detail`), so the page and the route never disagree.
 */
const BADGE: Record<Freshness['status'], { word: string; ink: string }> = {
  grey: { word: 'No promise', ink: 'border-border bg-bg-soft/60 text-text-mute' },
  green: { word: 'Fresh', ink: 'border-ok/40 bg-ok/10 text-ok' },
  amber: { word: 'Late (grace)', ink: 'border-warn/40 bg-warn/10 text-warn' },
  // `text-danger` is not a class in this theme; the series' fallback is the warning ink.
  red: { word: 'Late', ink: 'border-warn/40 bg-warn/10 text-warn' },
}

export default function TableFreshness({
  ns,
  tbl,
  catalog,
}: {
  ns: string
  tbl: string
  /** The catalog the table was browsed in; a write must land in the same one. */
  catalog?: string
}) {
  const qc = useQueryClient()
  const queryKey = ['table-freshness', catalog, ns, tbl]
  const read = useQuery({
    queryKey,
    queryFn: () => api.tableFreshness(ns, tbl, catalog),
    retry: false,
  })

  const promise = read.data?.promise ?? null
  const [by, setBy] = useState('07:00')
  const [timezone, setTimezone] = useState('Europe/Berlin')
  const [graceMinutes, setGraceMinutes] = useState('60')

  // The promise a person sees seeds the form: the first read, and any answer to a write.
  // A plain refetch of the same promise does not undo what is being typed.
  const seedKey = promise ? `${promise.by}|${promise.timezone}|${promise.graceMinutes}` : ''
  useEffect(() => {
    if (!promise) return
    setBy(promise.by)
    setTimezone(promise.timezone)
    setGraceMinutes(String(promise.graceMinutes))
  }, [seedKey, promise])

  const write = useMutation({
    mutationFn: (body: FreshnessUpdate) => api.putTableFreshness(ns, tbl, body, catalog),
    // The answer is the new state: the badge flips without a second read.
    onSuccess: (answer) => qc.setQueryData(queryKey, answer),
  })
  const pending = write.isPending

  const onSet = (event: FormEvent) => {
    event.preventDefault()
    write.mutate({ by, timezone, graceMinutes: Number(graceMinutes) })
  }
  const onRemove = () => write.mutate({ by: null })

  // A failed read hides the card, like the maintenance card: a table whose engine does not
  // answer shows no red notice next to a page that otherwise works.
  if (read.isError) return null

  const badge = BADGE[read.data?.status ?? 'grey']
  const lastWriteMs = read.data?.lastWrite ? Date.parse(read.data.lastWrite) : NaN
  const canEdit = read.data?.canEdit === true

  return (
    <section className="card p-3" aria-labelledby="table-freshness-title">
      <h2 id="table-freshness-title" className="label">Freshness</h2>
      {read.isLoading ? (
        <p className="mt-2 text-sm text-text-mute">loading…</p>
      ) : (
        <>
          <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1">
            <span className={`badge ${badge.ink}`}>{badge.word}</span>
            {read.data && <span className="text-sm text-text-dim break-words">{read.data.detail}</span>}
          </div>
          {promise && (
            <dl className="mt-2 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-sm">
              <dt className="text-text-mute">Promise</dt>
              <dd className="text-text-dim break-words">
                <span className="font-mono">{promise.by}</span> {promise.timezone} · grace{' '}
                {promise.graceMinutes} min
              </dd>
              <dt className="text-text-mute">Due</dt>
              <dd className="text-text-dim">{read.data?.dueAt ? formatTime(read.data.dueAt) : '—'}</dd>
              <dt className="text-text-mute">Next due</dt>
              <dd className="text-text-dim">
                {read.data?.nextDueAt ? formatTime(read.data.nextDueAt) : '—'}
              </dd>
            </dl>
          )}
          <p className="mt-2 text-sm text-text-mute">
            Last write {Number.isNaN(lastWriteMs) ? '—' : relativeTime(lastWriteMs)}
          </p>
          {canEdit && (
            <form className="mt-3 flex flex-wrap items-end gap-2" onSubmit={onSet}>
              <label className="flex flex-col gap-1 text-xs text-text-mute">
                Deadline
                <input
                  type="time"
                  className="input"
                  value={by}
                  onChange={(e) => setBy(e.target.value)}
                  required
                />
              </label>
              <label className="flex flex-col gap-1 text-xs text-text-mute">
                Timezone
                <input
                  type="text"
                  className="input"
                  value={timezone}
                  onChange={(e) => setTimezone(e.target.value)}
                  placeholder="Europe/Berlin"
                  required
                />
              </label>
              <label className="flex flex-col gap-1 text-xs text-text-mute">
                Grace (minutes)
                <input
                  type="number"
                  className="input w-24"
                  min={0}
                  max={1440}
                  value={graceMinutes}
                  onChange={(e) => setGraceMinutes(e.target.value)}
                />
              </label>
              <button type="submit" className="btn-primary py-1.5" disabled={pending}>
                Set
              </button>
              <button type="button" className="btn-ghost py-1.5" disabled={pending} onClick={onRemove}>
                Remove
              </button>
            </form>
          )}
          {write.isError && (
            <p role="alert" className="mt-2 text-sm text-bad break-words">
              {errorMessage(write.error)}
            </p>
          )}
        </>
      )}
    </section>
  )
}
