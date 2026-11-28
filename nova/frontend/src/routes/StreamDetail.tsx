import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useId, useRef, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api, ApiError, errorMessage, type StreamView } from '../api'
import { EmptyState } from '../components/EmptyState'
import ErrorBox from '../components/ErrorBox'
import { formatWhen } from './Pipelines'

const NEUTRAL = 'border-border bg-bg-soft text-text-dim'
const GOOD = 'border-emerald-400/40 bg-emerald-400/10 text-ok'
const BAD = 'border-red-400/40 bg-red-400/10 text-bad'
const BUSY = 'border-sky-400/40 bg-sky-400/10 text-info'

type PhaseLook = { word: string; cls: string }

/** The phase words of `StreamView.phase`; the word is shown as-is for unknown values. */
const PHASES: Record<string, PhaseLook> = {
  Running: { word: 'Running', cls: GOOD },
  Stopped: { word: 'Stopped', cls: NEUTRAL },
  Failed: { word: 'Failed', cls: BAD },
  Pending: { word: 'Pending', cls: BUSY },
}

/** The operator's own window: `Restarting` lasts ten minutes after a submission. */
const RESTART_WINDOW_MS = 10 * 60 * 1000

/** True when the last submission is inside the operator's restart window. */
function restartedRecently(iso: string | null): boolean {
  if (!iso) return false
  const then = Date.parse(iso)
  return !Number.isNaN(then) && Date.now() - then < RESTART_WINDOW_MS
}

function PhaseBadge({ phase, reason }: { phase: string; reason: string | null }) {
  const look = PHASES[phase] ?? { word: phase || 'Unknown', cls: NEUTRAL }
  return (
    <span className="inline-flex items-center gap-1">
      <span className={`badge ${look.cls}`}>{look.word}</span>
      {reason && reason !== phase && <span className={`badge ${BUSY}`}>{reason}</span>}
    </span>
  )
}

function Fact({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="label">{label}</dt>
      <dd className="mt-0.5 break-words">{children}</dd>
    </div>
  )
}

/** A native `<dialog>`, like `DeleteDestinationDialog`: the browser holds the focus and Escape cancels. */
function DeleteStreamDialog({ stream, onClose, onDeleted }: {
  stream: StreamView
  onClose: () => void
  onDeleted: () => void
}) {
  const titleId = useId()
  const dialog = useRef<HTMLDialogElement>(null)
  const [deleting, setDeleting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const element = dialog.current
    if (!element) return
    if (!element.open) element.showModal()
    return () => {
      if (element.open) element.close()
    }
  }, [])

  async function remove() {
    setDeleting(true)
    setError(null)
    try {
      await api.deleteStream(stream.namespace, stream.name)
      onDeleted()
    } catch (e) {
      setError(errorMessage(e))
    } finally {
      setDeleting(false)
    }
  }

  return (
    <dialog
      ref={dialog}
      aria-labelledby={titleId}
      onCancel={(event) => { event.preventDefault(); onClose() }}
      className="m-auto w-[min(30rem,calc(100vw_-_2rem))] max-w-none rounded-[14px] border border-border bg-bg-elev p-0 text-text shadow-2xl backdrop:bg-black/60"
    >
      <div className="space-y-3 p-5">
        <h2 id={titleId} className="text-lg font-semibold break-words">Delete “{stream.name}”?</h2>
        <p className="text-sm text-text-dim">
          {stream.deletion_policy === 'Delete'
            ? "Nova deletes the stream. It reads the platform's own bus, so its Kafka topic and every message in it are deleted too; a topic read through a connection stays."
            : "Nova deletes the stream. Its Kafka topic and every message in it stay; the topic keeps being managed. A topic read through a connection always stays."}
        </p>
        {error && <p role="alert" className="text-sm text-bad break-words">{error}</p>}
        <div className="flex items-center justify-end gap-2">
          <button type="button" className="btn-ghost" onClick={onClose}>Cancel</button>
          <button type="button" className="btn-primary" disabled={deleting} onClick={() => void remove()}>
            {deleting ? 'Deleting…' : 'Delete'}
          </button>
        </div>
      </div>
    </dialog>
  )
}

/**
 * The sink table's newest snapshot as the caller (#22 point 3): the row count Iceberg
 * recorded and when it committed. `GET /api/catalog/tables/{ns}/{tbl}/history` answers the
 * last 100 snapshots newest first; the first row is the current state. A missing table is
 * said as such, not as a read failure.
 */
function SinkFacts({ stream }: { stream: StreamView }) {
  const [schema, table] = stream.sink_table.split('.')
  const history = useQuery({
    queryKey: ['stream-sink', stream.sink_catalog, stream.sink_table],
    queryFn: () => api.tableHistory(schema, table, stream.sink_catalog),
    refetchInterval: 15_000,
    retry: false,
  })
  const columns = history.data?.columns ?? []
  const newest = (history.data?.rows ?? [])[0]
  const at = (name: string) =>
    columns.findIndex((column) => column.name === name)
  const iTime = at('committed_at')
  const iSummary = at('summary')
  const summary =
    iSummary >= 0 && newest?.[iSummary] && typeof newest[iSummary] === 'object'
      ? (newest[iSummary] as Record<string, unknown>)
      : undefined
  const rows = summary?.['total-records']
  const committed =
    iTime >= 0 && newest?.[iTime] != null
      ? String(newest[iTime]).replace(' ', 'T').replace(' UTC', 'Z')
      : null
  const when = committed ? formatWhen(committed) : { text: '—' }
  const missing = history.isError && (history.error as ApiError).status === 404
  return (
    <>
      <Fact label="Sink rows">
        {history.isPending ? <span>…</span> : <span>{rows == null ? '—' : String(rows)}</span>}
      </Fact>
      <Fact label="Last commit">
        {newest ? <span title={when.title ?? committed ?? undefined}>{when.text}</span> : <span>—</span>}
        {missing && (
          <div className="text-xs text-text-dim">The sink table does not exist.</div>
        )}
        {history.isError && !missing && (
          <div className="text-xs text-text-dim">The sink table could not be read.</div>
        )}
      </Fact>
    </>
  )
}

/**
 * One stream (D42): what it reads, where it writes, who may see it and whether it runs, plus the
 * same Stop/Resume the list offers. The row comes from `GET /api/streams`, read again every 15 s.
 * A stream the caller cannot see is not in the list at all, so its page says it does not exist.
 */
export default function StreamDetail() {
  const { namespace = '', name = '' } = useParams()
  const qc = useQueryClient()
  const navigate = useNavigate()
  const [deleting, setDeleting] = useState(false)
  const streams = useQuery({ queryKey: ['streams'], queryFn: api.streams, refetchInterval: 15_000 })
  const mutation = useMutation({
    mutationFn: ({ s, stopped }: { s: StreamView; stopped: boolean }) =>
      api.setStreamState(s.namespace, s.name, stopped),
    onMutate: async ({ s, stopped }) => {
      await qc.cancelQueries({ queryKey: ['streams'] })
      const previous = qc.getQueryData<{ streams: StreamView[] }>(['streams'])
      qc.setQueryData<{ streams: StreamView[] }>(['streams'], (old) =>
        old
          ? {
              ...old,
              streams: old.streams.map((row) =>
                row.namespace === s.namespace && row.name === s.name
                  ? { ...row, stopped, phase: stopped ? 'Stopped' : 'Running' }
                  : row,
              ),
            }
          : old,
      )
      return { previous }
    },
    onError: (_error, _vars, context) => {
      if (context?.previous) qc.setQueryData(['streams'], context.previous)
    },
    onSettled: () => qc.invalidateQueries({ queryKey: ['streams'] }),
  })

  if (streams.isPending) return null
  if (streams.isError) return <ErrorBox error={streams.error} onRetry={() => streams.refetch()} />

  const stream = streams.data?.streams.find((s) => s.namespace === namespace && s.name === name)

  if (!stream) {
    return (
      <div className="flex flex-col gap-4">
        <EmptyState icon="clock" title="Stream not found">
          This stream does not exist, or it belongs to a group you are not in.
        </EmptyState>
        <Link to="/streams" className="btn-ghost w-fit">Back to streams</Link>
      </div>
    )
  }

  const submitted = stream.last_submitted_at ? formatWhen(stream.last_submitted_at) : null

  return (
    <div className="max-w-4xl space-y-5">
      <header className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="mb-1 text-xs">
            <Link to="/streams" className="underline text-text-dim">Back to streams</Link>
          </div>
          <h1 className="text-2xl font-bold tracking-tight break-words font-mono">{stream.name}</h1>
          <p className="text-text-dim text-sm mt-1">
            Owner group <span className="break-words">{stream.owner_group}</span>
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <PhaseBadge phase={stream.phase} reason={stream.reason} />
          {typeof stream.attempts === 'number' && stream.attempts > 1 && (
            <span
              className={`badge ${restartedRecently(stream.last_submitted_at) ? BUSY : NEUTRAL}`}
              title={
                stream.last_submitted_at
                  ? `last submitted ${formatWhen(stream.last_submitted_at).text}`
                  : `${stream.attempts} submissions since the stream was created`
              }
            >
              {stream.attempts}× {restartedRecently(stream.last_submitted_at) ? 'restarted' : 'submissions'}
            </span>
          )}
          <button
            type="button"
            aria-label={`${stream.stopped ? 'Resume' : 'Stop'} ${stream.name}`}
            disabled={mutation.isPending}
            onClick={() => mutation.mutate({ s: stream, stopped: !stream.stopped })}
            className="btn-ghost py-1.5 text-sm"
          >
            {stream.stopped ? 'Resume' : 'Stop'}
          </button>
          <button
            type="button"
            aria-label={`Delete ${stream.name}`}
            className="btn-ghost py-1.5 text-sm text-bad"
            onClick={() => setDeleting(true)}
          >
            Delete
          </button>
        </div>
      </header>

      {mutation.isError && (
        <p role="alert" className="text-sm text-bad">
          {errorMessage(mutation.error)}
        </p>
      )}

      <dl className="grid grid-cols-1 gap-x-6 gap-y-3 text-sm sm:grid-cols-2 lg:grid-cols-3">
        <Fact label="Source">
          <span className="font-mono text-xs">
            {stream.source_connection} · {stream.source_topic}
          </span>
        </Fact>
        <Fact label="Sink">
          <div className="font-mono text-xs">
            {stream.sink_catalog}.{stream.sink_table}
          </div>
          <div className="text-xs text-text-dim">{stream.sink_mode}</div>
        </Fact>
        <SinkFacts stream={stream} />
        <Fact label="Runs as">
          <span className="font-mono text-xs">{stream.run_as ?? '—'}</span>
        </Fact>
        <Fact label="Viewers">
          <span>{stream.viewers.join(', ') || '—'}</span>
        </Fact>
        <Fact label="Attempts">
          <span>{stream.attempts ?? '—'}</span>
        </Fact>
        <Fact label="Last submitted">
          {submitted ? <span title={submitted.title}>{submitted.text}</span> : <span>—</span>}
        </Fact>
        <Fact label="Last error">
          <span className="text-bad">{stream.last_error ?? '—'}</span>
        </Fact>
      </dl>

      {deleting && (
        <DeleteStreamDialog
          stream={stream}
          onClose={() => setDeleting(false)}
          onDeleted={() => {
            setDeleting(false)
            void qc.invalidateQueries({ queryKey: ['streams'] })
            navigate('/streams')
          }}
        />
      )}
    </div>
  )
}
