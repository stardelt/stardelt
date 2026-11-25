import { useMe } from '../hooks'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { api, errorMessage, type ComputeCluster } from '../api'
import ComputeDialog from '../components/ComputeDialog'
import { EmptyState } from '../components/EmptyState'
import ErrorBox from '../components/ErrorBox'
import SqlCompute from '../components/SqlCompute'
import { isAdmin } from '../roles'
import { formatWhen } from './Pipelines'

const NEUTRAL = 'border-border bg-bg-soft text-text-dim'
const GOOD = 'border-emerald-400/40 bg-emerald-400/10 text-ok'
const BAD = 'border-red-400/40 bg-red-400/10 text-bad'
const BUSY = 'border-sky-400/40 bg-sky-400/10 text-info'

type PhaseLook = { word: string; cls: string }

/** The phase words of `ComputeCluster.phase`; the word is shown as-is for unknown values. */
const PHASES: Record<string, PhaseLook> = {
  Starting: { word: 'Starting', cls: BUSY },
  Running: { word: 'Running', cls: GOOD },
  Stopping: { word: 'Stopping', cls: BUSY },
  Stopped: { word: 'Stopped', cls: NEUTRAL },
  Failed: { word: 'Failed', cls: BAD },
}

/** Who may use a cluster: its group, else its person. */
function audience(owner: ComputeCluster['owner']): string {
  return owner.group ? owner.group : owner.user ?? '—'
}

/** The demo sandbox's line: it is the visitors' server, local mode, restarted every night. */
const SANDBOX_COPY =
  'Demo sandbox · shared by all visitors · local mode · 2 cores · 4 GiB · resets nightly 03:00 UTC'

/** The shared server's line: always on, its executors scale to zero when idle. */
const SHARED_COPY = 'Shared · always on · scales executors to zero after 60 s idle'

/** A pod shape as the API reports it (`2 cores · 4Gi`), or `not reported`. */
function shape(spec?: { cores?: number | null; memory?: string | null } | null): string {
  const cores = spec?.cores != null ? `${spec.cores} core${spec.cores === 1 ? '' : 's'}` : null
  return [cores, spec?.memory ?? null].filter(Boolean).join(' · ') || 'not reported'
}

/** The executor line: `0–2 × 1 core / 1g`, or `local mode, no executors` on the sandbox. */
function executorLine(c: ComputeCluster): string {
  if (c.mode === 'local') return 'local mode, no executors'
  const min = c.sparkConf?.['spark.dynamicAllocation.minExecutors']
  const max = c.maxExecutors
  const count = min != null && max != null ? `${min}–${max}` : max != null ? `max ${max}` : null
  const size =
    [c.executor?.cores != null ? `${c.executor.cores} core${c.executor.cores === 1 ? '' : 's'}` : null, c.executor?.memory ?? null]
      .filter(Boolean)
      .join(' / ')
  if (count && size) return `${count} × ${size}`
  return [count, size].filter(Boolean).join(' · ') || 'not reported'
}

/** The card order: the caller's own Spark first, then the demo sandbox, then by name. */
function rank(c: ComputeCluster): number {
  return c.yours ? 0 : c.audience === 'visitors' ? 1 : 2
}

export default function Compute() {
  // The tab is the URL's `?tab=`: `/compute` still opens Spark, so every existing link works.
  const [params, setParams] = useSearchParams()
  const tab = params.get('tab') === 'sql' ? 'sql' : 'spark'
  const qc = useQueryClient()
  const me = useMe()
  const admin = isAdmin(me.data)
  const clusters = useQuery({ queryKey: ['compute'], queryFn: api.compute, refetchInterval: 15_000 })
  const [editing, setEditing] = useState<null | { cluster: ComputeCluster | null }>(null)
  const start = useMutation({
    mutationFn: (name: string) => api.startCompute(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['compute'] }),
  })
  const stop = useMutation({
    mutationFn: (name: string) => api.stopCompute(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['compute'] }),
  })
  const pending = start.isPending || stop.isPending
  const sorted = clusters.data
    ? [...clusters.data.clusters].sort((a, b) => rank(a) - rank(b) || a.name.localeCompare(b.name))
    : []

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Compute</h1>
          <p className="text-sm text-text-dim">
            {tab === 'sql'
              ? 'The SQL engine and the classes your queries run in.'
              : 'Spark clusters a team starts when it needs them; the shared cluster is always on.'}
          </p>
        </div>
        {tab === 'spark' && admin && (
          <button type="button" className="btn-primary" onClick={() => setEditing({ cluster: null })}>
            New cluster
          </button>
        )}
      </div>

      <div className="flex flex-wrap gap-2" role="tablist" aria-label="Compute kinds">
        {([
          ['spark', 'Spark'],
          ['sql', 'SQL'],
        ] as const).map(([key, label]) => (
          <button
            key={key}
            role="tab"
            onClick={() => setParams(key === 'spark' ? {} : { tab: 'sql' })}
            aria-selected={tab === key}
            className={[
              'px-3 py-1.5 rounded-[8px] text-sm border',
              tab === key
                ? 'bg-accent/15 border-accent/30 text-text'
                : 'border-border text-text-dim hover:text-text',
            ].join(' ')}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === 'sql' && <SqlCompute />}

      {tab === 'spark' && clusters.isError && <ErrorBox error={clusters.error} onRetry={() => clusters.refetch()} />}

      {tab === 'spark' && clusters.data && clusters.data.clusters.length === 0 && (
        <EmptyState icon="spark" title="No compute clusters">
          A cluster is created as a <span className="font-mono">SparkCluster</span> resource; ask an administrator to
          create one.
        </EmptyState>
      )}

      {tab === 'spark' && clusters.data && clusters.data.clusters.length > 0 && (
        <>
          {(start.isError || stop.isError) && (
            <p role="alert" className="text-sm text-bad">
              {errorMessage((start.error ?? stop.error) as unknown)}
            </p>
          )}
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            {sorted.map((c) => {
              const look = PHASES[c.phase] ?? { word: c.phase || 'Unknown', cls: NEUTRAL }
              const since = c.startedAt && c.phase === 'Running' ? formatWhen(c.startedAt).text : null
              // A `platform` server is installed by the platform's manifests: it has no Start,
              // Stop or Edit, because nothing here is a `SparkCluster`.
              const platform = c.kind === 'platform'
              return (
                <article key={c.name} className="flex flex-col gap-2 rounded-[10px] border border-border bg-bg-soft/40 p-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="font-medium break-words">{c.displayName ?? c.name}</span>
                    <span className={`badge ${look.cls}`}>{look.word}</span>
                    {c.yours && (
                      <span className={`badge ${GOOD}`}>
                        Your notebook&apos;s <span className="font-mono">spark</span>
                      </span>
                    )}
                  </div>
                  <div className="flex flex-wrap items-center gap-2 text-xs text-text-dim">
                    <span>{audience(c.owner)}</span>
                    <span className={`badge ${NEUTRAL}`}>{c.size}</span>
                    <span className="font-mono text-xs">{c.runtime}</span>
                  </div>
                  <dl className="grid grid-cols-[5rem_1fr] gap-x-2 gap-y-1 text-xs">
                    <dt className="text-text-dim">driver</dt>
                    <dd className="font-mono break-words">{shape(c.driver)}</dd>
                    <dt className="text-text-dim">executors</dt>
                    <dd className="font-mono break-words">{executorLine(c)}</dd>
                    {c.limits && (
                      <>
                        <dt className="text-text-dim">limit</dt>
                        <dd className="font-mono break-words">{shape(c.limits)}</dd>
                      </>
                    )}
                    <dt className="text-text-dim">image</dt>
                    <dd className="font-mono break-words">{c.runtime || '—'}</dd>
                    {c.driverServiceAccount && (
                      <>
                        <dt className="text-text-dim">runs as</dt>
                        <dd className="font-mono break-words">{c.driverServiceAccount}</dd>
                      </>
                    )}
                    {c.workloadIdentity && (
                      <>
                        <dt className="text-text-dim">catalog identity</dt>
                        <dd className="font-mono break-words" title={c.workloadIdentity}>{c.workloadIdentity}</dd>
                      </>
                    )}
                  </dl>
                  {c.shared ? (
                    <p className="text-sm text-text-dim">{SHARED_COPY}</p>
                  ) : platform ? (
                    c.audience === 'visitors' && <p className="text-sm text-text-dim">{SANDBOX_COPY}</p>
                  ) : (
                    <>
                      <p className="text-sm text-text-dim">
                        {since ? `Running since ${since} · ` : ''}Stops after {c.idleStopMinutes} min idle
                      </p>
                      {c.phase === 'Failed' && c.message && (
                        <p className="text-sm text-bad">{c.message}</p>
                      )}
                      <div className="flex gap-2">
                        {c.canAct ? (
                          <>
                            {(c.phase === 'Stopped' || c.phase === 'Failed') && (
                              <button
                                type="button"
                                className="btn-ghost px-2.5 py-1 text-xs"
                                disabled={pending}
                                onClick={() => start.mutate(c.name)}
                              >
                                Start
                              </button>
                            )}
                            {c.phase === 'Running' && (
                              <button
                                type="button"
                                className="btn-ghost px-2.5 py-1 text-xs"
                                disabled={pending}
                                onClick={() => stop.mutate(c.name)}
                              >
                                Stop
                              </button>
                            )}
                            {admin && (
                              <button
                                type="button"
                                className="btn-ghost px-2.5 py-1 text-xs"
                                onClick={() => setEditing({ cluster: c })}
                              >
                                Edit
                              </button>
                            )}
                          </>
                        ) : (
                          <p className="text-sm text-text-dim">
                            Shared with you by {audience(c.owner)} — their group runs the code on it.
                          </p>
                        )}
                      </div>
                    </>
                  )}
                  {!platform && c.shared && admin && (
                    <div className="flex gap-2">
                      <button
                        type="button"
                        className="btn-ghost px-2.5 py-1 text-xs"
                        onClick={() => setEditing({ cluster: c })}
                      >
                        Edit
                      </button>
                    </div>
                  )}
                </article>
              )
            })}
          </div>
        </>
      )}

      {tab === 'spark' && editing && (
        <ComputeDialog
          cluster={editing.cluster}
          onClose={() => setEditing(null)}
          onSaved={() => {
            void qc.invalidateQueries({ queryKey: ['compute'] })
            setEditing(null)
          }}
          onDeleted={() => {
            void qc.invalidateQueries({ queryKey: ['compute'] })
            setEditing(null)
          }}
        />
      )}
    </div>
  )
}
