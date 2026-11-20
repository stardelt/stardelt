import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from 'react-router-dom'
import { api, runNotebookUrl } from '../api'
import ErrorBox from '../components/ErrorBox'
import ExecutedNotebook from '../components/ExecutedNotebook'
import { EmptyState } from '../components/EmptyState'
import { formatTime } from '../format'

/**
 * The report of a notebook pipeline (#120, #79 point 6): the executed notebook of its latest
 * **successful** run, so a visitor needs one link and no run id.
 *
 * The page reads `GET /api/pipelines/{namespace}/{name}` only — the detail with its latest 20
 * runs, honouring `spec.viewers` — and draws the notebook through the same `ExecutedNotebook`
 * the run route uses. Successful runs are ordered here rather than trusted from the scheduler's
 * ordering, so the page always shows the newest one.
 */
export default function Report() {
  const { namespace = '', name = '' } = useParams()
  const pipelineHref = `/pipelines/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`

  const detail = useQuery({
    queryKey: ['pipeline', namespace, name],
    queryFn: () => api.pipeline(namespace, name),
  })

  if (detail.isLoading) {
    return (
      <div className="max-w-6xl space-y-5">
        <h1 className="text-2xl font-bold tracking-tight break-words">{name}</h1>
        <p className="text-sm text-text-mute">Loading pipeline…</p>
      </div>
    )
  }

  if (detail.isError && !detail.data) {
    return (
      <div className="max-w-2xl space-y-4">
        <h1 className="text-2xl font-bold tracking-tight break-words">{name}</h1>
        <div className="card space-y-3 p-5">
          <ErrorBox error={detail.error} onRetry={() => void detail.refetch()} />
          <div>
            <Link to="/pipelines" className="btn-ghost w-fit">Back to Pipelines</Link>
          </div>
        </div>
      </div>
    )
  }

  const view = detail.data
  if (!view) {
    // `isLoading` and `isError` cover every state react-query has; this only narrows the type.
    return null
  }

  const notebookTask = view.tasks.find((task) => task.kind === 'notebook')
  const successful = [...view.runs]
    .filter((run) => run.state === 'success')
    .sort((a, b) => (b.startedAt ?? '').localeCompare(a.startedAt ?? ''))
  const run = successful[0]
  const earlier = successful.slice(1, 6)
  const asOf = run ? formatTime(run.endedAt ?? run.startedAt ?? 0) : null

  return (
    <div className="max-w-6xl space-y-5">
      <header className="space-y-2">
        <div className="text-xs">
          <Link to={pipelineHref} className="underline text-text-dim">◂ {name}</Link>
        </div>
        <h1 className="text-2xl font-bold tracking-tight break-words">{view.name}</h1>
        <p className="text-sm text-text-dim" data-testid="report-as-of">
          {asOf && <span>as of {asOf}</span>}
          {view.nextRun && <span> · next run {formatTime(view.nextRun)}</span>}
        </p>
      </header>

      {!notebookTask ? (
        <div className="card p-2">
          <EmptyState
            icon="notebook"
            title="No notebook here"
            action={<Link to={pipelineHref} className="btn-ghost px-3 py-1.5 text-sm">Open the pipeline</Link>}
          >
            This pipeline does not run a notebook.
          </EmptyState>
        </div>
      ) : !run ? (
        <div className="card p-2">
          <EmptyState
            icon="notebook"
            title="No report yet"
            action={
              <Link to={pipelineHref} className="btn-ghost px-3 py-1.5 text-sm">
                See the pipeline's runs
              </Link>
            }
          >
            The latest run is {view.lastRun?.state ?? 'never'}. This page shows the executed
            notebook of the newest successful run, and there is none yet.
          </EmptyState>
        </div>
      ) : (
        <>
          <ExecutedNotebook
            namespace={namespace}
            name={name}
            runId={run.runId}
            taskId={notebookTask.name}
          />

          <div className="flex flex-wrap items-center gap-2">
            <a
              className="btn-ghost py-1.5"
              href={runNotebookUrl(namespace, name, run.runId, notebookTask.name, true)}
              download
            >
              Download
            </a>
          </div>

          {earlier.length > 0 && (
            <section className="card p-5">
              <h2 className="text-sm font-semibold">Earlier reports</h2>
              <ul className="mt-2 space-y-1 text-sm">
                {earlier.map((older) => (
                  <li key={older.runId}>
                    <Link
                      className="underline text-text-dim"
                      to={`/pipelines/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/runs/${encodeURIComponent(older.runId)}/notebook/${encodeURIComponent(notebookTask.name)}`}
                    >
                      {formatTime(older.endedAt ?? older.startedAt ?? 0)}
                    </Link>
                  </li>
                ))}
              </ul>
            </section>
          )}
        </>
      )}
    </div>
  )
}
