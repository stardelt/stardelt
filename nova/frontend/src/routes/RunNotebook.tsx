import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api, runNotebookUrl } from '../api'
import ErrorBox from '../components/ErrorBox'
import ExecutedNotebook from '../components/ExecutedNotebook'
import { PipelineStateBadge, formatWhen, runDuration } from './Pipelines'

/**
 * One run's executed notebook (#683): the copy the run wrote, every cell's output and the
 * traceback, opened at the cell that failed.
 *
 * The notebook is rendered into the sandboxed frame `NotebookFrame`, whose sandbox omits the
 * same-origin permission; its HTML, images and scripts can run but reach neither Nova's DOM nor
 * its API. The drawing itself lives in `ExecutedNotebook`, which the report page shares.
 */
export default function RunNotebook() {
  const { namespace = '', name = '', runId = '', taskId = '' } = useParams()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [rerunning, setRerunning] = useState(false)
  const [retrying, setRetrying] = useState(false)
  const [retryError, setRetryError] = useState<unknown>(null)

  // The run's state is a fact about the run, not about the notebook: a failure here only
  // leaves the badge out.
  const run = useQuery({
    queryKey: ['pipeline-run', namespace, name, runId],
    queryFn: () => api.pipelineRun(namespace, name, runId),
  })

  // What ran: the pinned notebook version and its stored parameters. Like the run, a failure
  // here only leaves those parts out — the executed notebook is the page.
  const pipeline = useQuery({
    queryKey: ['pipeline', namespace, name],
    queryFn: () => api.pipeline(namespace, name),
  })
  const spec = pipeline.data?.tasks.find((t) => t.name === taskId)
  const started = formatWhen(run.data?.run.startedAt)
  const params = spec?.notebookParams && Object.keys(spec.notebookParams).length > 0 ? spec.notebookParams : null

  async function again() {
    setRerunning(true)
    try {
      await api.startPipelineRun(namespace, name)
      navigate(`/pipelines/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`)
    } finally {
      setRerunning(false)
    }
  }

  async function retryFailed() {
    setRetrying(true)
    setRetryError(null)
    try {
      await api.retryPipelineRun(namespace, name, runId)
      await queryClient.invalidateQueries({ queryKey: ['pipeline-run', namespace, name, runId] })
    } catch (e) {
      setRetryError(e)
    } finally {
      setRetrying(false)
    }
  }

  return (
    <div className="max-w-6xl space-y-5">
      <header className="space-y-2">
        <Link to={`/pipelines/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`} className="btn-ghost w-fit px-2 py-1 text-xs">
          ◂ {name}
        </Link>
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-2xl font-bold tracking-tight break-all">Run {runId}</h1>
          {run.data && <PipelineStateBadge state={run.data.run.state} />}
        </div>
        <p className="text-sm text-text-dim" data-testid="run-provenance">
          {spec && (
            <>
              <span>{spec.notebook || 'notebook'}</span>
              {spec.objectVersion && (
                <span title={spec.objectVersion}> · version {spec.objectVersion.slice(0, 8)}</span>
              )}
            </>
          )}
          {run.data?.run.startedAt && (
            <span title={started.title}> · started {started.text}</span>
          )}
          {run.data && run.data.run.durationSeconds != null && (
            <span> · took {runDuration(run.data.run.durationSeconds)}</span>
          )}
          {run.data && (
            <span> · started by {run.data.run.triggeredBy ?? run.data.run.type}</span>
          )}
          {pipeline.data?.runAs && <span> · as {pipeline.data.runAs}</span>}
        </p>
      </header>

      {params && (
        <details className="text-sm">
          <summary className="cursor-pointer text-text-dim">Parameters ({Object.keys(params).length})</summary>
          <div className="mt-2 space-y-1 font-mono text-xs">
            {Object.entries(params).map(([key, value]) => (
              <div key={key}>
                {key} = {value}
              </div>
            ))}
          </div>
        </details>
      )}

      <div className="flex flex-wrap items-center gap-2">
        {run.data?.run.state === 'failed' && (
          <button type="button" className="btn-primary py-1.5" onClick={() => void retryFailed()} disabled={retrying}>
            Retry failed tasks
          </button>
        )}
        <button type="button" className="btn-primary py-1.5" onClick={() => void again()} disabled={rerunning}>
          Run again
        </button>
        <a
          className="btn-ghost py-1.5"
          href={runNotebookUrl(namespace, name, runId, taskId, true)}
          download
        >
          Download
        </a>
      </div>

      {retryError ? <ErrorBox error={retryError} /> : null}

      <ExecutedNotebook namespace={namespace} name={name} runId={runId} taskId={taskId} />
    </div>
  )
}
