import { useQuery } from '@tanstack/react-query'
import { ApiError, api } from '../api'
import { EmptyState } from './EmptyState'
import ErrorBox from './ErrorBox'
import NotebookFrame from './NotebookFrame'
import { renderNotebook } from '../notebookHtml'

/**
 * One task instance's executed notebook (#683): the copy the run wrote, every cell's output
 * and the traceback, opened at the cell that failed.
 *
 * The notebook is rendered into the sandboxed frame `NotebookFrame`, whose sandbox omits the
 * same-origin permission; its HTML, images and scripts can run but reach neither Nova's DOM nor
 * its API. `RunNotebook` and the report page both draw the notebook through this component, so
 * the two cannot drift.
 */
export default function ExecutedNotebook({
  namespace,
  name,
  runId,
  taskId,
}: {
  namespace: string
  name: string
  runId: string
  taskId: string
}) {
  const notebook = useQuery({
    queryKey: ['run-notebook', namespace, name, runId, taskId],
    queryFn: () => api.runNotebook(namespace, name, runId, taskId),
  })

  const rendered = notebook.data ? renderNotebook(notebook.data) : null
  const missing = notebook.isError && notebook.error instanceof ApiError && notebook.error.status === 404

  if (missing) {
    return (
      <div className="card p-2">
        <EmptyState icon="notebook" title="No executed notebook for this run">
          This run's executed notebook is not there: it was never written, or it expired. The task
          log still has what the scheduler saw.
        </EmptyState>
      </div>
    )
  }

  if (notebook.isError) {
    return <ErrorBox error={notebook.error} onRetry={() => void notebook.refetch()} />
  }

  if (!rendered) {
    return <p className="text-sm text-text-mute">Loading the executed notebook…</p>
  }

  return (
    <>
      {rendered.error && (
        <div role="alert" className="rounded-[10px] border border-bad/30 bg-bad/5 px-4 py-3 text-sm">
          <p className="text-text font-medium break-words">
            Failed in cell {(rendered.failedCell ?? 0) + 1}: {rendered.error.ename}: {rendered.error.evalue}
          </p>
          <details className="mt-2">
            <summary className="cursor-pointer text-xs text-text-dim">Traceback</summary>
            <pre className="mt-2 overflow-x-auto rounded-lg bg-bg-soft px-3 py-2 font-mono text-xs whitespace-pre-wrap break-words">
              {rendered.error.traceback}
            </pre>
          </details>
        </div>
      )}

      <NotebookFrame title="Executed notebook" html={rendered.html} />
    </>
  )
}
