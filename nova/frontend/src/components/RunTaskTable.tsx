import { useState } from 'react'
import { useInfiniteQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api, type LogLine, type RunQueryView, type TaskView } from '../api'
import ErrorBox from './ErrorBox'
import { formatBytes, formatDurationMs, relativeTime } from '../format'
import { RunStateBadge, formatWhen, runDuration } from '../routes/Pipelines'

/**
 * The table of a run's tasks (Task, State, Try, Started, Duration, Kind, Tables, log, notebook) and,
 * when the run sent statements, their table slowest first. The pipeline page shows it under a
 * run; the run page shows it below the graph (stardelt-nova#1111) — one copy, so the two
 * cannot drift.
 */
export default function RunTaskTable({
  namespace,
  name,
  runId,
  tasks,
  queries,
  notebookTasks,
  taskKinds,
  taskWrites,
  taskRefresh,
}: {
  namespace: string
  name: string
  runId: string
  tasks: TaskView[]
  queries: RunQueryView[]
  notebookTasks: string[]
  taskKinds: Record<string, string>
  taskWrites?: Record<string, string[]>
  taskRefresh?: Record<string, string | null>
}) {
  const [log, setLog] = useState<{ taskId: string; tryNumber: number } | null>(null)

  return (
    <div className="space-y-3">
      {tasks.length === 0 ? (
        <p className="text-sm text-text-mute">This run has no tasks yet.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <caption className="sr-only">Task instances of this run.</caption>
            <thead>
              <tr>
                <th scope="col" className="th min-w-[7rem] px-3 py-2">Task</th>
                <th scope="col" className="th px-3 py-2">State</th>
                <th scope="col" className="th px-3 py-2">Try</th>
                <th scope="col" className="th px-3 py-2">Started</th>
                <th scope="col" className="th px-3 py-2">Duration</th>
                <th scope="col" className="th px-3 py-2">Kind</th>
                <th scope="col" className="th px-3 py-2">Tables</th>
                <th scope="col" className="th px-3 py-2">
                  <span className="sr-only">Log</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {tasks.map((task) => {
                const started = formatWhen(task.startedAt)
                return (
                  <tr key={`${task.taskId}-${task.tryNumber}`} className="border-t border-border/70">
                    <td className="td px-3"><span className="font-mono text-xs whitespace-nowrap">{task.taskId}</span></td>
                    <td className="td px-3"><RunStateBadge state={task.state} /></td>
                    <td className="td px-3 text-text-dim">{task.tryNumber}</td>
                    <td className="td px-3 text-text-dim text-xs whitespace-nowrap">
                      <span title={started.title}>{started.text}</span>
                    </td>
                    <td className="td px-3 text-text-dim text-xs whitespace-nowrap">
                      {runDuration(task.durationSeconds)}
                    </td>
                    <td className="td px-3 text-text-dim">{taskKinds[task.taskId] ?? '—'}</td>
                    <td className="td px-3">
                      {(() => {
                        const writes = taskWrites?.[task.taskId] ?? []
                        if (writes.length === 0) return <span className="text-text-dim">—</span>
                        const refresh = taskRefresh?.[task.taskId]
                        return (
                          <div className="space-y-0.5" title="The tables this task declares; row counts are not reported yet.">
                            {writes.map((table) => (
                              <div key={table} className="font-mono text-xs whitespace-nowrap">{table}</div>
                            ))}
                            {refresh && <div className="text-[11px] text-text-dim">{refresh} refresh</div>}
                          </div>
                        )
                      })()}
                    </td>
                    <td className="td px-3">
                      <div className="flex flex-wrap items-center gap-1.5">
                        <button
                          type="button"
                          className="btn-ghost py-1 px-2 text-xs"
                          aria-label={`View log of ${task.taskId} try ${task.tryNumber}`}
                          onClick={() => setLog({ taskId: task.taskId, tryNumber: task.tryNumber })}
                        >
                          View log
                        </button>
                        {notebookTasks.includes(task.taskId) && (
                          <Link
                            className="btn-ghost py-1 px-2 text-xs whitespace-nowrap"
                            to={`/pipelines/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/runs/${encodeURIComponent(runId)}/notebook/${encodeURIComponent(task.taskId)}`}
                          >
                            Open notebook
                          </Link>
                        )}
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {queries.length > 0 && (
        <div className="space-y-2">
          <h3 className="text-sm font-semibold">Statements this run sent, slowest first</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <caption className="sr-only">The run's queries, slowest first.</caption>
              <thead>
                <tr>
                  <th scope="col" className="th px-3 py-2">Task</th>
                  <th scope="col" className="th px-3 py-2">State</th>
                  <th scope="col" className="th px-3 py-2">Started</th>
                  <th scope="col" className="th px-3 py-2">Wall</th>
                  <th scope="col" className="th px-3 py-2">Read</th>
                  <th scope="col" className="th px-3 py-2">Statement</th>
                </tr>
              </thead>
              <tbody>
                {queries.map((q) => (
                  <tr key={q.id} className="border-t border-border/70">
                    <td className="td px-3"><span className="font-mono text-xs">{q.taskId ?? '—'}</span></td>
                    <td className="td px-3"><RunStateBadge state={q.state} /></td>
                    <td className="td px-3 text-text-dim text-xs whitespace-nowrap">
                      {q.startedAt != null ? relativeTime(q.startedAt) : '—'}
                    </td>
                    <td className="td px-3 text-text-dim text-xs whitespace-nowrap">{formatDurationMs(q.wallMs)}</td>
                    <td className="td px-3 text-text-dim text-xs whitespace-nowrap">{formatBytes(q.inputBytes)}</td>
                    <td className="td px-3">
                      <span className="font-mono text-xs line-clamp-2 break-all" title={q.sql}>{q.sql}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {log && (
        <TaskLog
          key={`${log.taskId}-${log.tryNumber}`}
          namespace={namespace}
          name={name}
          runId={runId}
          taskId={log.taskId}
          tryNumber={log.tryNumber}
        />
      )}
    </div>
  )
}

/** A level as a word: colour is never the only signal. */
function levelWord(level: string | null): string {
  if (!level) return 'Log'
  return level.charAt(0).toUpperCase() + level.slice(1).toLowerCase()
}

function TaskLog({
  namespace,
  name,
  runId,
  taskId,
  tryNumber,
}: {
  namespace: string
  name: string
  runId: string
  taskId: string
  tryNumber: number
}) {
  const [copied, setCopied] = useState(false)
  const log = useInfiniteQuery({
    queryKey: ['pipeline-log', namespace, name, runId, taskId, tryNumber],
    initialPageParam: undefined as string | undefined,
    queryFn: ({ pageParam }) =>
      api.pipelineLog(namespace, name, runId, taskId, { tryNumber, token: pageParam }),
    getNextPageParam: (page) => page.nextToken || undefined,
  })

  const pages = log.data?.pages ?? []
  const lines: LogLine[] = pages.flatMap((page) => page.lines)
  const truncated = pages.some((page) => page.truncated)

  async function copy() {
    const text = lines
      .map((line) => [line.timestamp ?? '', levelWord(line.level), line.text].filter(Boolean).join(' '))
      .join('\n')
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1500)
    } catch {
      // Clipboard access can be refused (insecure origin, permissions); nothing to report.
    }
  }

  return (
    <div className="rounded-[10px] border border-border bg-bg-elev/40">
      <div className="flex flex-wrap items-center gap-2 border-b border-border/70 px-3 py-2">
        <span className="min-w-0 text-sm font-medium break-all">
          Log of <span className="font-mono text-xs">{taskId}</span> · try {tryNumber}
        </span>
        <div className="ml-auto flex items-center gap-2">
          <button
            type="button"
            className="btn-ghost py-1 px-2 text-xs"
            disabled={lines.length === 0}
            onClick={() => void copy()}
          >
            {copied ? 'Copied' : 'Copy'}
          </button>
          {log.hasNextPage && (
            <button
              type="button"
              className="btn-ghost py-1 px-2 text-xs"
              disabled={log.isFetchingNextPage}
              onClick={() => void log.fetchNextPage()}
            >
              {log.isFetchingNextPage ? 'Loading…' : 'Load more'}
            </button>
          )}
        </div>
      </div>
      <div className="max-h-80 space-y-1 overflow-auto p-3">
        {log.isLoading && <p className="text-sm text-text-mute">Loading log…</p>}
        {log.isError && <ErrorBox error={log.error} onRetry={() => void log.refetch()} />}
        {log.data && lines.length === 0 && <p className="text-sm text-text-mute">No log lines (yet).</p>}
        {lines.map((line, index) => (
          <div key={index} className="flex items-start gap-2 text-xs" title={line.timestamp ?? undefined}>
            <span className="label shrink-0 pt-0.5">{levelWord(line.level)}</span>
            <span className="min-w-0 font-mono whitespace-pre-wrap break-words">{line.text}</span>
          </div>
        ))}
        {truncated && <p className="pt-1 text-xs text-text-mute">The log was cut at 2000 lines.</p>}
      </div>
    </div>
  )
}
