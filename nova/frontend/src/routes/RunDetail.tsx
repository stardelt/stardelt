import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from 'react-router-dom'
import { ApiError, api, type PipelineTaskSpec, type RunCheckView, type RunView, type TaskView } from '../api'
import ErrorBox from '../components/ErrorBox'
import RunTaskTable from '../components/RunTaskTable'
import { formatTime, shortRevision } from '../format'
import { stageHref } from './Deployments'
import {
  RunStateBadge,
  formatWhen,
  runDuration,
  runStateWord,
  runTypeWord,
  triggerWord,
} from './Pipelines'

/**
 * One run of a pipeline (stardelt-nova#1111): when it started, its timeline on the run's own
 * span, the tasks as a graph laid out from their `dependsOn` edges, and the task table. A run
 * that is still `queued` or `running` is read again every 5 s, like the pipeline page's panel.
 *
 * Below 700 px the graph is dropped and the table answers: a graph of boxes is a smear on a
 * phone (design stardelt-nova#818).
 */

/** Below this width the graph is dropped and the task table answers. */
const PHONE = '(max-width: 699px)'

function usePhone(): boolean {
  const [phone, setPhone] = useState(() => {
    try {
      return window.matchMedia(PHONE).matches
    } catch {
      return false
    }
  })
  useEffect(() => {
    let query: MediaQueryList
    try {
      query = window.matchMedia(PHONE)
    } catch {
      return
    }
    const read = () => setPhone(query.matches)
    read()
    query.addEventListener('change', read)
    return () => query.removeEventListener('change', read)
  }, [])
  return phone
}

export default function RunDetail() {
  const { namespace = '', name = '', runId = '' } = useParams()
  const phone = usePhone()

  const pipeline = useQuery({
    queryKey: ['pipeline', namespace, name],
    queryFn: () => api.pipeline(namespace, name),
  })
  const detail = useQuery({
    queryKey: ['pipeline-run', namespace, name, runId],
    queryFn: () => api.pipelineRun(namespace, name, runId),
    refetchInterval: (query) => {
      const state = query.state.data?.run.state
      return state === 'queued' || state === 'running' ? 5_000 : false
    },
  })
  const queries = useQuery({
    queryKey: ['pipeline-run-queries', namespace, name, runId],
    queryFn: () => api.pipelineRunQueries(namespace, name, runId),
    refetchInterval:
      detail.data?.run.state === 'running' || detail.data?.run.state === 'queued' ? 5_000 : false,
  })
  const checks = useQuery({
    queryKey: ['pipeline-run-checks', namespace, name, runId],
    queryFn: () => api.pipelineRunChecks(namespace, name, runId),
    refetchInterval:
      detail.data?.run.state === 'running' || detail.data?.run.state === 'queued' ? 5_000 : false,
  })

  if (detail.isLoading) {
    return (
      <div className="max-w-6xl space-y-5">
        <h1 className="text-2xl font-bold tracking-tight break-words">{name}</h1>
        <p className="text-sm text-text-mute">Loading run…</p>
      </div>
    )
  }

  if (detail.isError && !detail.data) {
    const status = detail.error instanceof ApiError ? detail.error.status : 0
    const notFound = status === 404
    return (
      <div className="max-w-2xl space-y-4">
        <h1 className="text-2xl font-bold tracking-tight">{notFound ? 'Not found' : name}</h1>
        <div className="card space-y-3 p-5">
          <ErrorBox
            error={detail.error}
            onRetry={notFound ? undefined : () => void detail.refetch()}
          />
          <div>
            <Link to={`/pipelines/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`} className="btn-ghost w-fit">
              Back to the pipeline
            </Link>
          </div>
        </div>
      </div>
    )
  }

  const view = detail.data
  if (!view) return null

  const run = view.run
  const started = formatWhen(run.startedAt)
  const specs = pipeline.data?.tasks ?? []
  const taskKinds = Object.fromEntries(specs.map((task) => [task.name, task.kind]))
  const notebookTasks = specs.filter((task) => task.kind === 'notebook').map((task) => task.name)
  const taskWrites = Object.fromEntries(
    specs.filter((task) => (task.writes?.length ?? 0) > 0).map((task) => [task.name, task.writes ?? []]),
  )
  const taskRefresh = Object.fromEntries(
    specs.filter((task) => task.kind === 'spark-pipeline').map((task) => [task.name, task.refresh ?? null]),
  )

  return (
    <div className="max-w-6xl space-y-5">
      <header className="min-w-0 space-y-1">
        <p className="text-sm text-text-mute break-all">
          <Link
            to={`/pipelines/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`}
            className="underline"
          >
            {name}
          </Link>
          <span className="pl-2 text-xs">{namespace}</span>
        </p>
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-2xl font-bold tracking-tight break-words" title={run.runId}>
            Run {started.title ?? started.text}
          </h1>
          <RunStateBadge state={run.state} />
        </div>
        <p className="flex flex-wrap items-center gap-x-2 gap-y-1 text-sm text-text-dim">
          <span>{runTypeWord(run.type)}</span>
          <span aria-hidden className="text-text-mute">·</span>
          <span title={run.triggeredBy ?? undefined}>{triggerWord(run.triggeredBy)}</span>
          <span aria-hidden className="text-text-mute">·</span>
          <span>{runDuration(run.durationSeconds)}</span>
        </p>
        {run.bundle && (
          <p className="text-sm text-text-dim break-words">
            Deployed from{' '}
            <Link to={stageHref(run.bundle, run.target ?? '')} className="underline">
              {run.bundle} · {run.target}
            </Link>
            {run.revision && <span className="font-mono text-xs"> @ {shortRevision(run.revision)}</span>}
          </p>
        )}
      </header>

      <section className="card space-y-3 p-4" aria-labelledby="run-timeline-heading">
        <h2 id="run-timeline-heading" className="text-sm font-semibold">Timeline</h2>
        <Timeline run={run} tasks={view.tasks} />
      </section>

      {!phone && (
        <section className="card space-y-3 p-4" aria-labelledby="run-graph-heading">
          <h2 id="run-graph-heading" className="text-sm font-semibold">Task graph</h2>
          {specs.length === 0 ? (
            <p className="text-sm text-text-mute">This pipeline declares no tasks.</p>
          ) : (
            <TaskGraph specs={specs} tasks={view.tasks} />
          )}
        </section>
      )}

      <section className="card space-y-3 p-4" aria-labelledby="run-tasks-heading">
        <h2 id="run-tasks-heading" className="text-sm font-semibold">Tasks</h2>
        <RunTaskTable
          namespace={namespace}
          name={name}
          runId={runId}
          tasks={view.tasks}
          queries={queries.data?.queries ?? []}
          notebookTasks={notebookTasks}
          taskKinds={taskKinds}
          taskWrites={taskWrites}
          taskRefresh={taskRefresh}
        />
      </section>

      <section className="card space-y-3 p-4" aria-labelledby="run-checks-heading">
        <h2 id="run-checks-heading" className="text-sm font-semibold">Checks</h2>
        <RunCheckTable
          checks={checks.data?.checks ?? []}
          error={checks.error}
          wap={pipeline.data?.publish === 'wap'}
          auditBranch={checks.data?.auditBranch}
          auditedTables={checks.data?.auditedTables ?? []}
        />
      </section>
    </div>
  )
}

/**
 * The checks a run declares and what each returned; `rows` is null until the task's log has the
 * line (#829). A `wap` run publishes only when its checks pass, so a failed blocking check also
 * says what that means for production (#866, design #114 decision 6), and which audit branch the
 * data is on so "inspect audit data" is a thing a person can do (#1044).
 */
function RunCheckTable({
  checks,
  error,
  wap,
  auditBranch,
  auditedTables,
}: {
  checks: RunCheckView[]
  error: unknown
  wap: boolean
  auditBranch?: string
  auditedTables: string[]
}) {
  if (error && checks.length === 0) {
    return <ErrorBox error={error} />
  }
  if (checks.length === 0) {
    return <p className="text-sm text-text-mute">This run declares no checks.</p>
  }
  const blocking = checks.filter((c) => c.passed === false && c.severity !== 'warn')
  return (
    <div className="space-y-2">
      {wap && blocking.length > 0 ? (
        <p className="rounded-md border border-amber-400/40 bg-amber-400/10 px-3 py-2 text-sm text-warn">
          Not published: {blocking.length === 1 ? '1 check failed' : `${blocking.length} checks failed`}. Production data is unchanged.
        </p>
      ) : null}
      {wap && auditBranch ? (
        <p className="text-sm text-text-mute">
          Audit branch <code>{auditBranch}</code>
          {auditedTables.length > 0 ? ` on ${auditedTables.join(', ')}` : null}. Inspect it with a
          branch-qualified table read, e.g.{' '}
          <code>
            {auditedTables[0] ?? '<catalog>.<schema>.<table>'}.branch_{auditBranch}
          </code>
          .
        </p>
      ) : null}
      <p className="text-sm text-text-dim">
        {checks.length === 1 ? '1 check' : `${checks.length} checks`}
        {blocking.length > 0 ? ` · ${blocking.length} failed` : ''}
      </p>
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-text-mute">
            <th className="py-1 pr-3 font-medium">Task</th>
            <th className="py-1 pr-3 font-medium">Check</th>
            <th className="py-1 pr-3 font-medium">Rows</th>
            <th className="py-1 font-medium">Verdict</th>
          </tr>
        </thead>
        <tbody>
          {checks.map((c) => (
            <tr key={`${c.task}/${c.name}`} className="border-t border-border">
              <td className="py-1 pr-3">{c.task}</td>
              <td className="py-1 pr-3 break-words">{c.name}</td>
              <td className="py-1 pr-3 tabular-nums">{c.rows ?? '—'}</td>
              <td className="py-1">{checkVerdict(c)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/** A pass is zero rows; `warn` never fails the run, so its non-zero answer reads Warned. */
function checkVerdict(check: RunCheckView): string {
  if (check.passed === null) return 'Not run yet'
  if (check.passed) return 'Passed'
  return check.severity === 'warn' ? 'Warned' : 'Failed'
}

/** The instants a bar is drawn on: the run's own span, or now while it is still running. */
function runSpan(run: RunView): { start: number; end: number } | null {
  if (!run.startedAt) return null
  const start = Date.parse(run.startedAt)
  if (!Number.isFinite(start)) return null
  const ended = run.endedAt ? Date.parse(run.endedAt) : NaN
  const end = Number.isFinite(ended) ? ended : Date.now()
  return { start, end: Math.max(end, start) }
}

/** A task's bar in percent of the run's span; `null` while the task has not started. */
function taskBar(task: TaskView, span: { start: number; end: number }) {
  if (!task.startedAt) return null
  const from = Date.parse(task.startedAt)
  if (!Number.isFinite(from)) return null
  const to = task.endedAt ? Date.parse(task.endedAt) : Date.now()
  const total = span.end - span.start
  if (total <= 0) return null
  const left = Math.min(100, Math.max(0, ((from - span.start) / total) * 100))
  const width = Math.min(100 - left, Math.max(0.5, ((to - from) / total) * 100))
  return { left, width }
}

function Timeline({ run, tasks }: { run: RunView; tasks: TaskView[] }) {
  const span = useMemo(() => runSpan(run), [run])
  if (tasks.length === 0) return <p className="text-sm text-text-mute">This run has no tasks yet.</p>
  if (!span) return <p className="text-sm text-text-mute">This run has not started yet.</p>
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs text-text-mute">
        <span>{formatTime(span.start)}</span>
        <span>{run.endedAt ? formatTime(span.end) : 'now'}</span>
      </div>
      <ol className="space-y-1.5">
        {tasks.map((task) => {
          const bar = taskBar(task, span)
          return (
            <li key={`${task.taskId}-${task.tryNumber}`} className="flex items-center gap-3">
              <span className="w-28 shrink-0 sm:w-40">
                <span className="block truncate font-mono text-xs" title={task.taskId}>
                  {task.taskId}
                </span>
                <span className="block text-[11px] text-text-dim">{runStateWord(task.state)}</span>
              </span>
              <div className="relative h-6 flex-1 rounded bg-bg-soft">
                {bar ? (
                  <div
                    data-testid="run-timeline-bar"
                    data-task={task.taskId}
                    title={`${task.taskId} · ${runStateWord(task.state)}`}
                    className="absolute top-0 flex h-full items-center overflow-hidden rounded bg-accent-solid/70 px-1.5 text-[11px] whitespace-nowrap text-white"
                    style={{ left: `${bar.left}%`, width: `${bar.width}%` }}
                  >
                    {/* The bar names itself where it is wide enough; the label beside it always does. */}
                    <span className="hidden min-[700px]:inline">
                      {task.taskId} · {runStateWord(task.state)}
                    </span>
                  </div>
                ) : (
                  <span className="absolute inset-y-0 left-2 flex items-center text-xs text-text-mute">
                    not started
                  </span>
                )}
              </div>
            </li>
          )
        })}
      </ol>
    </div>
  )
}

/** A box is this wide and high, a layer this far from the next; plain `div`s, no dependency. */
const NODE_W = 168
const NODE_H = 52
const COL_W = 216
const ROW_H = 68

type Node = { name: string; state: string | null; x: number; y: number }

/**
 * Lay the tasks out left to right from their edges: a task's layer is one more than the
 * deepest task it waits for, and the tasks of a layer stack downwards in declaration order.
 * A dependency outside the list, or a loop, is ignored here: the resource is validated on
 * create, and a hand-written one must still draw.
 */
function layout(specs: PipelineTaskSpec[], tasks: TaskView[]) {
  const known = new Set(specs.map((spec) => spec.name))
  const layer = new Map<string, number>()
  const depth = (name: string, seen: Set<string>): number => {
    const memo = layer.get(name)
    if (memo != null) return memo
    if (seen.has(name)) return 0
    seen.add(name)
    const spec = specs.find((candidate) => candidate.name === name)
    const deps = (spec?.dependsOn ?? []).filter((dep) => known.has(dep))
    const value = deps.length === 0 ? 0 : 1 + Math.max(...deps.map((dep) => depth(dep, seen)))
    layer.set(name, value)
    return value
  }
  const state = new Map<string, { state: string; tryNumber: number }>()
  for (const task of tasks) {
    const seen = state.get(task.taskId)
    if (!seen || task.tryNumber >= seen.tryNumber) state.set(task.taskId, task)
  }

  const rows = new Map<number, number>()
  const nodes: Node[] = specs.map((spec) => {
    const column = depth(spec.name, new Set())
    const row = rows.get(column) ?? 0
    rows.set(column, row + 1)
    return {
      name: spec.name,
      state: state.get(spec.name)?.state ?? null,
      x: column * COL_W,
      y: row * ROW_H,
    }
  })

  const at = new Map(nodes.map((node) => [node.name, node]))
  const edges: { from: string; to: string; path: string }[] = []
  for (const spec of specs) {
    const to = at.get(spec.name)
    if (!to) continue
    for (const dep of spec.dependsOn ?? []) {
      const from = at.get(dep)
      if (!from) continue
      const x1 = from.x + NODE_W
      const y1 = from.y + NODE_H / 2
      const x2 = to.x
      const y2 = to.y + NODE_H / 2
      const bend = Math.max(24, (x2 - x1) / 2)
      edges.push({
        from: dep,
        to: spec.name,
        path: `M ${x1} ${y1} C ${x1 + bend} ${y1}, ${x2 - bend} ${y2}, ${x2} ${y2}`,
      })
    }
  }

  const columns = Math.max(1, ...nodes.map((node) => node.x / COL_W + 1))
  const height = Math.max(1, ...nodes.map((node) => node.y / ROW_H + 1)) * ROW_H
  return { nodes, edges, width: columns * COL_W, height }
}

function TaskGraph({ specs, tasks }: { specs: PipelineTaskSpec[]; tasks: TaskView[] }) {
  const graph = useMemo(() => layout(specs, tasks), [specs, tasks])
  return (
    <div className="overflow-x-auto">
      <div className="relative" style={{ width: graph.width, height: graph.height }}>
        <svg className="absolute inset-0 text-border" width={graph.width} height={graph.height} aria-hidden>
          {graph.edges.map((edge) => (
            <path
              key={`${edge.from}->${edge.to}`}
              data-testid="run-graph-edge"
              d={edge.path}
              fill="none"
              stroke="currentColor"
              strokeWidth={1.5}
            />
          ))}
        </svg>
        {graph.nodes.map((node) => (
          <div
            key={node.name}
            data-testid="run-graph-node"
            data-task={node.name}
            className="absolute overflow-hidden rounded-[10px] border border-border bg-bg-elev/60 px-2.5 py-1.5"
            style={{ left: node.x, top: node.y, width: NODE_W, height: NODE_H }}
          >
            <p className="truncate font-mono text-xs" title={node.name}>{node.name}</p>
            <p className="text-[11px] text-text-dim">{node.state ? runStateWord(node.state) : 'Not run'}</p>
          </div>
        ))}
      </div>
    </div>
  )
}
