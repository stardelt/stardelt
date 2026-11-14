import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, type McpCall } from '../api'
import ErrorBox from '../components/ErrorBox'
import { formatDurationMs, formatTime, relativeTime } from '../format'

// The administrator's view of the hosted MCP endpoint (stardelt-nova#975, design
// agent-access.md §4 and §9, T7): every `/mcp` tool call, newest first — who, which
// client, which tool, the status, the duration and a hash of the arguments. The rows are
// written by the endpoint itself (`backend/src/mcp_audit.rs`, #970); this page only
// arranges them, so nothing here is state Nova keeps beyond the API's answer.

/** How many rows the page asks for; the API accepts 1 to 500. */
const LIMITS = [50, 100, 200, 500] as const

/** A filter typed into a box, applied once the person stops typing. */
function useDebounced<T>(value: T, ms: number): T {
  const [debounced, setDebounced] = useState(value)
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), ms)
    return () => clearTimeout(timer)
  }, [value, ms])
  return debounced
}

export default function AgentCalls() {
  const [person, setPerson] = useState('')
  const [tool, setTool] = useState('')
  const [limit, setLimit] = useState<number>(LIMITS[0])
  // The API matches `sub` and `tool` exactly, so the boxes are exact names, not searches.
  const sub = useDebounced(person.trim(), 300)
  const toolName = useDebounced(tool.trim(), 300)

  const calls = useQuery({
    queryKey: ['mcp-audit', sub, toolName, limit],
    queryFn: () =>
      api.mcpAudit({ limit, sub: sub || undefined, tool: toolName || undefined }),
    refetchInterval: 15_000,
  })

  const rows = calls.data?.calls ?? []

  return (
    <div className="max-w-6xl space-y-5">
      <header>
        <h1 className="text-2xl font-bold tracking-tight">Agent calls</h1>
        <p className="text-text-dim text-sm mt-1">
          Every call of the hosted MCP endpoint, newest first: the person, their client, the
          tool and how the call went. Nova keeps a hash of the arguments, never the arguments.
        </p>
      </header>

      <div className="flex flex-wrap items-end gap-3">
        <div>
          <label htmlFor="agent-calls-person" className="label block mb-1.5">Person</label>
          <input
            id="agent-calls-person"
            type="search"
            className="input w-full sm:w-64"
            placeholder="the signed-in subject"
            value={person}
            autoComplete="off"
            spellCheck={false}
            onChange={(event) => setPerson(event.target.value)}
          />
        </div>
        <div>
          <label htmlFor="agent-calls-tool" className="label block mb-1.5">Tool</label>
          <input
            id="agent-calls-tool"
            type="search"
            className="input w-full sm:w-56"
            placeholder="sql_read"
            value={tool}
            autoComplete="off"
            spellCheck={false}
            onChange={(event) => setTool(event.target.value)}
          />
        </div>
        <div>
          <label htmlFor="agent-calls-limit" className="label block mb-1.5">Show</label>
          <select
            id="agent-calls-limit"
            className="input !w-auto"
            value={limit}
            onChange={(event) => setLimit(Number(event.target.value))}
          >
            {LIMITS.map((value) => (
              <option key={value} value={value}>{value}</option>
            ))}
          </select>
        </div>
      </div>

      {calls.isLoading && <div className="text-text-mute text-sm">loading…</div>}
      {calls.isError && (
        <ErrorBox error={calls.error} onRetry={() => void calls.refetch()} />
      )}

      {calls.isSuccess && (
        <div className="card relative overflow-x-auto">
          <table className="w-full min-w-[44rem] text-sm">
            <caption className="sr-only">Agent calls, newest first</caption>
            <thead className="bg-bg-soft/50">
              <tr>
                <th scope="col" className="th px-5 py-2.5">When</th>
                <th scope="col" className="th px-3 py-2.5">Person</th>
                <th scope="col" className="th px-3 py-2.5">Client</th>
                <th scope="col" className="th px-3 py-2.5">Tool</th>
                <th scope="col" className="th px-3 py-2.5">Status</th>
                <th scope="col" className="th px-3 py-2.5 text-right">Duration</th>
                <th scope="col" className="th px-3 py-2.5">Arguments</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 && (
                <tr>
                  <td className="px-5 py-6" colSpan={7}>
                    <p className="font-medium">No agent calls yet.</p>
                    <p className="text-text-mute mt-1 max-w-2xl">
                      Every /mcp tool call is recorded here: who, which client, which tool, the
                      status, the duration and a hash of the arguments — never the arguments
                      themselves.
                    </p>
                  </td>
                </tr>
              )}
              {rows.map((call) => (
                <Row key={`${call.atMs}-${call.sub}-${call.tool}-${call.argumentsSha256}`} call={call} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

/** One recorded call: the time in words, the person, the client and how it went. */
function Row({ call }: { call: McpCall }) {
  const bad = call.status !== 'ok'
  return (
    <tr className="border-t border-border/70">
      <td
        className="td px-5 py-3 text-text-dim whitespace-nowrap"
        title={formatTime(call.atMs)}
      >
        {relativeTime(call.atMs)}
      </td>
      <td className="td px-3 py-3 font-mono text-xs break-words">{call.sub}</td>
      <td className="td px-3 py-3 text-text-dim break-words">
        {call.client ?? <span className="text-text-mute">—</span>}
      </td>
      <td className="td px-3 py-3 font-mono text-xs break-words">{call.tool}</td>
      <td className={bad ? 'td px-3 py-3 text-bad whitespace-nowrap' : 'td px-3 py-3 whitespace-nowrap'}>
        {call.status}
      </td>
      <td className="td px-3 py-3 text-right text-text-dim whitespace-nowrap">
        {formatDurationMs(call.durationMs)}
      </td>
      <td className="td px-3 py-3 font-mono text-xs text-text-mute whitespace-nowrap" title={call.argumentsSha256}>
        {call.argumentsSha256.slice(0, 12)}
      </td>
    </tr>
  )
}
