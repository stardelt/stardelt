import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  api,
  type ModelEndpoint,
  type ModelUsageCall,
  type ModelUsageRow,
} from '../api'
import ErrorBox from '../components/ErrorBox'
import { formatDurationMs, formatTime, relativeTime } from '../format'

// The administrator's view of the models (design stardelt-nova#890 item 5): the endpoints
// an administrator connected and what the probe found (`GET /api/model-endpoints`, #985),
// this month's tokens per group and endpoint (`GET /api/admin/model-usage`, #997) and the
// last calls. The page only arranges the API's answers; the endpoint stays a
// `ModelEndpoint` resource, so there is no form here — connecting one is Kubernetes work.
// Nova keeps sizes and hashes, never a conversation, and this page shows no URL and no key.

/** How many calls the page asks for; the API accepts 1 to 500. */
const LIMITS = [50, 100, 200, 500] as const

/** Where a prompt goes, in Nova's words rather than the spec's. */
function wherePromptsGo(location: string): string {
  return location === 'cloud' ? 'leaves your cluster' : 'on-premise'
}

/**
 * The probe's verdict in one phrase: `Ready`, `Not ready` or `Not probed`. An endpoint that
 * answers but cannot call tools is not ready either — Ask and the agents need the tool call
 * (`backend/src/model_probe.rs`, "an answer without a tool call is not tool calling").
 */
function probeWord(endpoint: ModelEndpoint): string {
  if (endpoint.reachable === false) return 'Not ready'
  if (endpoint.reachable === true) return endpoint.toolCalling === true ? 'Ready' : 'Not ready'
  return 'Not probed'
}

/** When the probe last looked, as "12 min ago"; an unparsable stamp reads as unknown. */
function checkedLabel(checkedAt: string | null | undefined): string | null {
  if (!checkedAt) return null
  const ms = Date.parse(checkedAt)
  return Number.isNaN(ms) ? null : relativeTime(ms)
}

export default function Models() {
  const qc = useQueryClient()
  const [limit, setLimit] = useState<number>(LIMITS[0])

  const endpoints = useQuery({
    queryKey: ['model-endpoints'],
    queryFn: api.modelEndpoints,
    refetchInterval: 15_000,
  })
  const usage = useQuery({
    queryKey: ['model-usage', limit],
    queryFn: () => api.modelUsage({ limit }),
    refetchInterval: 30_000,
  })

  const probe = useMutation({
    mutationFn: (name: string) => api.probeModelEndpoint(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['model-endpoints'] }),
  })

  const rows = endpoints.data ?? []
  const groups = usage.data?.groups ?? []
  const calls = usage.data?.calls ?? []
  const anyEstimated = groups.some((row) => row.estimatedCalls > 0)

  return (
    <div className="max-w-6xl space-y-5">
      <header>
        <h1 className="text-2xl font-bold tracking-tight">Models</h1>
        <p className="text-text-dim text-sm mt-1">
          The model endpoints an administrator connected, what the probe found, and what the
          platform used this month. Prompts go to the endpoint; Nova keeps sizes and hashes,
          never the conversation.
        </p>
      </header>

      <section className="space-y-3">
        <h2 className="text-lg font-semibold tracking-tight">Endpoints</h2>

        {endpoints.isLoading && <p className="text-text-mute text-sm">Loading…</p>}
        {endpoints.isError && (
          <ErrorBox error={endpoints.error} onRetry={() => void endpoints.refetch()} />
        )}
        {probe.isError && <ErrorBox error={probe.error} />}

        {endpoints.isSuccess && (
          <div className="card relative overflow-x-auto">
            <table className="w-full min-w-[46rem] text-sm">
              <caption className="sr-only">The model endpoints an administrator connected</caption>
              <thead className="bg-bg-soft/50">
                <tr>
                  <th scope="col" className="th px-5 py-2.5">Name</th>
                  <th scope="col" className="th px-3 py-2.5">Model</th>
                  <th scope="col" className="th px-3 py-2.5">Where prompts go</th>
                  <th scope="col" className="th px-3 py-2.5">Purposes</th>
                  <th scope="col" className="th px-3 py-2.5">Probe</th>
                  <th scope="col" className="th px-3 py-2.5" />
                </tr>
              </thead>
              <tbody>
                {rows.length === 0 && (
                  <tr>
                    <td className="px-5 py-6" colSpan={6}>
                      <p className="font-medium">No model endpoint is connected yet.</p>
                      <p className="text-text-mute mt-1 max-w-2xl">
                        An administrator connects one with a ModelEndpoint resource — see the{' '}
                        <a
                          href="https://stardelt.io/docs/guides/models/"
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-accent hover:underline"
                        >
                          guide
                        </a>
                        .
                      </p>
                    </td>
                  </tr>
                )}
                {rows.map((endpoint) => (
                  <EndpointRow
                    key={endpoint.name}
                    endpoint={endpoint}
                    disabled={probe.isPending}
                    busy={probe.isPending && probe.variables === endpoint.name}
                    onProbe={() => probe.mutate(endpoint.name)}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="space-y-3">
        <h2 className="text-lg font-semibold tracking-tight">
          This month{usage.data ? ` — ${usage.data.month}` : ''}
        </h2>

        {usage.isLoading && <p className="text-text-mute text-sm">Loading…</p>}
        {usage.isError && <ErrorBox error={usage.error} onRetry={() => void usage.refetch()} />}

        {usage.isSuccess && (
          <div className="card relative overflow-x-auto">
            <table className="w-full min-w-[34rem] text-sm">
              <caption className="sr-only">This month's tokens per group and endpoint</caption>
              <thead className="bg-bg-soft/50">
                <tr>
                  <th scope="col" className="th px-5 py-2.5">Group</th>
                  <th scope="col" className="th px-3 py-2.5">Endpoint</th>
                  <th
                    scope="col"
                    className="th px-3 py-2.5 text-right"
                    title={
                      anyEstimated
                        ? 'Some calls were counted from the bytes sent, as about a quarter of a token each.'
                        : undefined
                    }
                  >
                    Tokens in
                  </th>
                  <th scope="col" className="th px-3 py-2.5 text-right">Tokens out</th>
                  <th scope="col" className="th px-3 py-2.5 text-right">Calls</th>
                </tr>
              </thead>
              <tbody>
                {groups.length === 0 && (
                  <tr>
                    <td className="px-5 py-6" colSpan={5}>
                      <p className="font-medium">No model call this month.</p>
                      <p className="text-text-mute mt-1 max-w-2xl">
                        A call is counted when Ask or an agent uses an endpoint; the totals cover
                        the calendar month (UTC).
                      </p>
                    </td>
                  </tr>
                )}
                {groups.map((row) => (
                  <UsageRow key={`${row.group}-${row.endpoint}`} row={row} />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="space-y-3">
        <div className="flex flex-wrap items-end justify-between gap-3">
          <h2 className="text-lg font-semibold tracking-tight">Last calls</h2>
          {usage.isSuccess && (
            <div>
              <label htmlFor="models-limit" className="label block mb-1.5">Show</label>
              <select
                id="models-limit"
                className="input !w-auto"
                value={limit}
                onChange={(event) => setLimit(Number(event.target.value))}
              >
                {LIMITS.map((value) => (
                  <option key={value} value={value}>{value}</option>
                ))}
              </select>
            </div>
          )}
        </div>

        {usage.isSuccess && (
          <div className="card relative overflow-x-auto">
            <table className="w-full min-w-[54rem] text-sm">
              <caption className="sr-only">The last model calls, newest first</caption>
              <thead className="bg-bg-soft/50">
                <tr>
                  <th scope="col" className="th px-5 py-2.5">When</th>
                  <th scope="col" className="th px-3 py-2.5">Person</th>
                  <th scope="col" className="th px-3 py-2.5">Group</th>
                  <th scope="col" className="th px-3 py-2.5">Feature</th>
                  <th scope="col" className="th px-3 py-2.5">Endpoint</th>
                  <th scope="col" className="th px-3 py-2.5">Model</th>
                  <th scope="col" className="th px-3 py-2.5 text-right">Tokens</th>
                  <th scope="col" className="th px-3 py-2.5 text-right">Duration</th>
                  <th scope="col" className="th px-3 py-2.5">Outcome</th>
                </tr>
              </thead>
              <tbody>
                {calls.length === 0 && (
                  <tr>
                    <td className="px-5 py-6" colSpan={9}>
                      <p className="font-medium">No model call yet.</p>
                      <p className="text-text-mute mt-1 max-w-2xl">
                        Every call of an endpoint is listed here: who, for which group, what it was
                        for and how it went. Nova keeps the sizes, never the conversation.
                      </p>
                    </td>
                  </tr>
                )}
                {calls.map((call) => (
                  <CallRow key={`${call.atMs}-${call.sub}-${call.feature}-${call.endpoint}`} call={call} />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  )
}

/** One connected endpoint: where its prompts go, what the probe found, and the button. */
function EndpointRow({
  endpoint,
  disabled,
  busy,
  onProbe,
}: {
  endpoint: ModelEndpoint
  disabled: boolean
  busy: boolean
  onProbe: () => void
}) {
  const word = probeWord(endpoint)
  const checked = checkedLabel(endpoint.checkedAt)
  const bad = endpoint.reachable === false
  return (
    <tr className="border-t border-border/70">
      <td className="td px-5 py-3 font-mono text-xs break-words">{endpoint.name}</td>
      <td className="td px-3 py-3 break-words">{endpoint.model}</td>
      <td className="td px-3 py-3 text-text-dim whitespace-nowrap">
        {wherePromptsGo(endpoint.location)}
      </td>
      <td className="td px-3 py-3 text-text-dim">
        {endpoint.purposes.length > 0 ? endpoint.purposes.join(', ') : '—'}
      </td>
      <td className="td px-3 py-3">
        <span className={bad ? 'text-bad whitespace-nowrap' : 'whitespace-nowrap'}>{word}</span>
        {checked && <span className="text-text-mute text-xs"> · checked {checked}</span>}
        {endpoint.message && (
          <p className="text-text-mute text-xs mt-0.5 max-w-md break-words">{endpoint.message}</p>
        )}
      </td>
      <td className="td px-3 py-3 text-right">
        <button
          type="button"
          className="btn-ghost px-2.5 py-1 text-xs"
          disabled={disabled}
          onClick={onProbe}
        >
          {busy ? 'Probing…' : 'Probe now'}
        </button>
      </td>
    </tr>
  )
}

/** One group and endpoint of the month: the tokens, the calls, and "about" when estimated. */
function UsageRow({ row }: { row: ModelUsageRow }) {
  return (
    <tr className="border-t border-border/70">
      <td className="td px-5 py-3 break-words">{row.group}</td>
      <td className="td px-3 py-3 font-mono text-xs break-words">{row.endpoint}</td>
      <td className="td px-3 py-3 text-right whitespace-nowrap">
        {row.tokensIn.toLocaleString('en-US')}
        {row.estimatedCalls > 0 && <span className="text-text-mute text-xs"> about</span>}
      </td>
      <td className="td px-3 py-3 text-right whitespace-nowrap">
        {row.tokensOut.toLocaleString('en-US')}
      </td>
      <td className="td px-3 py-3 text-right text-text-dim whitespace-nowrap">
        {row.calls.toLocaleString('en-US')}
      </td>
    </tr>
  )
}

/** One recorded call, in the agent-audit shape: when, who, for which group, and how it went. */
function CallRow({ call }: { call: ModelUsageCall }) {
  const bad = call.outcome !== 'ok'
  return (
    <tr className="border-t border-border/70">
      <td className="td px-5 py-3 text-text-dim whitespace-nowrap" title={formatTime(call.atMs)}>
        {relativeTime(call.atMs)}
      </td>
      <td className="td px-3 py-3 font-mono text-xs break-words">{call.sub}</td>
      <td className="td px-3 py-3 text-text-dim break-words">
        {call.group ?? <span className="text-text-mute">—</span>}
      </td>
      <td className="td px-3 py-3 text-text-dim break-words">{call.feature}</td>
      <td className="td px-3 py-3 font-mono text-xs break-words">{call.endpoint}</td>
      <td className="td px-3 py-3 text-text-dim break-words">{call.model}</td>
      <td className="td px-3 py-3 text-right text-text-dim whitespace-nowrap">
        {call.tokensIn.toLocaleString('en-US')} in · {call.tokensOut.toLocaleString('en-US')} out
        {call.tokensEstimated && <span className="text-text-mute text-xs"> about</span>}
      </td>
      <td className="td px-3 py-3 text-right text-text-dim whitespace-nowrap">
        {formatDurationMs(call.latencyMs)}
      </td>
      <td className={bad ? 'td px-3 py-3 text-bad whitespace-nowrap' : 'td px-3 py-3 whitespace-nowrap'}>
        {call.outcome}
      </td>
    </tr>
  )
}
