import { useQuery } from '@tanstack/react-query'
import { api, errorMessage, type UsagePage } from '../api'
import { EmptyState } from '../components/EmptyState'
import { formatBytes, formatCount, formatDurationMs } from '../format'

/** Usage: who used the most compute, and how it adds up per day — the query log's second view. */

/** `Mon 16 Nov` for a UTC day key such as `2026-11-16`. */
function dayLabel(key: string): string {
  return new Date(`${key}T00:00:00Z`).toLocaleDateString('en-GB', {
    weekday: 'short',
    day: 'numeric',
    month: 'short',
    timeZone: 'UTC',
  })
}

/** `Mon 16`, the label under a day's bar — the day key is UTC, so the label is too. */
function dayShort(key: string): string {
  return new Date(`${key}T00:00:00Z`).toLocaleDateString('en-GB', {
    weekday: 'short',
    day: 'numeric',
    timeZone: 'UTC',
  })
}

/**
 * CPU time at the scale of the window: seconds are noise in a usage table, so a minute or
 * more loses them and an hour or more loses the minutes.
 */
function cpuText(ms: number): string {
  if (ms < 60_000) return formatDurationMs(ms)
  if (ms < 3_600_000) return `${Math.round(ms / 60_000)} m`
  return `${(ms / 3_600_000).toFixed(1)} h`
}

export function QueryUsage({ scope, fromMs }: { scope: 'mine' | 'team' | 'all'; fromMs: number }) {
  const by = scope === 'mine' ? 'warehouse' : 'principal'
  const usage = useQuery({
    queryKey: ['query-usage', scope, by, fromMs],
    queryFn: () => api.queryUsage({ scope, by, from: fromMs }),
    refetchInterval: 60_000,
  })
  const days = useQuery({
    queryKey: ['query-usage', scope, 'day', fromMs],
    queryFn: () => api.queryUsage({ scope, by: 'day', from: fromMs }),
    refetchInterval: 60_000,
  })

  if (usage.isError) {
    return (
      <div className="text-bad text-sm" role="alert">
        {errorMessage(usage.error)}{' '}
        <button type="button" className="underline" onClick={() => usage.refetch()}>Retry</button>
      </div>
    )
  }
  if (usage.isLoading || !usage.data) return <div className="text-text-mute text-sm">loading…</div>

  const page: UsagePage = usage.data
  if (page.items.length === 0) {
    return <EmptyState icon="search" title="No queries in this window.">
      Widen the time range, or switch the scope.
    </EmptyState>
  }

  const maxCpu = Math.max(...page.items.map(r => r.cpuMs), 1)
  const perDay = days.data?.items ?? []
  const maxDay = Math.max(...perDay.map(r => r.cpuMs), 1)
  const total = page.total

  return (
    <div className="space-y-5">
      <p className="text-sm text-text-dim" aria-live="polite">
        {formatCount(total.queries)} queries · {cpuText(total.cpuMs)} CPU ·{' '}
        {formatBytes(total.inputBytes)} read · {formatCount(total.failed)} failed
      </p>

      <h2 className="text-sm font-semibold">{scope === 'mine' ? 'By warehouse' : 'Who used the most compute'}</h2>

      <div className="card overflow-x-auto">
        <table className="w-full text-sm">
          <caption className="sr-only">{scope === 'mine' ? 'By warehouse' : 'Who used the most compute'}, heaviest first.</caption>
          <thead className="bg-bg-soft/50">
            <tr>
              <th className="th px-3 py-2.5">{scope === 'mine' ? 'Warehouse' : 'Person'}</th>
              <th className="th px-3 py-2.5 text-right">Queries</th>
              <th className="th px-3 py-2.5">CPU time</th>
              <th className="th px-3 py-2.5 text-right">Data read</th>
              <th className="th px-3 py-2.5 text-right hidden sm:table-cell">Data written</th>
              <th className="th px-3 py-2.5 text-right hidden sm:table-cell">Failed</th>
            </tr>
          </thead>
          <tbody>
            {page.items.map(row => (
              <tr key={row.key} className="border-t border-border/70 hover:bg-bg-soft/30">
                <td className="td px-3 whitespace-nowrap" title={row.key}>{row.key}</td>
                <td className="td px-3 text-right text-text-dim whitespace-nowrap">{formatCount(row.queries)}</td>
                <td className="td px-3 whitespace-nowrap">
                  <div
                    aria-hidden
                    className="inline-block h-2 rounded bg-accent/60 mr-2 align-middle"
                    style={{ width: `${Math.max(2, Math.round(80 * row.cpuMs / maxCpu))}px` }}
                  />
                  <span className="text-text-dim">{cpuText(row.cpuMs)}</span>
                </td>
                <td className="td px-3 text-right text-text-dim whitespace-nowrap">{formatBytes(row.inputBytes)}</td>
                <td className="td px-3 text-right text-text-dim whitespace-nowrap hidden sm:table-cell">{formatBytes(row.writtenBytes)}</td>
                <td className="td px-3 text-right text-text-dim whitespace-nowrap hidden sm:table-cell">{formatCount(row.failed)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <section>
        <h2 className="text-sm font-semibold mb-2">CPU time per day</h2>
        {/* No height on the row: the label became part of each column, so the bar area
            inside it is what carries the fixed `h-24` — a height here would overflow the
            tallest bar over the heading above. */}
        <div className="flex items-end gap-2">
          {perDay.map(row => {
            const title = `${dayLabel(row.key)} · ${cpuText(row.cpuMs)} CPU · ${formatCount(row.queries)} queries`
            return (
              <div key={row.key} className="flex flex-col items-center gap-1">
                <div className="h-24 flex items-end">
                  <div
                    role="img"
                    aria-label={title}
                    title={title}
                    className="w-7 shrink-0 rounded-t bg-accent/60"
                    style={{ height: `${Math.max(4, Math.round(100 * row.cpuMs / maxDay))}%` }}
                  />
                </div>
                <span className="text-[11px] text-text-mute">{dayShort(row.key)}</span>
              </div>
            )
          })}
        </div>
      </section>
    </div>
  )
}
