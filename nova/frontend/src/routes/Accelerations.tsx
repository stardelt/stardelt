import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api'
import ErrorBox from '../components/ErrorBox'
import { formatCount, formatDurationMs } from '../format'

/**
 * Accelerations (#38 slice 2): the statements this person (or their teams) runs again and
 * again, worst first, with the one supported way to make them cheap — a table refreshed by
 * a scheduled `sql` task. Nothing is created here.
 */
export default function Accelerations() {
  const [scope, setScope] = useState<'mine' | 'team'>('mine')
  const [copied, setCopied] = useState<string | null>(null)
  const list = useQuery({
    queryKey: ['repeated-queries', scope],
    queryFn: () => api.repeatedQueries(scope),
  })
  const statements = list.data?.statements ?? []

  async function copy(row: { sqlFingerprint: string; sql: string }) {
    try {
      await navigator.clipboard.writeText(row.sql)
      setCopied(row.sqlFingerprint)
      window.setTimeout(() => setCopied(null), 2000)
    } catch {
      setCopied(null)
    }
  }

  return (
    <div className="space-y-5">
      <header className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <h1 className="text-2xl font-bold tracking-tight">Accelerations</h1>
          <p className="mt-1 text-sm text-text-dim">
            Statements you repeat, worst first. A table refreshed by a scheduled pipeline
            makes the expensive ones cheap.
          </p>
        </div>
        <label className="flex items-center gap-2 text-sm text-text-dim">
          Scope
          <select
            className="input"
            value={scope}
            onChange={(event) => setScope(event.target.value === 'team' ? 'team' : 'mine')}
          >
            <option value="mine">Mine</option>
            <option value="team">My teams</option>
          </select>
        </label>
      </header>

      {list.isLoading && !list.data && <p className="text-sm text-text-mute">Loading…</p>}
      {list.isError && <ErrorBox error={list.error} onRetry={() => void list.refetch()} />}

      {list.data && (
        <p className="card p-4 text-sm text-text-dim">{list.data.suggestion}</p>
      )}

      {list.data && statements.length === 0 && (
        <p className="text-sm text-text-dim">
          Nothing repeated yet: a statement has to finish at least twice in the last 30 days.
        </p>
      )}

      {statements.length > 0 && (
        <div className="card p-5 overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-text-mute">
                <th className="py-1 pr-3 font-medium">Statement</th>
                <th className="py-1 pr-3 font-medium text-right">Runs</th>
                <th className="py-1 pr-3 font-medium text-right">Total</th>
                <th className="py-1 pr-3 font-medium text-right">Average</th>
                <th className="py-1 font-medium" />
              </tr>
            </thead>
            <tbody>
              {statements.map((row) => (
                <tr key={row.sqlFingerprint} className="border-t border-border align-top">
                  <td className="py-2 pr-3">
                    <code className="block max-w-xl whitespace-pre-wrap break-words text-xs text-text-dim">
                      {row.sql}
                    </code>
                  </td>
                  <td className="py-2 pr-3 text-right whitespace-nowrap">{formatCount(row.runs)}</td>
                  <td className="py-2 pr-3 text-right whitespace-nowrap">{formatDurationMs(row.totalMs)}</td>
                  <td className="py-2 pr-3 text-right whitespace-nowrap">{formatDurationMs(row.averageMs)}</td>
                  <td className="py-2 text-right">
                    <button
                      type="button"
                      className="btn-ghost px-2 py-1 text-xs"
                      onClick={() => void copy(row)}
                    >
                      {copied === row.sqlFingerprint ? 'Copied.' : 'Copy SQL'}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
