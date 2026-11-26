import { useQueries } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api, type ListedHealth } from '../api'
import { relativeTime } from '../format'
import ErrorBox from './ErrorBox'
import { EmptyState } from './EmptyState'
import { useCatalogs } from './CatalogSelect'
import { Skeleton } from './Skeleton'

/** The five promised tables that are late, worst first, across the catalogs a person may browse. */
const SHOWN = 5

/** The table page's words, so the two screens cannot drift. */
const WORD: Record<string, string> = { red: 'Late', amber: 'Late (grace)' }

function rank(status: string): number {
  return status === 'red' ? 0 : status === 'amber' ? 1 : 2
}

export default function TablesAtRisk() {
  const catalogs = useCatalogs()
  const health = useQueries({
    queries: (catalogs.data?.catalogs ?? []).map((catalog) => ({
      queryKey: ['catalog-health', catalog.name],
      queryFn: () => api.catalogHealth(catalog.name),
      staleTime: 60_000,
      retry: false,
    })),
  })
  const rows: ListedHealth[] = health
    .flatMap((answer) => answer.data?.tables ?? [])
    .filter((row) => row.status === 'red' || row.status === 'amber')
    .sort(
      (a, b) =>
        rank(a.status) - rank(b.status) ||
        `${a.namespace}.${a.name}`.localeCompare(`${b.namespace}.${b.name}`),
    )
    .slice(0, SHOWN)
  const loading = catalogs.isLoading || health.some((answer) => answer.isLoading)
  const failed = health.length > 0 && health.every((answer) => answer.isError)
  const promised = health.some((answer) => (answer.data?.tables.length ?? 0) > 0)

  return (
    <section className="card p-5 min-w-0" aria-labelledby="tables-at-risk-title">
      <div className="flex items-baseline justify-between gap-2">
        <h2 id="tables-at-risk-title" className="text-sm font-semibold">Tables at risk</h2>
        <span className="text-xs text-text-mute">Promised tables that are late</span>
      </div>
      {failed && (
        <div className="mt-3">
          <ErrorBox
            error={health.find((answer) => answer.isError)?.error}
            onRetry={() => health.forEach((answer) => void answer.refetch())}
          />
        </div>
      )}
      {!failed && loading && (
        <div className="mt-3 space-y-2">
          {[0, 1, 2].map((i) => <Skeleton key={i} className="h-6" />)}
        </div>
      )}
      {!failed && !loading && rows.length === 0 && (
        <EmptyState icon="clock" title={promised ? 'Every promised table is fresh' : 'No promised tables yet'}>
          {promised
            ? 'A table with a freshness promise appears here when it is late.'
            : 'Open a table and set its freshness promise to see it here when it is late.'}
        </EmptyState>
      )}
      {rows.length > 0 && (
        <ul className="mt-3 divide-y divide-border">
          {rows.map((row) => {
            const qs = new URLSearchParams({ catalog: row.catalog, ns: row.namespace, tbl: row.name })
            const wrote = Date.parse(row.lastWrite ?? '')
            return (
              <li key={`${row.catalog}.${row.namespace}.${row.name}`}>
                <Link to={`/catalog?${qs.toString()}`} className="block py-2 hover:text-text">
                  <span className="flex items-center justify-between gap-3">
                    <span className="font-mono text-sm truncate">{row.namespace}.{row.name}</span>
                    <span className={`text-xs shrink-0 ${row.status === 'red' ? 'text-bad' : 'text-warn'}`}>
                      {WORD[row.status] ?? row.status}
                    </span>
                  </span>
                  <span className="block text-xs text-text-mute break-words">
                    {row.detail}
                    {Number.isFinite(wrote) ? ` · last write ${relativeTime(wrote)}` : ''}
                  </span>
                </Link>
              </li>
            )
          })}
        </ul>
      )}
    </section>
  )
}
