import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { api, type CatalogMetrics } from '../api'
import { formatBytes, formatCount } from '../format'

/**
 * Storage per catalog, on the Usage view (stardelt-platform#138 slice 3): each catalog's
 * size as its own Iceberg metadata reports it (`GET /api/catalog/metrics`), not the object
 * store's numbers. Rendered only when the request answers: the view is for everyone, and a
 * caller with no catalogs gets a 403.
 */
export function StorageByCatalog() {
  const metrics = useQuery({
    queryKey: ['catalog-metrics'],
    queryFn: () => api.catalogMetrics(),
    refetchInterval: 5 * 60_000,
  })
  const page: CatalogMetrics | undefined = metrics.data
  // The other usage specs stub `**/api/**` with `{}`: a missing list is not an error here.
  if (!page || !Array.isArray(page.catalogs)) return null

  const rows = page.catalogs
    .filter(c => !c.pending && !c.error && c.sizeBytes > 0)
    .sort((a, b) => b.sizeBytes - a.sizeBytes)
  if (rows.length === 0) return null

  return (
    <section className="space-y-2">
      <h2 className="text-sm font-semibold">Storage by catalog</h2>
      <p className="text-sm text-text-dim">
        {formatBytes(page.totals.sizeBytes)} in {formatCount(page.totals.catalogs)} catalogs
      </p>

      <div className="card overflow-x-auto">
        <table className="w-full text-sm">
          <caption className="sr-only">Storage by catalog, largest first.</caption>
          <thead className="bg-bg-soft/50">
            <tr>
              <th className="th px-3 py-2.5">Catalog</th>
              <th className="th px-3 py-2.5 text-right">Tables</th>
              <th className="th px-3 py-2.5 text-right">Size</th>
              <th className="th px-3 py-2.5 text-right hidden sm:table-cell">Records</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(c => (
              <tr
                key={c.name}
                className="border-t border-border/70 hover:bg-bg-soft/30"
                title={c.truncated ? 'Size and records cover the first 200 tables.' : undefined}
              >
                <td className="td px-3 whitespace-nowrap">
                  <Link to={`/catalog?catalog=${encodeURIComponent(c.name)}`} className="text-accent hover:underline">
                    {c.name}
                  </Link>
                </td>
                <td className="td px-3 text-right text-text-dim whitespace-nowrap">{formatCount(c.tables)}</td>
                <td className="td px-3 text-right text-text-dim whitespace-nowrap">{formatBytes(c.sizeBytes)}</td>
                <td className="td px-3 text-right text-text-dim whitespace-nowrap hidden sm:table-cell">{formatCount(c.records)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}
