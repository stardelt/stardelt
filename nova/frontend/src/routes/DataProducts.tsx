import { useQuery } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import { api, type DataProduct, type ListedHealth } from '../api'
import CatalogSelect from '../components/CatalogSelect'
import ErrorBox from '../components/ErrorBox'

/** The badge per state: published is the goal, deprecated is a warning. */
const BADGE: Record<string, string> = {
  published: 'badge border-accent/40 bg-accent/10 text-accent',
  draft: 'badge border-border bg-bg-soft text-text-dim',
  deprecated: 'badge border-amber-400/40 bg-amber-400/10 text-warn',
}

/** The table page's words and ink (`TableFreshness.tsx`), so the two screens cannot drift. */
const FRESH: Record<string, { word: string; ink: string }> = {
  green: { word: 'Fresh', ink: 'border-ok/40 bg-ok/10 text-ok' },
  amber: { word: 'Late (grace)', ink: 'border-warn/40 bg-warn/10 text-warn' },
  red: { word: 'Late', ink: 'border-warn/40 bg-warn/10 text-warn' },
}

const byDomain = (a: DataProduct, b: DataProduct) => {
  const left = a.domain ?? ''
  const right = b.domain ?? ''
  // Tables without a domain last, exactly as the API orders them.
  if (left !== right) return left === '' ? 1 : right === '' ? -1 : left.localeCompare(right)
  return `${a.namespace}.${a.table}`.localeCompare(`${b.namespace}.${b.table}`)
}

/**
 * Data products (#37 slice 4): every table of a catalog that carries a lifecycle, grouped
 * by domain. The table page publishes one (#2029); this page only lists.
 */
export default function DataProducts() {
  const [params] = useSearchParams()
  const catalog = params.get('catalog') ?? undefined
  const list = useQuery({
    queryKey: ['data-products', catalog ?? ''],
    queryFn: () => api.dataProducts(catalog),
  })
  const products = [...(list.data?.products ?? [])].sort(byDomain)
  const domains = [...new Set(products.map((p) => p.domain ?? ''))]
  // Freshness (#37 slice 6): the promise the table's owner made, from the same route the
  // Overview and the table page read. A catalog with no promised table answers an empty list.
  const health = useQuery({
    queryKey: ['catalog-health', catalog ?? ''],
    queryFn: () => api.catalogHealth(catalog),
    staleTime: 60_000,
    retry: false,
  })
  const fresh = new Map<string, ListedHealth>()
  for (const row of health.data?.tables ?? []) fresh.set(`${row.namespace}.${row.name}`, row)

  return (
    <div className="space-y-6">
      <header className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <h1 className="text-2xl font-bold tracking-tight">Data products</h1>
          <p className="mt-1 text-sm text-text-dim">
            The tables this catalog publishes, by domain. Set a table&apos;s lifecycle on its
            table page to publish it, mark it a draft or deprecate it with a replacement.
          </p>
        </div>
        <CatalogSelect />
      </header>

      {list.isLoading && !list.data && <p className="text-sm text-text-mute">Loading…</p>}
      {list.isError && <ErrorBox error={list.error} onRetry={() => void list.refetch()} />}
      {list.data && products.length === 0 && (
        <p className="text-sm text-text-dim">
          No data products yet. Open a table and set <strong>Data product</strong> to{' '}
          <em>draft</em> or <em>published</em>.
        </p>
      )}

      {domains.map((domain) => (
        <section key={domain || '(no domain)'} className="card p-5 min-w-0">
          <h2 className="label">{domain === '' ? 'No domain' : domain}</h2>
          <table className="mt-3 w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-text-mute">
                <th className="py-1 pr-3 font-medium">Table</th>
                <th className="py-1 pr-3 font-medium">Description</th>
                <th className="py-1 pr-3 font-medium">Owner</th>
                <th className="py-1 pr-3 font-medium">Freshness</th>
                <th
                  className="py-1 pr-3 font-medium"
                  title="Distinct people and services that read this table in the last 30 days"
                >
                  Readers
                </th>
                <th className="py-1 pr-3 font-medium">State</th>
              </tr>
            </thead>
            <tbody>
              {products
                .filter((p) => (p.domain ?? '') === domain)
                .map((p) => (
                  <tr key={`${p.namespace}.${p.table}`} className="border-t border-border">
                    <td className="py-2 pr-3 align-top">
                      <Link
                        className="text-accent hover:underline break-all"
                        to={`/catalog?catalog=${encodeURIComponent(list.data?.catalog ?? '')}&ns=${encodeURIComponent(p.namespace)}&tbl=${encodeURIComponent(p.table)}`}
                      >
                        {p.namespace}.{p.table}
                      </Link>
                    </td>
                    <td className="py-2 pr-3 align-top text-text-dim break-words">
                      {p.description ?? <span className="text-text-mute">No description</span>}
                    </td>
                    <td className="py-2 pr-3 align-top text-text-dim">{p.owner ?? '—'}</td>
                    <td className="py-2 pr-3 align-top">
                      {(() => {
                        const h = fresh.get(`${p.namespace}.${p.table}`)
                        if (!h) return <span className="text-text-mute">—</span>
                        const badge = FRESH[h.status] ?? FRESH.red
                        return (
                          <span className={`badge ${badge.ink}`} title={h.detail}>
                            {badge.word}
                          </span>
                        )
                      })()}
                    </td>
                    <td className="py-2 pr-3 align-top text-text-dim">{p.readers}</td>
                    <td className="py-2 pr-3 align-top">
                      <span className={BADGE[p.lifecycle] ?? BADGE.draft}>{p.lifecycle}</span>
                      {p.lifecycle === 'deprecated' && p.replacedBy && (
                        <div className="mt-1 text-xs text-text-mute break-all">
                          replaced by {p.replacedBy}
                        </div>
                      )}
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        </section>
      ))}
    </div>
  )
}
