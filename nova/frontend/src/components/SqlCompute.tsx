import { useQuery } from '@tanstack/react-query'
import { api } from '../api'
import ErrorBox from './ErrorBox'
import WarehouseNote, { warehouseHint } from './WarehouseNote'

/**
 * Compute → SQL: which class this person's queries run in, and what each class allows. The
 * engine decides the pick (the first selector the person's groups match, largest class
 * first); Nova repeats it as `yours` on `GET /api/warehouses`.
 *
 * Same `['warehouses']` key as the SQL editor's `WarehouseNote`, so both read one answer.
 */

/** The `Starting`/`Running` emphasis the Platform page's table uses. */
function stateClass(phase: string | undefined): string {
  if (phase === 'Running') return 'text-success'
  if (phase === 'Starting') return 'text-warn font-semibold'
  return 'text-text-mute'
}

export default function SqlCompute() {
  const warehouses = useQuery({ queryKey: ['warehouses'], queryFn: api.warehouses })
  const rows = Array.isArray(warehouses.data) ? [...warehouses.data] : []
  // Yours first, the rest in the order the engine listed them.
  rows.sort((a, b) => Number(b.yours) - Number(a.yours))

  return (
    <div className="flex flex-col gap-4">
      <WarehouseNote className="text-sm text-text-dim" />

      {warehouses.isLoading && <div className="text-text-mute text-sm">loading…</div>}
      {warehouses.isError && (
        <ErrorBox error={warehouses.error} onRetry={() => warehouses.refetch()} />
      )}
      {warehouses.isSuccess && rows.length === 0 && (
        <p className="text-sm text-text-dim">No SQL compute classes are defined.</p>
      )}

      {rows.length > 0 && (
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-[11px] uppercase tracking-wider text-text-mute">
              <th className="py-1 pr-4 font-normal">Class</th>
              <th className="py-1 pr-4 font-normal">Size</th>
              <th className="py-1 pr-4 font-normal">Limits</th>
              <th className="py-1 pr-4 font-normal">Access</th>
              <th className="py-1 font-normal">State</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(w => (
              <tr key={w.name} className="border-t border-border/70">
                <td className="py-1.5 pr-4 font-medium">
                  {w.displayName ?? w.name}
                  {w.yours && <span className="badge ml-2">yours</span>}
                </td>
                <td className="py-1.5 pr-4">{w.size ?? '—'}</td>
                <td className="py-1.5 pr-4 font-mono text-xs text-text-dim">{warehouseHint(w)}</td>
                <td className="py-1.5 pr-4 text-text-dim">
                  {w.canUse ? 'your groups may use it' : 'not granted to your groups'}
                </td>
                <td className={`py-1.5 ${stateClass(w.scale?.phase)}`}>{w.scale?.phase ?? '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <p className="text-sm text-text-mute">
        There is one shared SQL engine; a class sets how much of it your queries may use.
      </p>
    </div>
  )
}
