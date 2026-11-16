import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type SqlWarehouse } from '../api'

/**
 * The SQL warehouse — the Trino compute class — a person uses, shown on the SQL editor and
 * on the Profile page (stardelt-nova#236, warehouses 3/3). Nova is not the authority: the
 * engine picks the class from the identity (the first selector the person's groups match,
 * largest class first), and `GET /api/warehouses` repeats that pick as `yours`.
 *
 * This is **not** `me.warehouse`: that is the default *catalog* (the Iceberg REST warehouse)
 * a query addresses, a different thing with the same word in it.
 */

/** `Warehouse: <displayName or name> (<size>)` — the one line both pages show. */
export function warehouseText(warehouse: SqlWarehouse): string {
  const label = warehouse.displayName || warehouse.name
  // The administrator row has no size (it is the `admin` root group, not a class). A class
  // without a display name would otherwise read twice: "small (small)".
  if (!warehouse.size || warehouse.size.toLowerCase() === label.toLowerCase()) return label
  return `${label} (${warehouse.size})`
}

/** What the class allows, for the `title` tooltip: the numbers the resource group renders with. */
export function warehouseHint(warehouse: SqlWarehouse): string {
  return `${warehouse.running} running, ${warehouse.queued} queued, ${warehouse.memoryShare} memory`
}

/**
 * The class this person lands in: the `yours` row of `GET /api/warehouses`. `undefined`
 * while the answer is on its way, when the list is empty, when no row is `yours`, and when
 * the answer is not a list at all (a proxy or a test stub can return anything) — callers
 * render nothing then. The navigation card shows it on every page, so it must not throw.
 *
 * While the scaler reports anything but `Running` it reads again every 15 s: the engine
 * comes up (or stops) on the scaler's two-minute tick and the line moves on by itself.
 */
export function useYourWarehouse(): SqlWarehouse | undefined {
  const { data } = useQuery({
    queryKey: ['warehouses'],
    queryFn: api.warehouses,
    refetchInterval: (q) =>
      q.state.data?.some?.(w => w.yours && w.scale && w.scale.phase !== 'Running') ? 15_000 : false,
  })
  return Array.isArray(data) ? data.find(warehouse => warehouse.yours) : undefined
}

/**
 * `Warehouse: Small`, next to the Run button. Nothing while the list is empty. When the
 * scaler says the shared engine is stopped, the line says so and offers **Start now**
 * (#1059): the first query would otherwise wait up to about three minutes.
 */
export default function WarehouseNote({ className }: { className?: string }) {
  const warehouse = useYourWarehouse()
  const qc = useQueryClient()
  const start = useMutation({
    mutationFn: api.warehouseStart,
    // The scaler writes `status.scale`, not this call: reading again shows the new word
    // once the scaler has run.
    onSuccess: () => qc.invalidateQueries({ queryKey: ['warehouses'] }),
  })
  if (!warehouse) return null
  const phase = warehouse.scale?.phase
  return (
    <span className={className} title={warehouseHint(warehouse)}>
      Warehouse: {warehouseText(warehouse)}
      {phase === 'Stopped' && (
        <>
          {' · Stopped (starts on the first query)'}
          <button
            type="button"
            className="text-xs ml-1 underline disabled:no-underline disabled:text-text-mute"
            disabled={start.isPending}
            onClick={() => start.mutate(warehouse.name)}
          >
            {start.isPending ? 'Starting…' : 'Start now'}
          </button>
        </>
      )}
      {phase === 'Starting' && ' · Starting — this can take up to 3 minutes'}
    </span>
  )
}
