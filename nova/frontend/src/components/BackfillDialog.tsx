import { useEffect, useId, useRef, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { api, errorMessage, type BackfillStart, type BackfillView, type PipelineDetail } from '../api'

/** A `type="date"` value for a day relative to the person's own clock. */
function daysAgo(n: number): string {
  const then = new Date()
  then.setDate(then.getDate() - n)
  const pad = (value: number) => String(value).padStart(2, '0')
  return `${then.getFullYear()}-${pad(then.getMonth() + 1)}-${pad(then.getDate())}`
}

function yesterday(): string {
  return daysAgo(1)
}

/**
 * The Run for a range form of the pipeline page (#117): an inclusive range of days, how to treat
 * dates that already ran and the pipeline's declared parameter values.
 *
 * A native `<dialog>` opened with `showModal()` like the Run now dialog: the browser traps focus,
 * makes the page behind it inert and closes it on Escape. Focus starts on Cancel, and the API's
 * own refusal sentence is shown here, not paraphrased.
 */
export default function BackfillDialog({
  pipeline,
  onClose,
  onStarted,
}: {
  pipeline: PipelineDetail
  onClose: () => void
  onStarted: (view: BackfillView) => void
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const opener = useRef<HTMLElement | null>(null)
  const id = useId()
  const titleId = `${id}-title`
  const declared = Object.entries(pipeline.params ?? {}).sort(([a], [b]) => a.localeCompare(b))
  const [fromDate, setFromDate] = useState(() => daysAgo(7))
  const [toDate, setToDate] = useState(yesterday)
  const [reprocess, setReprocess] = useState<'none' | 'failed' | 'completed'>('none')
  const [values, setValues] = useState<Record<string, string>>(() =>
    Object.fromEntries(declared.map(([name, value]) => [name, value])),
  )
  const [localError, setLocalError] = useState<string | null>(null)

  const start = useMutation({
    mutationFn: (body: BackfillStart) => api.startBackfill(pipeline.namespace, pipeline.name, body),
    onSuccess: onStarted,
  })

  useEffect(() => {
    const dialog = ref.current
    if (!dialog) return
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null
    if (!dialog.open) dialog.showModal()
    // Cancel is focused first: Return does not start a backfill by accident.
    dialog.querySelector<HTMLElement>('[data-autofocus]')?.focus()
    return () => {
      if (dialog.open) dialog.close()
      opener.current?.focus()
    }
  }, [])

  const error = localError ?? (start.error ? errorMessage(start.error) : null)

  return (
    <dialog
      ref={ref}
      aria-labelledby={titleId}
      onCancel={(event) => {
        event.preventDefault()
        onClose()
      }}
      className="m-auto w-[min(32rem,calc(100vw_-_2rem))] max-w-none rounded-[14px] border border-border bg-bg-elev p-0 text-text shadow-2xl backdrop:bg-black/60"
    >
      <form
        className="flex max-h-[85vh] flex-col"
        onSubmit={(event) => {
          event.preventDefault()
          if (start.isPending) return
          if (fromDate > toDate) {
            setLocalError('the first date must not be after the last date')
            return
          }
          setLocalError(null)
          start.mutate({ fromDate, toDate, reprocess, params: { ...values } })
        }}
      >
        <div className="px-5 pt-5">
          <h2 id={titleId} className="text-lg font-semibold">Run {pipeline.name} for a range</h2>
        </div>
        <div className="min-h-0 space-y-3 overflow-y-auto px-5 py-4 text-sm">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <div className="space-y-1">
              <label htmlFor={`${id}-from`} className="block text-xs font-medium text-text-dim">
                First date
              </label>
              <input
                id={`${id}-from`}
                type="date"
                className="input w-full"
                value={fromDate}
                onChange={(event) => setFromDate(event.target.value)}
              />
            </div>
            <div className="space-y-1">
              <label htmlFor={`${id}-to`} className="block text-xs font-medium text-text-dim">
                Last date
              </label>
              <input
                id={`${id}-to`}
                type="date"
                className="input w-full"
                value={toDate}
                onChange={(event) => setToDate(event.target.value)}
              />
            </div>
          </div>
          <p className="text-text-dim">
            At most 31 dates; dates that already ran are skipped unless you ask for them.
          </p>
          <div className="space-y-1">
            <label htmlFor={`${id}-reprocess`} className="block text-xs font-medium text-text-dim">
              Dates that already ran
            </label>
            <select
              id={`${id}-reprocess`}
              className="input w-full"
              value={reprocess}
              onChange={(event) => setReprocess(event.target.value as 'none' | 'failed' | 'completed')}
            >
              <option value="none">Skip them</option>
              <option value="failed">Redo failed</option>
              <option value="completed">Redo all</option>
            </select>
          </div>
          {declared.length > 0 && (
            <div className="space-y-2">
              {declared.map(([name]) => (
                <div key={name} className="space-y-1">
                  <label htmlFor={`${id}-${name}`} className="block font-mono text-xs text-text-dim">
                    {name}
                  </label>
                  <input
                    id={`${id}-${name}`}
                    type="text"
                    className="input w-full font-mono text-xs"
                    value={values[name] ?? ''}
                    onChange={(event) => setValues((old) => ({ ...old, [name]: event.target.value }))}
                  />
                </div>
              ))}
            </div>
          )}
          {error && (
            <div
              role="alert"
              className="rounded-[10px] border border-red-500/40 bg-red-500/10 p-3 text-sm text-bad break-words"
            >
              {error}
            </div>
          )}
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-border px-5 py-3">
          <button type="submit" className="btn-primary" disabled={start.isPending}>
            {start.isPending ? 'Starting…' : 'Start backfill'}
          </button>
          <button type="button" className="btn-ghost" onClick={onClose} data-autofocus>
            Cancel
          </button>
        </div>
      </form>
    </dialog>
  )
}
