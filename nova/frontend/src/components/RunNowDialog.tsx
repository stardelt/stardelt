import { useEffect, useId, useRef, useState } from 'react'
import type { PipelineDetail, RunStart } from '../api'

/** A `type="date"` value for the person's own today: `toISOString()` would name another day. */
function localToday(): string {
  const now = new Date()
  const pad = (value: number) => String(value).padStart(2, '0')
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`
}

/**
 * The Run now form of the pipeline page (nova#817): a run date and the pipeline's declared
 * parameter values for this run only.
 *
 * A native `<dialog>` opened with `showModal()` like the stop dialog: the browser traps
 * focus, makes the page behind it inert and closes it on Escape. Focus starts on Cancel,
 * returns to the button that opened it when it closes, and Run now sends exactly one
 * request. The API's own refusal sentence is shown here, not paraphrased.
 */
export default function RunNowDialog({
  pipeline,
  onClose,
  onRun,
  pending,
  error,
}: {
  pipeline: PipelineDetail
  onClose: () => void
  onRun: (start: RunStart) => void
  pending: boolean
  /** The API's sentence when the run was refused; shown inside the dialog. */
  error?: string | null
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const opener = useRef<HTMLElement | null>(null)
  const id = useId()
  const titleId = `${id}-title`
  const declared = Object.entries(pipeline.params ?? {}).sort(([a], [b]) => a.localeCompare(b))
  const [runDate, setRunDate] = useState(localToday)
  const [values, setValues] = useState<Record<string, string>>(() =>
    Object.fromEntries(declared.map(([name, value]) => [name, value])),
  )

  useEffect(() => {
    const dialog = ref.current
    if (!dialog) return
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null
    if (!dialog.open) dialog.showModal()
    // Cancel is focused first: Return does not start a run by accident.
    dialog.querySelector<HTMLElement>('[data-autofocus]')?.focus()
    return () => {
      if (dialog.open) dialog.close()
      opener.current?.focus()
    }
  }, [])

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
          if (pending) return
          onRun({ runDate, params: { ...values } })
        }}
      >
        <div className="px-5 pt-5">
          <h2 id={titleId} className="text-lg font-semibold">Run {pipeline.name}</h2>
        </div>
        <div className="min-h-0 space-y-3 overflow-y-auto px-5 py-4 text-sm">
          <div className="space-y-1">
            <label htmlFor={`${id}-date`} className="block text-xs font-medium text-text-dim">
              Run for
            </label>
            <input
              id={`${id}-date`}
              type="date"
              className="input w-full"
              value={runDate}
              onChange={(event) => setRunDate(event.target.value)}
            />
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
          <p className="text-text-dim">Values apply to this run only. The schedule does not change.</p>
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
          <button type="submit" className="btn-primary" disabled={pending}>
            {pending ? 'Starting…' : 'Run now'}
          </button>
          <button type="button" className="btn-ghost" onClick={onClose} data-autofocus>
            Cancel
          </button>
        </div>
      </form>
    </dialog>
  )
}
