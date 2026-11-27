import { useEffect, useId, useRef, useState, type FormEvent, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, errorMessage, type StreamView, type StreamWriteBody } from '../api'

/**
 * The create dialog of the Streams page (stardelt/stardelt#1977).
 *
 * It writes nothing until `POST /api/streams/validate` answers `ok`: the server's refusal
 * sentences are shown in place and the create is never sent. A native `<dialog>` opened with
 * `showModal()`, like the connection dialogs: focus trapping, Escape and the inert background
 * are the browser's job, and focus returns to the button that opened it.
 */

const NAME_RE = /^[a-z][a-z0-9-]{0,39}$/
const NAME_RULE = '1 to 40 lower-case letters, digits and dashes, starting with a letter.'
const TOPIC_RE = /^[A-Za-z0-9._-]{1,249}$/
const TOPIC_RULE = '1 to 249 letters, digits, dot, underscore or dash.'
const TABLE_RULE = 'schema.table, both parts filled in.'

/** True when the value is `schema.table` with exactly one dot and both parts filled in. */
function tableOk(value: string): boolean {
  const parts = value.trim().split('.')
  return parts.length === 2 && parts[0] !== '' && parts[1] !== ''
}

/** The comma-separated field, split; blank entries are ignored. */
function splitList(value: string): string[] {
  return value.split(',').map((entry) => entry.trim()).filter((entry) => entry !== '')
}

/** The frame the dialog shares with the connection dialogs: heading, body, visible footer. */
function DialogFrame({
  title,
  titleId,
  initialFocus,
  onClose,
  children,
  footer,
}: {
  title: string
  titleId: string
  /** A CSS selector inside the dialog; the first match gets focus on open. */
  initialFocus: string
  onClose: () => void
  children: ReactNode
  footer: ReactNode
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const opener = useRef<HTMLElement | null>(null)

  useEffect(() => {
    const dialog = ref.current
    if (!dialog) return
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null
    if (!dialog.open) dialog.showModal()
    const target = dialog.querySelector<HTMLElement>(initialFocus)
    target?.focus()
    return () => {
      if (dialog.open) dialog.close()
      opener.current?.focus()
    }
  }, [initialFocus])

  return (
    <dialog
      ref={ref}
      aria-labelledby={titleId}
      onCancel={(event) => { event.preventDefault(); onClose() }}
      className="m-auto w-[min(44rem,calc(100vw_-_2rem))] max-w-none rounded-[14px] border border-border bg-bg-elev p-0 text-text shadow-2xl backdrop:bg-black/60"
    >
      <div className="flex max-h-[85vh] flex-col">
        <div className="px-5 pt-5">
          <h2 id={titleId} className="text-lg font-semibold break-words">{title}</h2>
        </div>
        <div className="min-h-0 overflow-y-auto px-5 py-4">{children}</div>
        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-border px-5 py-3">
          {footer}
        </div>
      </div>
    </dialog>
  )
}

/** One label and its control; the hint is described, not part of the control's name. */
function Field({
  id,
  label,
  hint,
  children,
}: {
  id: string
  label: string
  hint?: string
  children: ReactNode
}) {
  return (
    <div>
      <label htmlFor={id} className="label block mb-1.5">{label}</label>
      {children}
      {hint && (
        <p id={`${id}-hint`} className="mt-1 block text-xs text-text-dim">{hint}</p>
      )}
    </div>
  )
}

export function NewStreamDialog({
  namespace,
  onClose,
  onSaved,
}: {
  namespace: string
  onClose: () => void
  onSaved: (view: StreamView) => void
}) {
  const prefix = useId()
  const titleId = `${prefix}-title`
  const formId = `${prefix}-form`

  const principals = useQuery({ queryKey: ['pipeline-principals'], queryFn: api.pipelinePrincipals })
  const catalogs = useQuery({ queryKey: ['catalogs'], queryFn: api.catalogs })
  const connections = useQuery({ queryKey: ['connections'], queryFn: api.connections })

  const groups = principals.data?.groups ?? []
  const unusable = groups.filter((group) => !group.ready && group.message)
  const catalogNames = catalogs.data?.catalogs.map((catalog) => catalog.name) ?? []
  const kafkaConnections = (connections.data?.connections ?? []).filter((c) => c.type === 'kafka')

  const [name, setName] = useState('')
  const [ownerGroup, setOwnerGroup] = useState('')
  const [topic, setTopic] = useState('')
  const [connectionRef, setConnectionRef] = useState('')
  const [catalog, setCatalog] = useState('')
  const [table, setTable] = useState('')
  const [mode, setMode] = useState('merge')
  const [mergeKeys, setMergeKeys] = useState('')
  const [restartPolicy, setRestartPolicy] = useState('always')
  const [viewers, setViewers] = useState('')
  const [checkpointPrefix, setCheckpointPrefix] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [refusals, setRefusals] = useState<string[]>([])

  // The group and catalog options arrive after the dialog opens (their reads are async).
  useEffect(() => {
    if (!ownerGroup) setOwnerGroup(groups.find((group) => group.ready)?.group ?? '')
  }, [ownerGroup, groups])
  useEffect(() => {
    if (!catalog && catalogs.data) setCatalog(catalogs.data.default)
  }, [catalog, catalogs.data])

  const disabled = submitting || !NAME_RE.test(name) || !TOPIC_RE.test(topic) || !tableOk(table) || !ownerGroup || !catalog

  async function submit(event: FormEvent) {
    event.preventDefault()
    if (disabled) return
    setSubmitting(true)
    setError(null)
    setRefusals([])

    const prefixValue = checkpointPrefix.trim()
    const body: StreamWriteBody = {
      name,
      ownerGroup,
      source: { ...(connectionRef ? { connectionRef } : {}), topic },
      sink: {
        catalog,
        table,
        mode,
        ...(mode === 'merge' ? { mergeKeys: splitList(mergeKeys) } : {}),
        ...(prefixValue !== '' ? { checkpointPrefix: prefixValue } : {}),
      },
      restartPolicy,
      ...(splitList(viewers).length > 0 ? { viewers: splitList(viewers) } : {}),
    }

    try {
      const check = await api.validateStream(body)
      if (!check.ok) {
        setRefusals(check.errors.map((issue) => issue.message))
        setSubmitting(false)
        return
      }
      const view = await api.saveStream(namespace, body)
      onSaved(view)
      onClose()
    } catch (e: unknown) {
      setError(errorMessage(e))
      setSubmitting(false)
    }
  }

  return (
    <DialogFrame
      title="New stream"
      titleId={titleId}
      initialFocus="[data-autofocus]"
      onClose={onClose}
      footer={
        <>
          <button type="submit" form={formId} className="btn-primary" disabled={disabled}>
            {submitting ? 'Creating…' : 'Create stream'}
          </button>
          <button type="button" className="btn-ghost" onClick={onClose}>Cancel</button>
        </>
      }
    >
      <form id={formId} className="space-y-4" onSubmit={(event) => void submit(event)}>
        {error && <p role="alert" className="text-sm text-bad break-words">{error}</p>}

        {refusals.length > 0 && (
          <ul role="alert" className="text-sm text-bad list-disc space-y-1 pl-5">
            {refusals.map((message) => (
              <li key={message} className="break-words">{message}</li>
            ))}
          </ul>
        )}

        <Field id={`${prefix}-name`} label="Name" hint={NAME_RULE}>
          <input
            id={`${prefix}-name`}
            className="input font-mono"
            autoComplete="off"
            spellCheck={false}
            data-autofocus
            pattern="[a-z][a-z0-9-]{0,39}"
            title={NAME_RULE}
            aria-describedby={`${prefix}-name-hint`}
            value={name}
            onChange={(event) => setName(event.target.value)}
          />
        </Field>

        <Field id={`${prefix}-group`} label="Owner group">
          <select
            id={`${prefix}-group`}
            className="input"
            value={ownerGroup}
            onChange={(event) => setOwnerGroup(event.target.value)}
          >
            {!ownerGroup && <option value="">Choose a group</option>}
            {groups.map((group) => (
              <option key={group.group} value={group.group} disabled={!group.ready}>
                {group.group}
              </option>
            ))}
          </select>
          {unusable.length > 0 && (
            <ul className="mt-1 space-y-0.5 text-xs text-text-dim">
              {unusable.map((group) => (
                <li key={group.group} className="break-words">
                  <span className="font-mono">{group.group}</span>: {group.message}
                </li>
              ))}
            </ul>
          )}
        </Field>

        <Field id={`${prefix}-topic`} label="Topic" hint={TOPIC_RULE}>
          <input
            id={`${prefix}-topic`}
            className="input font-mono"
            autoComplete="off"
            spellCheck={false}
            pattern="[A-Za-z0-9._-]{1,249}"
            title={TOPIC_RULE}
            aria-describedby={`${prefix}-topic-hint`}
            value={topic}
            onChange={(event) => setTopic(event.target.value)}
          />
        </Field>

        <Field id={`${prefix}-source`} label="Source connection">
          <select
            id={`${prefix}-source`}
            className="input"
            value={connectionRef}
            onChange={(event) => setConnectionRef(event.target.value)}
          >
            <option value="">the platform's own bus</option>
            {kafkaConnections.map((connection) => (
              <option key={connection.name} value={connection.name}>{connection.name}</option>
            ))}
          </select>
        </Field>

        <Field id={`${prefix}-catalog`} label="Catalog">
          <select
            id={`${prefix}-catalog`}
            className="input"
            value={catalog}
            onChange={(event) => setCatalog(event.target.value)}
          >
            {catalogNames.map((catalogName) => (
              <option key={catalogName} value={catalogName}>{catalogName}</option>
            ))}
          </select>
        </Field>

        <Field id={`${prefix}-table`} label="Table" hint={TABLE_RULE}>
          <input
            id={`${prefix}-table`}
            className="input font-mono"
            autoComplete="off"
            spellCheck={false}
            placeholder="schema.table"
            aria-describedby={`${prefix}-table-hint`}
            value={table}
            onChange={(event) => setTable(event.target.value)}
          />
        </Field>

        <Field id={`${prefix}-mode`} label="Write mode">
          <select
            id={`${prefix}-mode`}
            className="input"
            value={mode}
            onChange={(event) => setMode(event.target.value)}
          >
            <option value="merge">merge</option>
            <option value="append">append</option>
          </select>
        </Field>

        {mode === 'merge' && (
          <Field
            id={`${prefix}-merge-keys`}
            label="Merge keys"
            hint="Comma-separated column names; a merge sink needs 1 to 8 keys."
          >
            <input
              id={`${prefix}-merge-keys`}
              className="input font-mono"
              autoComplete="off"
              spellCheck={false}
              placeholder="id, region"
              aria-describedby={`${prefix}-merge-keys-hint`}
              value={mergeKeys}
              onChange={(event) => setMergeKeys(event.target.value)}
            />
          </Field>
        )}

        <Field id={`${prefix}-restart`} label="Restart policy">
          <select
            id={`${prefix}-restart`}
            className="input"
            value={restartPolicy}
            onChange={(event) => setRestartPolicy(event.target.value)}
          >
            <option value="always">always</option>
            <option value="on-failure">on-failure</option>
          </select>
        </Field>

        <Field
          id={`${prefix}-viewers`}
          label="Viewers"
          hint="Groups that may read the stream without being members. Comma-separated, optional."
        >
          <input
            id={`${prefix}-viewers`}
            className="input font-mono"
            autoComplete="off"
            spellCheck={false}
            placeholder="analysts, reporting"
            aria-describedby={`${prefix}-viewers-hint`}
            value={viewers}
            onChange={(event) => setViewers(event.target.value)}
          />
        </Field>

        <Field
          id={`${prefix}-checkpoint`}
          label="Checkpoint prefix"
          hint="Optional; blank uses the platform's default."
        >
          <input
            id={`${prefix}-checkpoint`}
            className="input font-mono"
            autoComplete="off"
            spellCheck={false}
            aria-describedby={`${prefix}-checkpoint-hint`}
            value={checkpointPrefix}
            onChange={(event) => setCheckpointPrefix(event.target.value)}
          />
        </Field>
      </form>
    </DialogFrame>
  )
}
