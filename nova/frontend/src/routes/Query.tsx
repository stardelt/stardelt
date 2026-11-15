import { useEffect, useId, useRef, useState, type CSSProperties, type KeyboardEvent } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import {
  api, allTables, downloadCsv, errorMessage, formatCell,
  type Me, type QueryResult, type SavedQuery, type TableRef,
} from '../api'
import { useCatalogs } from '../components/CatalogSelect'
import CatalogTree from '../components/CatalogTree'
import CollapsiblePane, { usePaneOpen } from '../components/CollapsiblePane'
import ErrorBox from '../components/ErrorBox'
import { Icon } from '../components/Icon'
import ScheduleDialog from '../components/ScheduleDialog'
import WarehouseNote from '../components/WarehouseNote'
import { formatCount, sqlTableRef } from '../format'
import { tokenizeSql } from '../sqlHighlight'
import { splitStatements, statementToRun } from '../sqlStatements'
import { TOKEN_CLASS } from '../sqlTokens'

const HISTORY_KEY = 'stardelt-nova:history'
const HISTORY_LIMIT = 50
/** Rows the table draws: a result can hold 50,000, and a browser draws that many table rows very slowly. The CSV holds them all. */
const SHOWN_ROWS = 1000

/**
 * A sample that exists: the NYC demo table if present, else the first table, else a
 * constant query. With a catalog, the statement names that catalog, so the person's
 * chosen catalog is the one the editor opens in (stardelt-nova#234).
 */
function pickSample(tables: TableRef[] | undefined, catalog?: string): string {
  if (!tables || tables.length === 0) return 'SELECT 1'
  const t = tables.find(x => x.ns === 'nyc_taxi' && x.name === 'yellow_trips') ?? tables[0]
  return `SELECT * FROM ${sqlTableRef(t.ns, t.name, catalog)} LIMIT 100`
}

function loadHistory(): string[] {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(HISTORY_KEY) ?? '[]')
    // Stored data is untrusted: anything but an array of strings would crash `.filter` / `.slice` below.
    return Array.isArray(parsed) ? parsed.filter((h): h is string => typeof h === 'string') : []
  } catch {
    return []
  }
}
function saveHistory(h: string[]) {
  // Persisting history is a convenience; blocked or full storage must not fail the query.
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(h.slice(0, HISTORY_LIMIT)))
  } catch {
    /* ignore */
  }
}

/**
 * Save the statement above under a name (`POST /api/saved-queries`).
 *
 * A native `<dialog>` opened with `showModal()`, like StopRunDialog: the browser traps
 * focus, makes the page behind it inert and closes it on Escape. Focus starts in the title
 * input and returns to the button that opened it. Save stays disabled while the title is
 * blank or the editor is empty.
 */
function SaveQueryDialog({
  sql,
  onClose,
  onSaved,
}: {
  sql: string
  onClose: () => void
  onSaved: (title: string) => void
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const opener = useRef<HTMLElement | null>(null)
  const input = useRef<HTMLInputElement>(null)
  const id = useId()
  const titleId = `${id}-title`
  const [title, setTitle] = useState('')
  const qc = useQueryClient()
  const save = useMutation({
    mutationFn: (req: { title: string; sql: string; warehouse: string }) => api.saveQuery(req),
    onSuccess: (row) => {
      void qc.invalidateQueries({ queryKey: ['saved-queries'] })
      onSaved(row.title)
    },
  })
  const blocked = title.trim() === '' || sql.trim() === ''

  useEffect(() => {
    const dialog = ref.current
    if (!dialog) return
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null
    if (!dialog.open) dialog.showModal()
    input.current?.focus()
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
      <div className="flex max-h-[85vh] flex-col">
        <div className="px-5 pt-5">
          <h2 id={titleId} className="text-lg font-semibold">Save this query</h2>
        </div>
        <form
          className="min-h-0 space-y-3 overflow-y-auto px-5 py-4 text-sm"
          onSubmit={(e) => {
            e.preventDefault()
            if (!blocked && !save.isPending) save.mutate({ title: title.trim(), sql, warehouse: '' })
          }}
        >
          <label className="grid gap-1.5">
            <span className="label">Title</span>
            <input
              ref={input}
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              maxLength={200}
              aria-label="Query title"
              className="w-full rounded-[10px] border border-border bg-bg-soft/60 px-3 py-2 text-sm text-text outline-hidden focus:border-accent/60"
            />
          </label>
          <p className="text-xs text-text-mute">The name the query is listed under. It can be up to 200 characters.</p>
          {save.error != null && (
            <div role="alert" className="rounded-[10px] border border-red-500/40 bg-red-500/10 p-3 text-sm text-bad break-words">
              {errorMessage(save.error)}
            </div>
          )}
          <div className="flex flex-wrap items-center justify-end gap-2 border-t border-border pt-3">
            <button type="submit" className="btn-primary py-1.5" disabled={blocked || save.isPending}>
              {save.isPending ? 'Saving…' : 'Save'}
            </button>
            <button type="button" className="btn-ghost" onClick={onClose}>
              Cancel
            </button>
          </div>
        </form>
      </div>
    </dialog>
  )
}

/** Most characters a question may carry; the API's own limit (#1018). */
const MAX_QUESTION = 500
/** Most characters the SQL may carry; the API's own limit (#1018). */
const MAX_SQL = 20000
/** Most rows the API keeps of an expected result (#912). */
const MAX_EXPECTED_ROWS = 100

/**
 * Verify the statement at the cursor against the table it answers (design #911 slice 2c).
 *
 * The table is picked here instead of parsed out of the statement: a statement can join
 * three tables, and a verified query belongs to the one whose owners keep it. The question
 * is what a person asks; the statement is prefilled with the editor's selection. The rows
 * the last run produced are offered as the expected answer when the API could keep them —
 * a check (`#1019`) compares against exactly those.
 */
function VerifiedQueryDialog({
  catalog,
  sql,
  result,
  onClose,
  onSaved,
}: {
  catalog?: string
  /** The statement at the cursor, prefilled into the textarea. */
  sql: string
  /** The last run's result, offered as the expected answer. */
  result: QueryResult | null
  onClose: () => void
  onSaved: (table: string) => void
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const opener = useRef<HTMLElement | null>(null)
  const questionInput = useRef<HTMLInputElement>(null)
  const id = useId()
  const titleId = `${id}-title`
  const qc = useQueryClient()
  const catalogs = useCatalogs()
  const catalogList = catalogs.data?.catalogs ?? []
  // One catalog is no choice: the page's own `?catalog=` is the one the write lands in.
  const manyCatalogs = catalogList.length > 1
  const [pickedCatalog, setPickedCatalog] = useState('')
  const effectiveCatalog = manyCatalogs
    ? (pickedCatalog || catalog || catalogs.data?.default || catalogList[0]?.name || '')
    : (catalog ?? '')
  const tables = useQuery({
    queryKey: ['catalog', 'all', effectiveCatalog || undefined],
    queryFn: () => allTables(effectiveCatalog || undefined),
  })
  const tableList = tables.data ?? []
  /** `${ns}.${name}` of the picked table; the list is the only source of the pair. */
  const [table, setTable] = useState('')
  const [question, setQuestion] = useState('')
  const [statement, setStatement] = useState(sql)
  const [useResult, setUseResult] = useState(false)
  const rows = result?.rows ?? []
  const resultUsable = rows.length > 0 && rows.length <= MAX_EXPECTED_ROWS
  const target = tableList.find((t) => `${t.ns}.${t.name}` === table)
  const blocked = target === undefined || question.trim() === '' || statement.trim() === ''

  const create = useMutation({
    mutationFn: (req: { target: TableRef; body: { question: string; sql: string; expected?: unknown[][] } }) =>
      api.createVerifiedQuery(req.target.ns, req.target.name, req.body, effectiveCatalog || undefined),
    onSuccess: (_, req) => {
      void qc.invalidateQueries({ queryKey: ['verified-queries'] })
      onSaved(effectiveCatalog ? `${effectiveCatalog}.${req.target.ns}.${req.target.name}` : `${req.target.ns}.${req.target.name}`)
    },
  })

  useEffect(() => {
    const dialog = ref.current
    if (!dialog) return
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null
    if (!dialog.open) dialog.showModal()
    questionInput.current?.focus()
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
      className="m-auto w-[min(38rem,calc(100vw_-_2rem))] max-w-none rounded-[14px] border border-border bg-bg-elev p-0 text-text shadow-2xl backdrop:bg-black/60"
    >
      <div className="flex max-h-[85vh] flex-col">
        <div className="px-5 pt-5">
          <h2 id={titleId} className="text-lg font-semibold">Save as verified query</h2>
          <p className="mt-1 text-xs text-text-mute">
            The question this table answers, with the statement that answered it. The table's owners keep it.
          </p>
        </div>
        <form
          className="min-h-0 space-y-3 overflow-y-auto px-5 py-4 text-sm"
          onSubmit={(e) => {
            e.preventDefault()
            if (blocked || create.isPending || !target) return
            create.mutate({
              target,
              body: {
                question: question.trim(),
                sql: statement.trim(),
                ...(useResult && resultUsable ? { expected: rows } : {}),
              },
            })
          }}
        >
          {manyCatalogs && (
            <label className="grid gap-1.5">
              <span className="label">Catalog</span>
              <select
                value={effectiveCatalog}
                onChange={(e) => {
                  setPickedCatalog(e.target.value)
                  setTable('')
                }}
                aria-label="Catalog"
                className="w-full rounded-[10px] border border-border bg-bg-soft/60 px-3 py-2 text-sm text-text outline-hidden focus:border-accent/60"
              >
                {catalogList.map((c) => (
                  <option key={c.name} value={c.name}>{c.name}</option>
                ))}
              </select>
            </label>
          )}

          <label className="grid gap-1.5">
            <span className="label">Table</span>
            <select
              value={table}
              onChange={(e) => setTable(e.target.value)}
              required
              disabled={tables.isPending}
              aria-label="Table"
              className="w-full rounded-[10px] border border-border bg-bg-soft/60 px-3 py-2 text-sm text-text outline-hidden focus:border-accent/60 disabled:opacity-60"
            >
              <option value="">{tables.isPending ? 'Tables…' : 'Pick a table'}</option>
              {tableList.map((t) => (
                <option key={`${t.ns}.${t.name}`} value={`${t.ns}.${t.name}`}>{`${t.ns}.${t.name}`}</option>
              ))}
            </select>
          </label>

          <label className="grid gap-1.5">
            <span className="label">Question</span>
            <input
              ref={questionInput}
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              maxLength={MAX_QUESTION}
              required
              aria-label="Question"
              placeholder="What did revenue last month come to?"
              className="w-full rounded-[10px] border border-border bg-bg-soft/60 px-3 py-2 text-sm text-text outline-hidden focus:border-accent/60"
            />
          </label>

          <label className="grid gap-1.5">
            <span className="label">Statement</span>
            <textarea
              value={statement}
              onChange={(e) => setStatement(e.target.value)}
              rows={8}
              maxLength={MAX_SQL}
              required
              aria-label="Statement"
              spellCheck={false}
              className="w-full resize-y rounded-[10px] border border-border bg-bg-soft/60 px-3 py-2 font-mono text-xs text-text outline-hidden focus:border-accent/60"
            />
          </label>

          <fieldset className="grid gap-1.5">
            <legend className="label">Expected answer</legend>
            <label className="flex items-center gap-2">
              <input
                type="radio"
                name={`${id}-expected`}
                checked={!useResult || !resultUsable}
                onChange={() => setUseResult(false)}
              />
              <span>No expected answer</span>
            </label>
            <label className="flex items-center gap-2">
              <input
                type="radio"
                name={`${id}-expected`}
                checked={useResult && resultUsable}
                disabled={!resultUsable}
                onChange={() => setUseResult(true)}
              />
              <span>{`Use this run's result (${rows.length} ${rows.length === 1 ? 'row' : 'rows'})`}</span>
            </label>
            {!resultUsable && (
              <p className="text-xs text-text-mute">
                The API keeps at most 100 rows; run a statement that returns fewer.
              </p>
            )}
          </fieldset>

          {create.error != null && (
            <div role="alert" className="rounded-[10px] border border-red-500/40 bg-red-500/10 p-3 text-sm text-bad break-words">
              {errorMessage(create.error)}
            </div>
          )}

          <div className="flex flex-wrap items-center justify-end gap-2 border-t border-border pt-3">
            <button type="submit" className="btn-primary py-1.5" disabled={blocked || create.isPending}>
              {create.isPending ? 'Saving…' : 'Save'}
            </button>
            <button type="button" className="btn-ghost" onClick={onClose}>
              Cancel
            </button>
          </div>
        </form>
      </div>
    </dialog>
  )
}

/**
 * Delete one saved query (`DELETE /api/saved-queries/{id}`). The confirmation names the
 * title, so a person can see which one goes; Cancel is focused first and Escape closes.
 */
function DeleteQueryDialog({
  row,
  onClose,
  onDeleted,
}: {
  row: SavedQuery
  onClose: () => void
  onDeleted: (title: string) => void
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const opener = useRef<HTMLElement | null>(null)
  const id = useId()
  const titleId = `${id}-title`
  const qc = useQueryClient()
  const remove = useMutation({
    mutationFn: () => api.deleteSavedQuery(row.id),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['saved-queries'] })
      onDeleted(row.title)
    },
  })

  useEffect(() => {
    const dialog = ref.current
    if (!dialog) return
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null
    if (!dialog.open) dialog.showModal()
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
      <div className="flex max-h-[85vh] flex-col">
        <div className="px-5 pt-5">
          <h2 id={titleId} className="text-lg font-semibold">Delete “{row.title}”?</h2>
        </div>
        <div className="min-h-0 overflow-y-auto px-5 py-4 space-y-3 text-sm">
          <p className="text-text-dim">The saved query is removed from your list. The statement itself is not run or changed.</p>
          {remove.error != null && (
            <div role="alert" className="rounded-[10px] border border-red-500/40 bg-red-500/10 p-3 text-sm text-bad break-words">
              {errorMessage(remove.error)}
            </div>
          )}
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2 border-t border-border px-5 py-3">
          <button
            type="button"
            className="btn-danger"
            disabled={remove.isPending}
            onClick={() => remove.mutate()}
          >
            {remove.isPending ? 'Deleting…' : 'Delete'}
          </button>
          <button type="button" className="btn-ghost" onClick={onClose} data-autofocus>
            Cancel
          </button>
        </div>
      </div>
    </dialog>
  )
}

/**
 * Share one saved query with a group (`POST /api/saved-queries/{id}/share`). The groups
 * offered are the caller's own, minus the ones the query is already shared with: the API
 * answers `204` for a group the caller is not in, and sharing again would say nothing.
 */
function ShareQueryDialog({
  row,
  me,
  onClose,
  onShared,
}: {
  row: SavedQuery
  me: Me | undefined
  onClose: () => void
  onShared: (group: string) => void
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const opener = useRef<HTMLElement | null>(null)
  const select = useRef<HTMLSelectElement>(null)
  const cancel = useRef<HTMLButtonElement>(null)
  const id = useId()
  const titleId = `${id}-title`
  const qc = useQueryClient()
  const available = (me?.groups ?? []).filter((g) => !row.shared_with.includes(g))
  const [group, setGroup] = useState(available[0] ?? '')
  const share = useMutation({
    mutationFn: (g: string) => api.shareSavedQuery(row.id, g),
    onSuccess: (_answer, g) => {
      void qc.invalidateQueries({ queryKey: ['saved-queries'] })
      onShared(g)
    },
  })

  useEffect(() => {
    const dialog = ref.current
    if (!dialog) return
    opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null
    if (!dialog.open) dialog.showModal()
    if (select.current) select.current.focus()
    else cancel.current?.focus()
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
      <div className="flex max-h-[85vh] flex-col">
        <div className="px-5 pt-5">
          <h2 id={titleId} className="text-lg font-semibold">Share “{row.title}”</h2>
        </div>
        <form
          className="min-h-0 space-y-3 overflow-y-auto px-5 py-4 text-sm"
          onSubmit={(e) => {
            e.preventDefault()
            if (group !== '' && !share.isPending) share.mutate(group)
          }}
        >
          {available.length === 0 ? (
            <p className="text-xs text-text-mute">Every group you are in already has this query.</p>
          ) : (
            <>
              <label className="grid gap-1.5">
                <span className="label">Share with group</span>
                <select
                  ref={select}
                  value={group}
                  onChange={(e) => setGroup(e.target.value)}
                  aria-label="Share with group"
                  className="w-full rounded-[10px] border border-border bg-bg-soft/60 px-3 py-2 text-sm text-text outline-hidden focus:border-accent/60"
                >
                  {available.map((g) => <option key={g} value={g}>{g}</option>)}
                </select>
              </label>
              <p className="text-xs text-text-mute">Everyone in the group can open it from their saved queries and run it. Only you can delete it.</p>
            </>
          )}
          {share.error != null && (
            <div role="alert" className="rounded-[10px] border border-red-500/40 bg-red-500/10 p-3 text-sm text-bad break-words">
              {errorMessage(share.error)}
            </div>
          )}
          <div className="flex flex-wrap items-center justify-end gap-2 border-t border-border pt-3">
            {available.length > 0 && (
              <button type="submit" className="btn-primary py-1.5" disabled={group === '' || share.isPending}>
                {share.isPending ? 'Sharing…' : 'Share'}
              </button>
            )}
            <button ref={cancel} type="button" className="btn-ghost" onClick={onClose}>
              Cancel
            </button>
          </div>
        </form>
      </div>
    </dialog>
  )
}

export default function Query() {
  const [params] = useSearchParams()
  // The catalog the person works in. It names the catalogs the sample is generated from,
  // and the Catalog page's "Query table" link carries it along (stardelt-nova#234).
  const catalog = params.get('catalog') ?? undefined
  const catalogs = useCatalogs()
  const tables = useQuery({
    queryKey: ['catalog', 'all', catalog],
    queryFn: () => allTables(catalog),
  })
  const sample = pickSample(tables.data, catalog)
  // `null` = the user has not typed or picked anything: show the sample.
  const [typed, setTyped] = useState<string | null>(params.get('sql'))
  const sql = typed ?? sample
  const [running, setRunning] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [hint, setHint] = useState<string | null>(null)
  const [result, setResult] = useState<QueryResult | null>(null)
  const [elapsed, setElapsed] = useState<number | null>(null)
  const [history, setHistory] = useState<string[]>(loadHistory)
  const [scheduling, setScheduling] = useState(false)
  const [scheduled, setScheduled] = useState<{ name: string; namespace: string } | null>(null)
  const [scheduleSql, setScheduleSql] = useState('')
  const [verifying, setVerifying] = useState(false)
  const [verifySql, setVerifySql] = useState('')
  const saved = useQuery({ queryKey: ['saved-queries'], queryFn: () => api.savedQueries() })
  const me = useQuery({ queryKey: ['me'], queryFn: api.me })
  const [saving, setSaving] = useState(false)
  const [deleting, setDeleting] = useState<SavedQuery | null>(null)
  const [sharing, setSharing] = useState<SavedQuery | null>(null)
  const qc = useQueryClient()
  // The last successful save, delete, share or unshare, in the person's words (role="status");
  // a failed unshare sets `bad`, because it is the API's sentence and not a confirmation.
  const [notice, setNotice] = useState<{ text: string; bad?: boolean } | null>(null)
  // The Tables pane's search box; it only narrows what the tree shows.
  const [filter, setFilter] = useState('')
  const taRef = useRef<HTMLTextAreaElement>(null)
  // The line-number gutter reads `sql`, never `typed`: the sample is a statement too (#732).
  const gutterRef = useRef<HTMLDivElement>(null)
  // The coloured overlay behind the textarea; it reads `sql` too, so the first paint has colour.
  const highlightRef = useRef<HTMLPreElement>(null)
  const lineCount = Math.max(1, sql.split('\n').length)
  const empty = sql.trim() === ''
  // The two side panes are user-collapsible; their state sets the grid's columns (#586).
  const tablesOpen = usePaneOpen('tables')
  const historyOpen = usePaneOpen('history')
  const columns = `${tablesOpen ? '260px' : '40px'} minmax(0,1fr) ${historyOpen ? '260px' : '40px'}`

  useEffect(() => {
    const fromUrl = params.get('sql')
    if (fromUrl) setTyped(fromUrl)
  }, [params])

  async function run() {
    if (running) return // Ctrl+Enter bypasses the disabled button
    const area = taRef.current
    const target = statementToRun(
      sql,
      area ? area.selectionStart : sql.length,
      area ? area.selectionEnd : sql.length,
    )
    if ('error' in target) {
      setResult(null); setElapsed(null); setError(null); setHint(target.error)
      area?.focus() // keep the person's selection visible next to the advice
      return
    }
    const stmt = target.statement.text
    // Several statements in the editor: select the one that runs, so the person sees which.
    if (area && splitStatements(sql).length > 1) {
      area.focus()
      area.setSelectionRange(target.statement.start, target.statement.end)
    }
    setRunning(true); setError(null); setHint(null); setResult(null); setElapsed(null)
    const t0 = performance.now()
    try {
      const r = await api.query(stmt)
      setResult(r)
      setElapsed(performance.now() - t0)
      const next = [stmt, ...history.filter(h => h !== stmt)].slice(0, HISTORY_LIMIT)
      setHistory(next); saveHistory(next)
    } catch (e: unknown) {
      setError(errorMessage(e))
      setElapsed(performance.now() - t0)
    } finally {
      setRunning(false)
    }
  }

  function onKey(e: KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault()
      run()
    }
  }

  /**
   * Insert a name from the Tables pane at the cursor (stardelt-nova#586, nova#783). A table
   * inserts the fully qualified name the editor elsewhere generates (`catalog.schema.table`);
   * a column inserts just the column. The caret lands after it, so the next keystroke
   * continues the statement.
   */
  function insertRef({
    catalog: inCatalog,
    ns,
    tbl,
    column,
  }: {
    catalog: string
    ns: string
    tbl: string
    column?: string
  }) {
    const text = column ?? sqlTableRef(ns, tbl, inCatalog)
    const area = taRef.current
    const start = area ? area.selectionStart : sql.length
    const end = area ? area.selectionEnd : sql.length
    setTyped(sql.slice(0, start) + text + sql.slice(end))
    const caret = start + text.length
    requestAnimationFrame(() => {
      const next = taRef.current
      if (!next) return
      next.focus()
      next.setSelectionRange(caret, caret)
    })
  }

  /** `/api/me` carries `sub` in oidc mode only; dev mode reports the static user as `user`, which is what the store wrote as `owner_sub`. */
  const mySub = me.data?.sub ?? me.data?.user ?? ''
  const mine = (row: SavedQuery) => row.owner_sub === mySub

  /**
   * Stop sharing one query with one group (`DELETE /api/saved-queries/{id}/share/{group}`).
   * A failure is shown: the API's sentence explains why the row still says "shared with".
   */
  async function unshare(row: SavedQuery, group: string) {
    try {
      await api.unshareSavedQuery(row.id, group)
      setNotice({ text: `Stopped sharing “${row.title}” with ${group}.` })
      await qc.invalidateQueries({ queryKey: ['saved-queries'] })
    } catch (e: unknown) {
      setNotice({ text: errorMessage(e), bad: true })
    }
  }

  return (
    <div
      className="grid grid-cols-1 lg:grid-cols-[var(--sql-panes)] content-start lg:content-stretch gap-5 h-full min-h-0"
      style={{ '--sql-panes': columns } as CSSProperties}
    >
      <h1 className="sr-only">SQL editor</h1>

      <CollapsiblePane id="tables" title="Tables" side="left">
        <input
          value={filter}
          onChange={e => setFilter(e.target.value)}
          placeholder="Search tables…"
          aria-label="Search tables"
          className="input mb-2 py-1.5"
        />
        <div className="min-h-0 flex-1 overflow-auto">
          {catalogs.isError ? (
            <ErrorBox error={catalogs.error} onRetry={() => void catalogs.refetch()} />
          ) : (catalogs.data?.catalogs?.length ?? 0) === 0 ? (
            <p className="px-2 py-1 text-xs text-text-mute">
              {catalogs.isPending ? 'catalogs…' : 'No catalog is open to you yet.'}
            </p>
          ) : (
            <CatalogTree
              key={catalogs.data?.default ?? ''}
              catalogs={catalogs.data?.catalogs ?? []}
              defaultCatalog={catalogs.data?.default ?? ''}
              filter={filter}
              selected={null}
              onSelect={() => {}}
              mode="pick"
              onPick={insertRef}
            />
          )}
        </div>
      </CollapsiblePane>

      <div className="flex flex-col gap-4 min-h-0 min-w-0">
        <div className="card p-3 focus-within:border-accent/60">
          <div className="flex h-36 gap-2">
            <div
              ref={gutterRef}
              aria-hidden="true"
              data-testid="sql-gutter"
              className="w-9 shrink-0 overflow-hidden select-none text-right font-mono text-sm leading-6 text-text-mute"
            >
              {Array.from({ length: lineCount }, (_, i) => (
                <div key={i}>{i + 1}</div>
              ))}
            </div>
            <div className="relative min-h-0 min-w-0 flex-1">
              <pre
                ref={highlightRef}
                aria-hidden="true"
                data-testid="sql-highlight"
                className="pointer-events-none absolute inset-0 m-0 overflow-hidden whitespace-pre font-mono text-sm leading-6 text-text"
              >
                {tokenizeSql(sql).map((token, i) => (
                  <span key={i} data-token={token.kind} className={TOKEN_CLASS[token.kind]}>
                    {token.text}
                  </span>
                ))}
              </pre>
              <textarea
                ref={taRef}
                value={sql}
                onChange={e => setTyped(e.target.value)}
                onKeyDown={onKey}
                onScroll={e => {
                  const { scrollTop, scrollLeft } = e.currentTarget
                  if (gutterRef.current) gutterRef.current.scrollTop = scrollTop
                  if (highlightRef.current) {
                    highlightRef.current.scrollTop = scrollTop
                    highlightRef.current.scrollLeft = scrollLeft
                  }
                }}
                aria-label="SQL query"
                wrap="off"
                className="relative h-full w-full resize-none overflow-auto whitespace-pre bg-transparent font-mono text-sm leading-6 text-text [-webkit-text-fill-color:transparent] caret-[var(--color-text)] outline-hidden selection:bg-accent/25 placeholder:text-text-mute"
                spellCheck={false}
                placeholder="SELECT ..."
              />
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 mt-2">
            <button onClick={run} disabled={running} className="btn-primary py-1.5">
              <Icon name="terminal" className="w-4 h-4" />
              {running ? 'Running…' : 'Run'}
            </button>
            <button onClick={() => setTyped(null)} className="btn-ghost py-1.5">Sample</button>
            <button
              type="button"
              onClick={() => {
                const area = taRef.current
                const t = statementToRun(sql, area ? area.selectionStart : sql.length, area ? area.selectionEnd : sql.length)
                if ('error' in t) {
                  setResult(null); setElapsed(null); setError(null); setHint(t.error)
                  area?.focus() // the same refusal as Run, and the person's selection stays visible
                  return
                }
                setScheduleSql(t.statement.text)
                setScheduling(true)
              }}
              disabled={empty}
              title={empty ? 'Write a statement first' : undefined}
              aria-label="Schedule this statement"
              className="btn-ghost py-1.5"
            >
              <Icon name="clock" className="w-4 h-4" />
              Schedule
            </button>
            <button
              type="button"
              onClick={() => {
                const area = taRef.current
                const t = statementToRun(sql, area ? area.selectionStart : sql.length, area ? area.selectionEnd : sql.length)
                if ('error' in t) {
                  setResult(null); setElapsed(null); setError(null); setHint(t.error)
                  area?.focus() // the same refusal as Run, and the person's selection stays visible
                  return
                }
                setVerifySql(t.statement.text)
                setVerifying(true)
              }}
              disabled={empty}
              title={empty ? 'Write a statement first' : undefined}
              aria-label="Save as verified query"
              className="btn-ghost py-1.5"
            >
              <Icon name="spark" className="w-4 h-4" />
              Verify
            </button>
            <div className="text-xs text-text-mute ml-auto">⌘/Ctrl+Enter runs the statement at the cursor</div>
            {/* The class the engine will schedule this statement into (#236): the same
                line the Profile page shows. Nothing while the answer is on its way or the
                platform has no warehouse to name. */}
            <WarehouseNote className="text-xs text-text-mute" />
          </div>
        </div>

        {scheduled && (
          <p role="status" className="text-sm text-ok break-words">
            Scheduled{' '}
            <Link
              to={`/pipelines/${encodeURIComponent(scheduled.namespace)}/${encodeURIComponent(scheduled.name)}`}
              className="underline"
            >
              {scheduled.name}
            </Link>
            . It appears under Pipelines within a minute.
          </p>
        )}

        {notice && (
          <p
            role={notice.bad ? 'alert' : 'status'}
            className={`text-sm break-words ${notice.bad ? 'text-bad' : 'text-ok'}`}
          >{notice.text}</p>
        )}

        {hint && (
          <p role="status" className="rounded-[14px] border border-border bg-bg-elev p-4 text-sm text-text-dim">
            {hint}
          </p>
        )}

        {error && (
          <div className="rounded-[14px] border border-red-500/40 bg-red-500/10 p-4 text-sm text-bad font-mono whitespace-pre-wrap break-words">
            {error}
          </div>
        )}

        {!result && !error && !hint && !running && (
          <div className="card flex-1 min-h-0 grid place-items-center p-6 text-center">
            <div>
              <p className="text-text-dim text-sm">Run a query to see results here.</p>
              <p className="text-xs text-text-mute mt-1">
                ⌘/Ctrl+Enter runs the statement at the cursor, or the selected text. The row count, the timing and a CSV download appear in this panel.
              </p>
            </div>
          </div>
        )}

        {result && (
          <div className="card flex-1 min-h-0 overflow-hidden flex flex-col">
            <div className="px-4 py-2 text-xs text-text-mute border-b border-border flex items-center gap-3">
              <span>{result.row_count} row{result.row_count === 1 ? '' : 's'}</span>
              {elapsed != null && <span>· {(elapsed / 1000).toFixed(2)}s</span>}
              <span className="font-mono truncate">{result.query_id}</span>
              <button
                onClick={() => downloadCsv(`nova-query-${result.query_id ?? 'result'}.csv`, result.columns.map(c => c.name), result.rows)}
                className="btn-ghost ml-auto py-1 px-2 text-xs"
                title={result.truncated ? 'This CSV holds only the first rows: add a LIMIT or aggregate to see the rest.' : 'Download these rows as CSV'}
              >
                <Icon name="download" className="w-3.5 h-3.5" />
                CSV
              </button>
            </div>
            {result.truncated && (
              <div className="px-4 py-2 text-xs bg-amber-500/10 border-b border-amber-500/40 text-amber-600 dark:text-amber-400">
                Showing the first {formatCount(result.row_count)} rows. Add a LIMIT or aggregate to see the rest.
              </div>
            )}
            {result.rows.length > SHOWN_ROWS && (
              <div className="px-4 py-2 text-xs border-b border-border text-text-mute">
                The table shows the first {formatCount(SHOWN_ROWS)} of {formatCount(result.rows.length)} rows{result.truncated ? '; the CSV holds the rows returned.' : '; the CSV holds all of them.'}
              </div>
            )}
            <div className="overflow-auto flex-1">
              <table className="w-full text-sm font-mono">
                <thead className="text-text-mute sticky top-0 bg-bg-elev">
                  <tr>{result.columns.map((c, i) => <th key={i} className="th px-4 py-2">{c.name}</th>)}</tr>
                </thead>
                <tbody>
                  {result.rows.slice(0, SHOWN_ROWS).map((row, i) => (
                    <tr key={i} className="border-t border-border/60">
                      {row.map((cell, j) => <td key={j} className="td px-4 text-text-dim">{formatCell(cell)}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>

      <CollapsiblePane id="history" title="History" side="right">
        <div className="min-h-0 flex-1 overflow-auto">
          {/* The pane's own header names the section; this row holds the Clear button. */}
          <section>
            <div className="flex items-center justify-end mb-2">
              <button
                onClick={() => { setHistory([]); saveHistory([]) }}
                className="text-xs text-text-mute hover:text-text"
              >clear</button>
            </div>
            {history.length === 0 && <div className="text-text-mute text-sm">No queries yet — run one above.</div>}
            <ul className="space-y-2">
              {history.map((h, i) => (
                <li key={i}>
                  <button
                    onClick={() => { setTyped(h); taRef.current?.focus() }}
                    className="w-full text-left text-xs font-mono text-text-dim hover:text-text break-all"
                    title={h}
                  >
                    {h.length > 60 ? h.slice(0, 60) + '…' : h}
                  </button>
                </li>
              ))}
            </ul>
          </section>

          <section className="mt-5 border-t border-border pt-4">
            <div className="flex items-center justify-between mb-3">
              <h3 className="label">Saved queries</h3>
              <button
                type="button"
                onClick={() => setSaving(true)}
                disabled={saved.isError}
                title={saved.isError ? 'The saved queries could not be loaded' : undefined}
                className="text-xs text-text-mute hover:text-text disabled:opacity-50"
              >Save query</button>
            </div>
            {saved.isPending && <div className="text-text-mute text-sm">saved queries…</div>}
            {saved.isError && <ErrorBox error={saved.error} onRetry={() => void saved.refetch()} />}
            {saved.data?.length === 0 && (
              <div className="text-text-mute text-sm">No saved queries yet — run a query and choose Save query.</div>
            )}
            <ul className="space-y-2">
              {saved.data?.map((row) => (
                <li key={row.id} className="flex items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <button
                      type="button"
                      onClick={() => { setTyped(row.sql); taRef.current?.focus() }}
                      className="w-full text-left text-xs text-text-dim hover:text-text break-words"
                      title={row.sql}
                    >
                      {row.title}
                    </button>
                    {row.shared_with.length > 0 && (
                      <div className="text-[11px] text-text-mute">
                        shared with {row.shared_with.join(', ')}
                        {mine(row) && row.shared_with.map((group) => (
                          <button
                            key={group}
                            type="button"
                            onClick={() => void unshare(row, group)}
                            aria-label={`Stop sharing “${row.title}” with ${group}`}
                            className="ml-1 underline hover:text-bad"
                          >Unshare</button>
                        ))}
                      </div>
                    )}
                  </div>
                  {mine(row) ? (
                    <div className="flex shrink-0 items-start gap-2">
                      <button
                        type="button"
                        onClick={() => setSharing(row)}
                        className="text-xs text-text-mute hover:text-text"
                      >Share</button>
                      <button
                        type="button"
                        onClick={() => setDeleting(row)}
                        aria-label={`Delete “${row.title}”`}
                        className="text-xs text-text-mute hover:text-bad"
                      >Delete</button>
                    </div>
                  ) : (
                    <span className="shrink-0 text-[11px] text-text-mute">shared with you</span>
                  )}
                </li>
              ))}
            </ul>
          </section>
        </div>
      </CollapsiblePane>

      {saving && (
        <SaveQueryDialog
          sql={sql}
          onClose={() => setSaving(false)}
          onSaved={(title) => {
            setSaving(false)
            setNotice({ text: `Saved “${title}”.` })
          }}
        />
      )}

      {deleting && (
        <DeleteQueryDialog
          row={deleting}
          onClose={() => setDeleting(null)}
          onDeleted={(title) => {
            setDeleting(null)
            setNotice({ text: `Deleted “${title}”.` })
          }}
        />
      )}

      {sharing && (
        <ShareQueryDialog
          row={sharing}
          me={me.data}
          onClose={() => setSharing(null)}
          onShared={(group) => {
            setSharing(null)
            setNotice({ text: `Shared “${sharing.title}” with ${group}.` })
          }}
        />
      )}

      {scheduling && (
        <ScheduleDialog
          mode="create"
          sql={scheduleSql}
          onClose={() => setScheduling(false)}
          onScheduled={(view) => {
            setScheduling(false)
            setScheduled({ name: view.name, namespace: view.namespace })
          }}
        />
      )}

      {verifying && (
        <VerifiedQueryDialog
          catalog={catalog}
          sql={verifySql}
          result={result}
          onClose={() => setVerifying(false)}
          onSaved={(table) => {
            setVerifying(false)
            setNotice({ text: `Saved — ${table} lists it under Verified queries.` })
          }}
        />
      )}
    </div>
  )
}
