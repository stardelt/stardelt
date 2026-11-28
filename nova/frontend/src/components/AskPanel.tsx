/**
 * Ask — the panel (design nova#891 slice b).
 *
 * A person asks in words; a `ModelEndpoint` an administrator connected with purpose `ask`
 * writes one statement, and Nova checks it with `EXPLAIN` as the asking person. The
 * statement is never run here: the card offers the SQL editor and the person decides there.
 *
 * The promise on the outside is the rule inside: metadata only leaves the platform — the
 * question and the documentation of a few tables, never a row. The footer says where the
 * prompts go, so the promise is visible, not claimed.
 *
 * `Auto` is the default (#2342): the backend ranks the ready endpoints by this catalog's
 * evals and falls back once. The switcher offers every ready endpoint with its location,
 * and the person's choice is kept in this browser.
 */
import { useEffect, useRef, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { ApiError, api, errorMessage, type AskAnswer, type AskConversation, type AskPrevious, type ModelEndpoint } from '../api'
import { formatTime } from '../format'
import { tokenizeSql } from '../sqlHighlight'
import { TOKEN_CLASS } from '../sqlTokens'

/** What one question left in the panel. */
type Turn = {
  question: string
  answer?: AskAnswer
  /** `budget` marks a spent quota: a notice, never a red error. */
  error?: { message: string; budget: boolean }
  vote?: 'up' | 'down'
  /** The sentence of a vote that did not reach its row. */
  voteError?: string
}

/** The three stages one question goes through; the panel says which one it is in. */
const STEPS = ['Finding tables…', 'Writing SQL…', 'Checking as you…']
/** How long one stage shows before the next one is named. */
const STEP_MS = 900
/** How many of the person's read tables become example questions. */
const EXAMPLES = 3
const EMPTY =
  'Ask a question about the tables you can see. stardelt writes SQL, checks it as you, and never runs it without you.'
/** Where this browser keeps the person's endpoint choice (`auto` when nothing is stored). */
const ENDPOINT_KEY = 'stardelt.ask.endpoint'
/** The stored choice; a browser that refuses storage keeps Auto. */
function storedEndpoint(): string {
  try {
    return localStorage.getItem(ENDPOINT_KEY) ?? 'auto'
  } catch {
    return 'auto'
  }
}

export default function AskPanel({ endpoints, onClose, initialQuestion, onInitialQuestion }: {
  /** Every ready endpoint Shell found, in name order; the switcher offers them all. */
  endpoints: ModelEndpoint[]
  onClose: () => void
  /** A question handed in from outside the panel (the ⌘K row), asked once. */
  initialQuestion?: string
  /** Called after `initialQuestion` was taken, so the shell can forget it. */
  onInitialQuestion?: () => void
}) {
  const navigate = useNavigate()
  const [question, setQuestion] = useState('')
  const [turns, setTurns] = useState<Turn[]>([])
  // The answered turn the next question refines (nova#2341); `null` after "New question".
  const [followUp, setFollowUp] = useState<AskPrevious | null>(null)
  const [busy, setBusy] = useState(false)
  const [step, setStep] = useState(0)
  const [showHistory, setShowHistory] = useState(false)
  const bodyRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)
  // `busy` is state, so two fast Enters would both pass its check; the ref is the guard.
  const inFlight = useRef(false)
  // React runs an effect twice in development; the question from outside is asked once.
  const askedInitial = useRef(false)

  const [choice, setChoice] = useState(storedEndpoint)
  // A stored name that is no longer ready is Auto: a name that cannot answer is refused.
  const selected = choice !== 'auto' && endpoints.some(e => e.name === choice) ? choice : 'auto'

  const examples = useQuery({ queryKey: ['ask', 'examples'], queryFn: () => api.readTables() })
  const exampleTables = (examples.data?.tables ?? []).slice(0, EXAMPLES)

  const history = useQuery({
    queryKey: ['ask', 'history'],
    queryFn: api.askHistory,
    enabled: showHistory,
    retry: false,
  })
  const conversations = history.data ?? []

  // Pinning keeps a conversation for good; deleting removes one (maintainer, 2026-11-28).
  // Both act on the caller's own rows only — the API answers a foreign id with a 404.
  const queryClient = useQueryClient()
  const [confirmId, setConfirmId] = useState<number | null>(null)
  const [historyError, setHistoryError] = useState<string | null>(null)
  const refreshHistory = () => {
    void queryClient.invalidateQueries({ queryKey: ['ask', 'history'] })
  }
  const pin = useMutation({
    mutationFn: ({ id, pinned }: { id: number; pinned: boolean }) => api.askHistoryPin(id, pinned),
    onSuccess: () => {
      setHistoryError(null)
      refreshHistory()
    },
    onError: e => setHistoryError(errorMessage(e)),
  })
  const remove = useMutation({
    mutationFn: (id: number) => api.askHistoryDelete(id),
    onSuccess: () => {
      setConfirmId(null)
      setHistoryError(null)
      refreshHistory()
    },
    onError: e => setHistoryError(errorMessage(e)),
  })
  const pinned = conversations.filter(c => c.pinned)
  const unpinned = conversations.filter(c => !c.pinned)

  /** One History row: opening it, pinning it and deleting it are siblings (#2396). */
  function historyRow(c: AskConversation) {
    return (
      <div key={c.id} className="flex items-start gap-1">
        <button
          type="button"
          className="btn-ghost min-w-0 flex-1 text-left"
          data-testid="history-open"
          onClick={() => openConversation(c)}
        >
          <span className="block truncate text-sm">{c.question}</span>
          <span className="block text-xs text-text-mute">
            {formatTime(c.createdAt)}
            {c.explain === 'passed' && ' ✓ checked'}
            {c.explain === 'failed' && ' ✕ not runnable'}
          </span>
        </button>
        {confirmId === c.id ? (
          <span className="flex items-center gap-1 py-1">
            <span className="text-xs text-text-dim">Delete this conversation?</span>
            <button type="button" className="btn-ghost px-2 py-1 text-xs" onClick={() => remove.mutate(c.id)}>Delete</button>
            <button type="button" className="btn-ghost px-2 py-1 text-xs" onClick={() => setConfirmId(null)}>Cancel</button>
          </span>
        ) : (
          <>
            <button
              type="button"
              className={`btn-ghost px-2 py-1${c.pinned ? '' : ' opacity-40'}`}
              aria-label={c.pinned ? 'Unpin' : 'Pin'}
              aria-pressed={c.pinned}
              onClick={() => pin.mutate({ id: c.id, pinned: !c.pinned })}
            >📌</button>
            <button
              type="button"
              className="btn-ghost px-2 py-1"
              aria-label="Delete"
              onClick={() => setConfirmId(c.id)}
            >✕</button>
          </>
        )}
      </div>
    )
  }

  const sqlOfLastTurn = turns.length > 0 ? turns[turns.length - 1].answer?.sql ?? null : null

  function patchTurn(index: number, patch: Partial<Turn>) {
    setTurns(current => current.map((turn, i) => (i === index ? { ...turn, ...patch } : turn)))
  }

  async function send(text?: string) {
    const q = (text ?? question).trim()
    if (!q || inFlight.current) return
    inFlight.current = true
    const index = turns.length
    setTurns(current => [...current, { question: q }])
    setQuestion('')
    setBusy(true)
    setStep(0)
    const ticker = setInterval(() => setStep(s => Math.min(s + 1, STEPS.length - 1)), STEP_MS)
    try {
      const endpoint = selected === 'auto' ? undefined : selected
      const answer = await api.ask(q, undefined, followUp ?? undefined, endpoint)
      patchTurn(index, { answer })
      // The next question refines this answer, unless "New question" is pressed first.
      if (answer.sql !== null) {
        setFollowUp({ question: q, sql: answer.sql, tables: answer.tables })
      }
    } catch (e) {
      // A spent budget is the API's 429: a calm notice, not a failure of the question.
      const budget = e instanceof ApiError && e.status === 429
      patchTurn(index, { error: { message: errorMessage(e), budget } })
    } finally {
      clearInterval(ticker)
      inFlight.current = false
      setBusy(false)
      setStep(0)
      requestAnimationFrame(() => {
        const el = bodyRef.current
        if (el) el.scrollTop = el.scrollHeight
      })
      inputRef.current?.focus()
    }
  }

  async function vote(index: number, upOrDown: 'up' | 'down') {
    const callId = turns[index]?.answer?.callId
    if (callId == null) return
    try {
      await api.askFeedback(callId, upOrDown)
      patchTurn(index, { vote: upOrDown, voteError: undefined })
    } catch (e) {
      patchTurn(index, { voteError: errorMessage(e) })
    }
  }

  // The panel's own keys: Escape closes it, ArrowUp recalls the last question, and
  // ⌘/Ctrl+Enter opens the last statement in the editor — the question asked in words
  // becomes the statement the person runs, but only there.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault()
        onClose()
        return
      }
      const input = inputRef.current
      if (e.key === 'ArrowUp' && input && document.activeElement === input && input.value === '') {
        const previous = turns[turns.length - 1]?.question
        if (previous) {
          e.preventDefault()
          setQuestion(previous)
        }
        return
      }
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter' && sqlOfLastTurn) {
        e.preventDefault()
        navigate(`/query?sql=${encodeURIComponent(sqlOfLastTurn)}`)
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [turns, sqlOfLastTurn, navigate, onClose])

  function chooseEndpoint(name: string) {
    setChoice(name)
    try {
      localStorage.setItem(ENDPOINT_KEY, name)
    } catch {
      // A browser with no storage keeps the choice for this panel only.
    }
  }

  // Where the prompts of the next question go: the chosen endpoint's location, or, on
  // Auto, the strongest promise the ready endpoints keep.
  const chosen = endpoints.find(e => e.name === selected)
  const cloud = chosen ? chosen.location === 'cloud' : endpoints.some(e => e.location === 'cloud')
  const promise =
    selected === 'auto'
      ? cloud
        ? 'may use a cloud model · prompts may leave your cluster'
        : 'on-premise · prompts stay here'
      : cloud
        ? 'cloud · prompts leave your cluster'
        : 'on-premise · prompts stay here'

  // A question handed in by the ⌘K row: ask it once, then tell the shell it was taken.
  useEffect(() => {
    if (initialQuestion == null) {
      askedInitial.current = false
      return
    }
    if (askedInitial.current) return
    askedInitial.current = true
    void send(initialQuestion)
    onInitialQuestion?.()
  }, [initialQuestion, onInitialQuestion])

  /** Show one stored conversation as it was; the model is never asked again. */
  function openConversation(c: AskConversation) {
    setTurns([{
      question: c.question,
      answer: {
        endpoint: c.endpoint,
        model: c.model ?? '',
        tried: [],
        sql: c.sql,
        sentence: c.sentence,
        tables: c.tables,
        explain: c.explain,
        error: c.error,
        callId: c.callId ?? undefined,
      },
    }])
    setShowHistory(false)
    requestAnimationFrame(() => {
      const el = bodyRef.current
      if (el) el.scrollTop = el.scrollHeight
    })
  }

  return (
    <aside
      id="ask-panel"
      aria-label="Ask"
      className="fixed inset-0 z-50 flex flex-col bg-bg border-border md:border-l xl:static xl:z-auto xl:w-[420px] xl:shrink-0"
    >
      <div className="h-14 shrink-0 border-b border-border px-4 flex items-center gap-2">
        <span className="text-sm font-medium mr-auto">Ask</span>
        <button
          type="button"
          className="btn-ghost px-2 py-1 text-xs"
          aria-expanded={showHistory}
          aria-controls="ask-history"
          onClick={() => setShowHistory(v => !v)}
        >History</button>
        <button type="button" className="btn-ghost px-2.5 py-2" aria-label="Close Ask" onClick={onClose}>✕</button>
      </div>

      <div ref={bodyRef} className="flex-1 overflow-y-auto px-4 py-3 space-y-4">
        {showHistory ? (
          <div id="ask-history" className="space-y-1">
            {conversations.length === 0 && history.error == null && (
              <p className="text-sm text-text-dim">No questions yet.</p>
            )}
            {history.error != null && (
              <p role="status" className="text-sm text-text-dim">{errorMessage(history.error)}</p>
            )}
            {historyError != null && (
              <p role="status" className="text-sm text-text-dim">{historyError}</p>
            )}
            {pinned.length > 0 && (
              <p className="px-1 pt-1 text-xs text-text-mute">Pinned</p>
            )}
            {pinned.map(historyRow)}
            {unpinned.map(historyRow)}
          </div>
        ) : (
          <>
            {turns.map((turn, i) => (
              <TurnView key={i} turn={turn} onVote={vote.bind(null, i)} />
            ))}

            {busy && (
              <span role="status" aria-live="polite" className="text-xs text-text-mute">{STEPS[step]}</span>
            )}

            {turns.length === 0 && !busy && (
              <div className="space-y-3">
                <p className="text-sm text-text-dim">{EMPTY}</p>
                {exampleTables.map(table => {
                  const ask = `How many rows did ${table.namespace}.${table.name} get yesterday?`
                  return (
                    <button
                      key={`${table.namespace}.${table.name}`}
                      type="button"
                      className="btn-ghost text-left text-sm w-full"
                      onClick={() => void send(ask)}
                    >{ask}</button>
                  )
                })}
              </div>
            )}
          </>
        )}
      </div>

      <form
        className="shrink-0 border-t border-border px-4 py-2 flex items-center gap-2"
        onSubmit={e => {
          e.preventDefault()
          void send()
        }}
      >
        <input
          ref={inputRef}
          className="input"
          placeholder="Ask about your data…"
          aria-label="Ask a question"
          value={question}
          onChange={e => setQuestion(e.target.value)}
        />
        <button type="submit" className="btn-primary px-2.5 py-2" aria-label="Send the question" disabled={busy}>↵</button>
      </form>

      {followUp !== null && (
        <div className="shrink-0 px-4 pb-2">
          <button
            type="button"
            className="btn-ghost px-2 py-1 text-xs"
            onClick={() => setFollowUp(null)}
          >New question</button>
        </div>
      )}

      <div className="border-t border-border px-4 py-2 text-[11px] flex flex-wrap items-center gap-x-2 gap-y-1">
        <label htmlFor="ask-endpoint" className="text-text-mute">Model</label>
        <select
          id="ask-endpoint"
          className="input w-auto max-w-[260px] py-0.5 text-[11px]"
          value={selected}
          onChange={e => chooseEndpoint(e.target.value)}
        >
          <option value="auto">{endpoints.some(e => e.location === 'cloud') ? 'Auto · may use a cloud model' : 'Auto'}</option>
          {endpoints.map(e => (
            <option key={e.name} value={e.name} title={e.name}>
              {e.model} · {e.location === 'cloud' ? 'cloud' : 'on-premise'}
            </option>
          ))}
        </select>
        <span className={cloud ? 'text-warn' : 'text-text-mute'}>{promise}</span>
      </div>
    </aside>
  )
}

/** One question and its answer: the sentence, the statement, the check, the sources, the vote. */
function TurnView({ turn, onVote }: { turn: Turn; onVote: (vote: 'up' | 'down') => void }) {
  const navigate = useNavigate()
  const [copied, setCopied] = useState(false)
  const [verifying, setVerifying] = useState(false)
  const [verifiedOn, setVerifiedOn] = useState<string | null>(null)
  const [verifyError, setVerifyError] = useState<string | null>(null)
  const [verifyQuestion, setVerifyQuestion] = useState('')
  const [verifyTable, setVerifyTable] = useState('')
  const [savingVerify, setSavingVerify] = useState(false)
  const answer = turn.answer

  async function copy(text: string) {
    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // Clipboard access can be refused (insecure origin, permissions); nothing to report.
    }
  }

  const label = (table: AskAnswer['tables'][number]) =>
    `${table.catalog}.${table.namespace}.${table.table}`

  async function saveVerified() {
    if (answer?.sql == null) return
    const table = answer.tables.find((t) => label(t) === verifyTable)
    if (!table) return
    setSavingVerify(true)
    setVerifyError(null)
    try {
      await api.createVerifiedQuery(
        table.namespace,
        table.table,
        { question: verifyQuestion.trim(), sql: answer.sql },
        table.catalog,
      )
      setVerifiedOn(label(table))
      setVerifying(false)
    } catch (e) {
      setVerifyError(errorMessage(e))
    } finally {
      setSavingVerify(false)
    }
  }

  return (
    <div className="space-y-2">
      <div className="w-fit ml-auto max-w-[85%] rounded-[12px] border border-accent/30 bg-accent/15 px-3 py-2 text-sm">
        {turn.question}
      </div>

      {turn.error && (
        turn.error.budget
          ? <div role="status" className="rounded-[10px] border border-border bg-bg-soft p-3 text-sm text-text-dim">{turn.error.message}</div>
          : <div role="alert" className="rounded-[10px] border border-red-400/40 bg-red-400/10 p-3 text-sm text-bad">{turn.error.message}</div>
      )}

      {answer && (
        <div className="card p-3 space-y-2 text-sm">
          {answer.mode === 'agent'
            ? <p className="whitespace-pre-wrap">{answer.text ?? answer.sentence}</p>
            : <p>{answer.sentence}</p>}
          <p className="text-xs text-text-mute">
            answered by {answer.model || answer.endpoint}
            {(answer.tried?.length ?? 0) > 1 && ` after ${answer.tried[0]} failed`}
          </p>
          {answer.mode === 'sql' && (
            <p className="text-xs text-text-mute">SQL only: this model cannot use tools.</p>
          )}
          {answer.mode === 'agent' && <Steps steps={answer.steps ?? []} />}

          {answer.sql !== null && (
            <>
              <div className="flex items-center gap-2">
                <button type="button" className="btn-ghost py-0.5 px-2 text-xs" onClick={() => void copy(answer.sql ?? '')}>
                  {copied ? 'Copied' : 'Copy'}
                </button>
              </div>
              <pre
                data-testid="ask-sql"
                className="whitespace-pre-wrap break-words rounded-[10px] border border-border bg-bg-soft/60 p-3 font-mono text-xs"
              >
                {tokenizeSql(answer.sql).map((token, i) => (
                  <span key={i} data-token={token.kind} className={TOKEN_CLASS[token.kind]}>{token.text}</span>
                ))}
              </pre>

              {answer.explain === 'passed' && <p className="text-xs text-ok">✓ Checked: EXPLAIN passed as you</p>}
              {answer.explain === 'failed' && (
                <p className="text-xs text-bad">✕ Not runnable: {answer.error}. The model tried twice.</p>
              )}

              <div className="text-xs text-text-dim">Sources</div>
              <div className="flex flex-wrap gap-x-3 gap-y-1">
                {answer.tables.map(table => (
                  <Link
                    key={`${table.catalog}.${table.namespace}.${table.table}`}
                    className="text-xs text-accent hover:underline"
                    to={`/catalog?${new URLSearchParams({ catalog: table.catalog, ns: table.namespace, tbl: table.table }).toString()}`}
                  >{`${table.catalog}.${table.namespace}.${table.table}`}</Link>
                ))}
              </div>

              <div className="flex items-center gap-2 pt-1">
                <button
                  type="button"
                  className="btn-primary py-1.5 text-sm"
                  onClick={() => navigate(`/query?sql=${encodeURIComponent(answer.sql ?? '')}`)}
                >Open in SQL editor</button>
                {!verifying && verifiedOn === null && answer.tables.length > 0 && (
                  <button
                    type="button"
                    className="btn-ghost py-1.5 px-3 text-sm"
                    onClick={() => {
                      setVerifyError(null)
                      setVerifyQuestion(turn.question)
                      setVerifyTable(label(answer.tables[0]))
                      setVerifying(true)
                    }}
                  >Mark as verified</button>
                )}
                {answer.callId != null && (
                  <>
                    <button
                      type="button"
                      className="btn-ghost px-2 py-1"
                      aria-label="This answer helped"
                      aria-pressed={turn.vote === 'up'}
                      disabled={turn.vote != null}
                      onClick={() => onVote('up')}
                    >👍</button>
                    <button
                      type="button"
                      className="btn-ghost px-2 py-1"
                      aria-label="This answer was wrong"
                      aria-pressed={turn.vote === 'down'}
                      disabled={turn.vote != null}
                      onClick={() => onVote('down')}
                    >👎</button>
                  </>
                )}
              </div>
              {turn.voteError && <p className="text-xs text-bad">{turn.voteError}</p>}
              {verifying && (
                <form
                  className="space-y-2 rounded-[10px] border border-border bg-bg-soft/60 p-3"
                  onSubmit={(e) => {
                    e.preventDefault()
                    void saveVerified()
                  }}
                >
                  <label className="block text-xs text-text-dim" htmlFor="verify-table">Table</label>
                  <select
                    id="verify-table"
                    className="input w-full"
                    value={verifyTable}
                    onChange={(e) => setVerifyTable(e.target.value)}
                  >
                    {answer.tables.map((table) => (
                      <option key={label(table)} value={label(table)}>{label(table)}</option>
                    ))}
                  </select>
                  <label className="block text-xs text-text-dim" htmlFor="verify-question">Question</label>
                  <input
                    id="verify-question"
                    className="input w-full"
                    maxLength={500}
                    value={verifyQuestion}
                    onChange={(e) => setVerifyQuestion(e.target.value)}
                  />
                  <p className="text-xs text-text-dim">The statement above is stored as this question&rsquo;s verified answer.</p>
                  {verifyError && <p className="text-xs text-bad">{verifyError}</p>}
                  <div className="flex items-center gap-2">
                    <button type="submit" className="btn-primary py-1.5 px-3 text-sm" disabled={savingVerify || verifyQuestion.trim() === ''}>
                      {savingVerify ? 'Saving…' : 'Save verified query'}
                    </button>
                    <button type="button" className="btn-ghost py-1.5 px-3 text-sm" onClick={() => setVerifying(false)}>Cancel</button>
                  </div>
                </form>
              )}
              {verifiedOn !== null && (
                <p className="text-xs text-ok">Verified on {verifiedOn}</p>
              )}
            </>
          )}
        </div>
      )}
    </div>
  )
}

/** One tool call the agent made, as the step list draws it: a line, and its rows. */
function Steps({ steps }: { steps: NonNullable<AskAnswer['steps']> }) {
  const [open, setOpen] = useState(false)
  if (steps.length === 0) return null
  return (
    <div className="space-y-1">
      <button
        type="button"
        className="btn-ghost py-0.5 px-2 text-xs"
        aria-expanded={open}
        data-testid="ask-steps"
        onClick={() => setOpen(value => !value)}
      >Steps ({steps.length})</button>
      {open && (
        <ol className="space-y-2 text-xs text-text-dim">
          {steps.map((step, index) => (
            <li key={index} className="space-y-1">
              <span className="font-mono break-words">{step.summary}</span>
              {Array.isArray(step.rows) && step.rows.length > 0 && (
                <table className="text-[11px] border border-border">
                  <tbody>
                    {step.rows.slice(0, 5).map((row, rowIndex) => (
                      <tr key={rowIndex}>
                        {Array.isArray(row)
                          ? row.map((cell, cellIndex) => (
                            <td key={cellIndex} className="border border-border px-1 align-top">
                              {cellText(cell)}
                            </td>
                          ))
                          : <td className="border border-border px-1">{cellText(row)}</td>}
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </li>
          ))}
        </ol>
      )}
    </div>
  )
}

/** One cell of a `sql_read` row as text; `null` reads as an empty cell. */
function cellText(value: unknown): string {
  if (value === null || value === undefined) return ''
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
