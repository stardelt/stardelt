/**
 * Ask — the panel (design nova#891 slice b).
 *
 * A person asks in words; a `ModelEndpoint` an administrator connected with purpose `ask`
 * writes one statement, and Nova checks it with `EXPLAIN` as the asking person. The
 * statement is never run here: the card offers the SQL editor and the person decides there.
 *
 * The promise on the outside is the rule inside: metadata only leaves the platform — the
 * question and the documentation of a few tables, never a row. The footer says where the
 * prompts of the endpoint that answers go, so the promise is visible, not claimed.
 */
import { useEffect, useRef, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { ApiError, api, errorMessage, type AskAnswer, type AskConversation, type ModelEndpoint } from '../api'
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

export default function AskPanel({ endpoint, onClose, initialQuestion, onInitialQuestion }: {
  /** The ready endpoint Shell found; its model and location fill the footer. */
  endpoint: ModelEndpoint
  onClose: () => void
  /** A question handed in from outside the panel (the ⌘K row), asked once. */
  initialQuestion?: string
  /** Called after `initialQuestion` was taken, so the shell can forget it. */
  onInitialQuestion?: () => void
}) {
  const navigate = useNavigate()
  const [question, setQuestion] = useState('')
  const [turns, setTurns] = useState<Turn[]>([])
  const [busy, setBusy] = useState(false)
  const [step, setStep] = useState(0)
  const [showHistory, setShowHistory] = useState(false)
  const bodyRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)
  // `busy` is state, so two fast Enters would both pass its check; the ref is the guard.
  const inFlight = useRef(false)
  // React runs an effect twice in development; the question from outside is asked once.
  const askedInitial = useRef(false)

  const examples = useQuery({ queryKey: ['ask', 'examples'], queryFn: () => api.readTables() })
  const exampleTables = (examples.data?.tables ?? []).slice(0, EXAMPLES)

  const history = useQuery({
    queryKey: ['ask', 'history'],
    queryFn: api.askHistory,
    enabled: showHistory,
    retry: false,
  })
  const conversations = history.data?.items ?? []

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
      const answer = await api.ask(q)
      patchTurn(index, { answer })
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

  const cloud = endpoint.location === 'cloud'

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
            {conversations.map(c => (
              <button
                key={c.id}
                type="button"
                className="btn-ghost w-full text-left"
                onClick={() => openConversation(c)}
              >
                <span className="block truncate text-sm">{c.question}</span>
                <span className="block text-xs text-text-mute">
                  {formatTime(c.createdAt)}
                  {c.explain === 'passed' && ' ✓ checked'}
                  {c.explain === 'failed' && ' ✕ not runnable'}
                </span>
              </button>
            ))}
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

      <div className="border-t border-border px-4 py-2 text-[11px]">
        <span className={cloud ? 'text-warn' : 'text-text-mute'}>
          {endpoint.model} · {cloud ? 'cloud · prompts leave your cluster' : 'on-premise · prompts stay here'}
        </span>
      </div>
    </aside>
  )
}

/** One question and its answer: the sentence, the statement, the check, the sources, the vote. */
function TurnView({ turn, onVote }: { turn: Turn; onVote: (vote: 'up' | 'down') => void }) {
  const navigate = useNavigate()
  const [copied, setCopied] = useState(false)
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
          <p>{answer.sentence}</p>

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
            </>
          )}
        </div>
      )}
    </div>
  )
}
