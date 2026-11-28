/**
 * An agent answer is Markdown: models write `**bold**`, lists, tables and code.
 *
 * The text comes from a model, whose input can include table descriptions and query
 * results that someone else wrote, so it is untrusted. This component therefore never
 * builds HTML: `marked.lexer` turns the text into tokens and each token becomes a React
 * element. The model's own HTML is shown as its text, an image is shown as its alt text
 * and is never fetched, and a link is only drawn for an `http(s)://` target or a path on
 * this site (not `//host`, which a browser reads as another site).
 */
import type { ReactNode } from 'react'
import { marked, type Token, type Tokens } from 'marked'
import { tokenizeSql } from '../sqlHighlight'
import { TOKEN_CLASS } from '../sqlTokens'

/** The link targets Nova draws: the web and its own site. Everything else is text. */
function safeHref(href: string): string | null {
  // `//host` and `/\host` are protocol-relative to the browser, not site-relative.
  return /^(https?:\/\/|\/(?![/\\]))/.test(href) ? href : null
}

/** One inline token as an element; unknown tokens keep their text. */
function inlineToken(token: Token, key: number): ReactNode {
  switch (token.type) {
    case 'text':
      return <span key={key}>{token.tokens ? inline(token.tokens) : token.text}</span>
    case 'escape':
      return <span key={key}>{token.text}</span>
    case 'strong':
      return <strong key={key} className="font-semibold">{inline(token.tokens)}</strong>
    case 'em':
      return <em key={key} className="italic">{inline(token.tokens)}</em>
    case 'del':
      return <del key={key} className="line-through">{inline(token.tokens)}</del>
    case 'codespan':
      return (
        <code key={key} className="rounded bg-bg-soft px-1 py-0.5 font-mono text-xs">{token.text}</code>
      )
    case 'br':
      return <br key={key} />
    case 'link': {
      const content = token.tokens ? inline(token.tokens) : token.text
      const href = safeHref(token.href)
      return href === null
        ? <span key={key}>{content}</span>
        : (
            <a
              key={key}
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent hover:underline"
            >{content}</a>
          )
    }
    case 'image':
      // The alt text only: loading the source would send a request to any host.
      return <span key={key}>{token.text}</span>
    case 'html':
      // The model's own markup, as text: never parsed into Nova's page.
      return <span key={key}>{token.raw}</span>
    default:
      return <span key={key}>{token.raw}</span>
  }
}

/** A list of inline tokens. */
function inline(tokens: Token[] | undefined): ReactNode {
  if (!tokens) return null
  return tokens.map((token, i) => inlineToken(token, i))
}

/** One block token as its element. */
function block(token: Token, key: number): ReactNode {
  switch (token.type) {
    case 'space':
    case 'def':
      return null
    case 'paragraph':
      return <p key={key}>{inline(token.tokens)}</p>
    case 'heading':
      // h1 to h6 are one style: bold, one size up from the panel's `text-sm`.
      return <p key={key} className="text-base font-semibold">{inline(token.tokens)}</p>
    case 'code': {
      const code = token as Tokens.Code
      const cls = 'whitespace-pre-wrap break-words rounded-[10px] border border-border bg-bg-soft/60 p-3 font-mono text-xs'
      return (
        <pre key={key} className={cls}>
          {code.lang === 'sql'
            ? tokenizeSql(code.text).map((part, i) => (
                <span key={i} data-token={part.kind} className={TOKEN_CLASS[part.kind]}>{part.text}</span>
              ))
            : code.text}
        </pre>
      )
    }
    case 'blockquote': {
      const quote = token as Tokens.Blockquote
      return (
        <blockquote key={key} className="border-l-2 border-border pl-3 text-text-dim">
          {quote.tokens.map(block)}
        </blockquote>
      )
    }
    case 'hr':
      return <hr key={key} className="border-border" />
    case 'list': {
      const list = token as Tokens.List
      const items = list.items.map((item, i) => <li key={i}>{item.tokens.map(block)}</li>)
      return list.ordered
        ? <ol key={key} className="list-decimal pl-5 space-y-1" start={list.start === '' ? undefined : list.start}>{items}</ol>
        : <ul key={key} className="list-disc pl-5 space-y-1">{items}</ul>
    }
    case 'table': {
      const table = token as Tokens.Table
      return (
        <div key={key} className="overflow-x-auto">
          <table className="border-collapse text-xs">
            <thead>
              <tr>
                {table.header.map((cell, i) => (
                  <th key={i} className="border border-border px-2 py-1 text-left font-semibold">{inline(cell.tokens)}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {table.rows.map((row, r) => (
                <tr key={r}>
                  {row.map((cell, c) => (
                    <td key={c} className="border border-border px-2 py-1 align-top">{inline(cell.tokens)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )
    }
    case 'html':
      return <p key={key} className="whitespace-pre-wrap break-words">{token.raw}</p>
    case 'text':
      return token.tokens
        ? <p key={key}>{inline(token.tokens)}</p>
        : <p key={key} className="whitespace-pre-wrap break-words">{token.text}</p>
    default:
      return <p key={key} className="whitespace-pre-wrap break-words">{token.raw}</p>
  }
}

/** Untrusted Markdown as React elements; never HTML. */
export function Markdown({ text }: { text: string }) {
  return <div className="space-y-2 break-words">{marked.lexer(text, { gfm: true }).map(block)}</div>
}
