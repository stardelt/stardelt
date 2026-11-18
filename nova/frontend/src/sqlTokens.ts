import type { SqlTokenKind } from './sqlHighlight'

/** The colour each token kind carries wherever a statement is painted (the editor, Ask). */
export const TOKEN_CLASS: Record<SqlTokenKind, string> = {
  keyword: 'text-accent', string: 'text-ok', comment: 'text-text-mute italic', number: 'text-warn', plain: 'text-text',
}
