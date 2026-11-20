/**
 * The environment marker a catalog carries (#103): `Production` when its `Catalog` resource
 * says `stardelt.io/environment: production`. The API sends only that one value and omits it
 * for every other catalog, so anything else renders nothing — a typo is no marker.
 */
export default function EnvironmentBadge({
  environment,
  className,
}: {
  environment?: string | null
  className?: string
}) {
  if (environment?.trim() !== 'production') return null
  return (
    <span
      className={[
        'badge border-bad/40 bg-bad/10 text-bad max-w-full break-words',
        className,
      ].filter(Boolean).join(' ')}
      title="A production catalog: a development target must not write to it (#103)"
    >
      Production
    </span>
  )
}
