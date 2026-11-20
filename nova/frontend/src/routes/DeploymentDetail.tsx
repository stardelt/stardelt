import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from 'react-router-dom'
import { api } from '../api'
import ErrorBox from '../components/ErrorBox'
import { SkeletonTable } from '../components/Skeleton'
import { shortRevision } from '../format'
import { formatWhen } from './Pipelines'

/**
 * One recorded bundle deploy (stardelt#102, slice 4): what it put on the stage, which pipelines
 * it wrote or removed, who deployed it, and the rendered documents a rollback replays. The rows
 * of the list page carry only a count, so this page is where a person reads the documents before
 * `stardelt bundle rollback --deployment <id>`.
 */

/** Kind and name of a rendered document; a document without them still shows up, labelled. */
function documentLabel(document: unknown): string {
  const doc = (document ?? {}) as { kind?: unknown; metadata?: { namespace?: unknown; name?: unknown } }
  const kind = typeof doc.kind === 'string' ? doc.kind : 'Document'
  const namespace = typeof doc.metadata?.namespace === 'string' ? doc.metadata.namespace : '—'
  const name = typeof doc.metadata?.name === 'string' ? doc.metadata.name : '—'
  return `${kind} ${namespace}/${name}`
}

export default function DeploymentDetail() {
  const { id = '' } = useParams()
  const deploymentId = Number(id)
  const me = useQuery({ queryKey: ['me'], queryFn: api.me })
  const keep401 = me.data ? me.data.auth_mode !== 'oidc' : false

  const detail = useQuery({
    queryKey: ['deployment', deploymentId],
    queryFn: () => api.deployment(deploymentId, keep401),
    retry: false,
  })

  if (detail.isLoading) {
    return (
      <div className="max-w-4xl space-y-5">
        <h1 className="text-2xl font-bold tracking-tight">Deployment</h1>
        <SkeletonTable rows={6} label="Loading the deployment" />
      </div>
    )
  }

  if (detail.isError && !detail.data) {
    return (
      <div className="max-w-2xl space-y-4">
        <h1 className="text-2xl font-bold tracking-tight">Deployment</h1>
        <div className="card space-y-3 p-5">
          <ErrorBox error={detail.error} onRetry={() => void detail.refetch()} />
          <div>
            <Link to="/deployments" className="btn-ghost w-fit">
              ← Deployments
            </Link>
          </div>
        </div>
      </div>
    )
  }

  const row = detail.data
  if (!row) return null

  const when = formatWhen(row.createdAt)
  const resources = Array.isArray(row.resources) ? row.resources : []

  return (
    <div className="max-w-4xl space-y-5">
      <p className="text-sm text-text-mute">
        <Link to="/deployments" className="underline">
          ← Deployments
        </Link>
      </p>

      <header className="min-w-0 space-y-1">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-2xl font-bold tracking-tight break-words">
            {row.bundle} · {row.target}
          </h1>
          {row.status === 'failed' ? (
            <span className="badge border-bad/40 bg-bad/10 text-bad">Failed</span>
          ) : (
            <span className="badge border-accent/40 bg-accent/10 text-accent">Applied</span>
          )}
        </div>
        <p className="flex flex-wrap items-center gap-x-2 gap-y-1 text-sm text-text-dim">
          <span className="font-mono text-xs" title={row.revision ?? undefined}>
            {row.revision ? shortRevision(row.revision) : '—'}
          </span>
          {row.repository && (
            <>
              <span aria-hidden className="text-text-mute">·</span>
              <a href={row.repository} target="_blank" rel="noreferrer" className="underline break-all">
                {row.repository}
              </a>
            </>
          )}
          <span aria-hidden className="text-text-mute">·</span>
          <span className="break-words">{row.actorName ?? row.actor}</span>
          <span aria-hidden className="text-text-mute">·</span>
          <span title={when.title}>{when.text}</span>
        </p>
      </header>

      <section aria-labelledby="deployment-pipelines" className="space-y-2">
        <h2 id="deployment-pipelines" className="text-lg font-semibold">Pipelines</h2>
        {row.pipelines.length === 0 ? (
          <div className="card px-5 py-3 text-sm text-text-mute">This deploy touched no pipeline</div>
        ) : (
          <div className="card relative overflow-x-auto">
            <table className="w-full text-sm">
              <caption className="sr-only">The pipelines this deploy wrote or removed.</caption>
              <thead className="bg-bg-soft/50">
                <tr>
                  <th scope="col" className="th px-5 py-2.5">Namespace</th>
                  <th scope="col" className="th px-3 py-2.5">Pipeline</th>
                  <th scope="col" className="th px-3 py-2.5">Action</th>
                </tr>
              </thead>
              <tbody>
                {row.pipelines.map((pipeline) => (
                  <tr key={`${pipeline.namespace}/${pipeline.name}`} className="border-t border-border/70">
                    <td className="td px-5 font-mono text-xs break-words">{pipeline.namespace}</td>
                    <td className="td px-3 font-mono text-xs break-words">{pipeline.name}</td>
                    <td className="td px-3">{pipeline.action}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section aria-labelledby="deployment-resources" className="space-y-2">
        <h2 id="deployment-resources" className="text-lg font-semibold">Resources</h2>
        <p className="text-sm text-text-mute">
          {resources.length === 1 ? '1 rendered document' : `${resources.length} rendered document(s)`}
        </p>
        {resources.length === 0 ? (
          <div className="card px-5 py-3 text-sm text-text-mute">
            A failed deploy stores no rendered documents
          </div>
        ) : (
          <div className="space-y-2">
            {resources.map((document, index) => (
              <details key={index} className="card px-5 py-3">
                <summary className="cursor-pointer font-mono text-xs break-words">
                  {documentLabel(document)}
                </summary>
                <pre className="text-xs overflow-x-auto">{JSON.stringify(document, null, 2)}</pre>
              </details>
            ))}
          </div>
        )}
      </section>

      <p className="text-sm text-text-dim">
        Roll back with <code className="font-mono text-xs">{`stardelt bundle rollback --deployment ${row.id}`}</code>{' '}
        (it replays these documents; #616).
      </p>
    </div>
  )
}
