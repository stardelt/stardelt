import { useMe } from '../hooks'
import { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api, type DeploymentRecord } from '../api'
import ErrorBox from '../components/ErrorBox'
import { SkeletonTable } from '../components/Skeleton'
import { shortRevision } from '../format'
import { formatWhen } from './Pipelines'

/**
 * One recorded bundle deploy (stardelt#102, slice 4): what it put on the stage, which pipelines
 * it wrote or removed, who deployed it, and the rendered documents a rollback replays. The rows
 * of the list page carry only a count, so this page is where a person reads the documents before
 * `stardelt bundle rollback --deployment <id>`. It also lists the bundle files and their digests,
 * so a person can compare a deploy with the commit it names. A rollback's page says which
 * deployment it restored.
 */

/** Kind and name of a rendered document; a document without them still shows up, labelled. */
function documentLabel(document: unknown): string {
  const doc = (document ?? {}) as { kind?: unknown; metadata?: { namespace?: unknown; name?: unknown } }
  const kind = typeof doc.kind === 'string' ? doc.kind : 'Document'
  const namespace = typeof doc.metadata?.namespace === 'string' ? doc.metadata.namespace : '—'
  const name = typeof doc.metadata?.name === 'string' ? doc.metadata.name : '—'
  return `${kind} ${namespace}/${name}`
}

export type PeerPromotion = {
  target: string
  id: number
  createdAt: string
  /** Files whose bytes differ in the peer (or that the peer does not have). */
  changed: string[]
  /** Files the peer has that this deploy does not. */
  added: string[]
}

/** Newest first: an RFC 3339 timestamp, then the id (two deploys can share a second). */
function newestFirst(a: DeploymentRecord, b: DeploymentRecord): number {
  if (a.createdAt !== b.createdAt) return a.createdAt < b.createdAt ? 1 : -1
  return b.id - a.id
}

/** For one deploy, the newest applied deploy of the same bundle in every other stage, and the
 * files whose digests differ. `[]` when the bundle reached no other stage. */
export function promotions(here: DeploymentRecord, all: DeploymentRecord[]): PeerPromotion[] {
  const mine = new Map((Array.isArray(here.files) ? here.files : []).map((file) => [file.path, file.sha256]))
  const newest = new Map<string, DeploymentRecord>()
  for (const other of Array.isArray(all) ? all : []) {
    if (other.id === here.id || other.target === here.target) continue
    if (other.bundle !== here.bundle || other.status !== 'applied') continue
    const kept = newest.get(other.target)
    if (!kept || newestFirst(kept, other) > 0) newest.set(other.target, other)
  }
  return [...newest.values()]
    .sort((a, b) => (a.target < b.target ? -1 : a.target > b.target ? 1 : 0))
    .map((peer) => {
      const changed: string[] = []
      const added: string[] = []
      const theirs = new Map((Array.isArray(peer.files) ? peer.files : []).map((file) => [file.path, file.sha256]))
      for (const [path, sha256] of mine) {
        if (theirs.get(path) !== sha256) changed.push(path)
      }
      for (const path of theirs.keys()) {
        if (!mine.has(path)) added.push(path)
      }
      return { target: peer.target, id: peer.id, createdAt: peer.createdAt, changed: changed.sort(), added: added.sort() }
    })
}

export default function DeploymentDetail() {
  const { id = '' } = useParams()
  const deploymentId = Number(id)
  const me = useMe()
  const keep401 = me.data ? me.data.auth_mode !== 'oidc' : false
  const navigate = useNavigate()
  const [confirming, setConfirming] = useState(false)
  const rollback = useMutation({
    mutationFn: () => api.deploymentRollback(deploymentId),
    onSuccess: (created) => navigate(`/deployments/${created.id}`),
  })

  const detail = useQuery({
    queryKey: ['deployment', deploymentId],
    queryFn: () => api.deployment(deploymentId, keep401),
    retry: false,
  })

  const records = useQuery({
    queryKey: ['deployment-records', keep401],
    queryFn: () => api.deploymentRecords(keep401),
    retry: false,
    staleTime: 30_000,
    enabled: Boolean(detail.data),
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
  const files = Array.isArray(row.files) ? row.files : []
  const peers = detail.data && Array.isArray(records.data) ? promotions(detail.data, records.data) : []
  const mayAct =
    me.data?.is_admin === true || (me.data?.groups ?? []).includes(row.ownerGroup)
  const canRollBack = row.status === 'applied' && resources.length > 0 && mayAct

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

      {row.previousDeploymentId != null && (
        <p className="text-sm text-text-mute">
          This is a rollback of{' '}
          <Link to={`/deployments/${row.previousDeploymentId}`} className="underline">
            deployment #{row.previousDeploymentId}
          </Link>
          .
        </p>
      )}

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

      <section aria-labelledby="deployment-files" className="space-y-2">
        <h2 id="deployment-files" className="text-lg font-semibold">Files</h2>
        <p className="text-sm text-text-mute">
          {files.length === 1 ? '1 bundle file' : `${files.length} bundle files`}
        </p>
        {files.length === 0 ? (
          <div className="card px-5 py-3 text-sm text-text-mute">This deploy recorded no bundle files</div>
        ) : (
          <div className="card relative overflow-x-auto">
            <table className="w-full text-sm">
              <caption className="sr-only">The bundle files this deploy read, with their sha256.</caption>
              <thead className="bg-bg-soft/50">
                <tr>
                  <th scope="col" className="th px-5 py-2.5">Path</th>
                  <th scope="col" className="th px-3 py-2.5">sha256</th>
                </tr>
              </thead>
              <tbody>
                {files.map((file) => (
                  <tr key={file.path} className="border-t border-border/70">
                    <td className="td px-5 font-mono text-xs break-words">{file.path}</td>
                    <td className="td px-3 font-mono text-xs" title={file.sha256}>{file.sha256.slice(0, 12)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {row.status === 'applied' && (
        <section aria-labelledby="deployment-promotion" className="space-y-2">
          <h2 id="deployment-promotion" className="text-lg font-semibold">
            Promotion
          </h2>
          {peers.length === 0 ? (
            <p className="text-sm text-text-mute">No deploy of this bundle reached another stage.</p>
          ) : (
            <ul className="space-y-2">
              {peers.map((peer) => {
                const differing = [...peer.changed, ...peer.added]
                return (
                  <li key={peer.target} className="card space-y-1 px-5 py-3 text-sm">
                    {differing.length === 0 ? (
                      <p className="text-text-dim">
                        The same bundle files reached {peer.target} (
                        <Link to={`/deployments/${peer.id}`} className="underline">
                          #{peer.id}
                        </Link>
                        )
                      </p>
                    ) : (
                      <>
                        <p className="text-text-dim">
                          Changed since {peer.target} (
                          <Link to={`/deployments/${peer.id}`} className="underline">
                            #{peer.id}
                          </Link>
                          ):
                        </p>
                        <p className="text-text-dim">
                          {peer.changed.length === 1
                            ? `1 file: ${peer.changed[0]}`
                            : peer.changed.length > 1
                              ? `${peer.changed.length} files`
                              : ''}
                          {peer.changed.length > 0 && peer.added.length > 0 && ' · '}
                          {peer.added.length > 0 && `${peer.added.length} more that only that stage has`}
                        </p>
                        <ul className="text-xs font-mono break-words">
                          {differing.slice(0, 5).map((path) => (
                            <li key={path}>{path}</li>
                          ))}
                        </ul>
                        {differing.length > 5 && (
                          <p className="text-xs text-text-mute">and {differing.length - 5} more</p>
                        )}
                      </>
                    )}
                  </li>
                )
              })}
            </ul>
          )}
        </section>
      )}

      <section aria-labelledby="deployment-rollback" className="space-y-2">
        <h2 id="deployment-rollback" className="text-lg font-semibold">Roll back</h2>
        {row.status !== 'applied' ? (
          <p className="text-sm text-text-mute">A failed deploy has nothing to replay.</p>
        ) : resources.length === 0 ? (
          <p className="text-sm text-text-mute">
            This deploy recorded no rendered documents, so there is nothing to replay.
          </p>
        ) : !mayAct ? (
          <p className="text-sm text-text-mute">
            Only the owner group's members ({row.ownerGroup}) and administrators may roll back.
          </p>
        ) : (
          <p className="text-sm text-text-dim">
            A rollback replays the {resources.length === 1 ? 'document' : `${resources.length} documents`} stored
            above, exactly as they ran, as you; it records a new deployment and deletes nothing.
          </p>
        )}
        {canRollBack && !confirming && (
          <button
            type="button"
            className="btn-primary w-fit"
            onClick={() => {
              rollback.reset()
              setConfirming(true)
            }}
          >
            Roll back
          </button>
        )}
        {canRollBack && confirming && (
          <div className="card space-y-3 border-amber-500/40 p-5" role="dialog" aria-label="Confirm rollback">
            <p className="text-sm font-semibold">
              Roll back {row.bundle} · {row.target} to deployment #{row.id}?
            </p>
            <p className="text-sm text-text-dim">
              The stored documents are replayed as you and recorded as a new deployment.
            </p>
            {rollback.isError && <ErrorBox error={rollback.error} onRetry={() => rollback.mutate()} />}
            <div className="flex gap-2">
              <button type="button" className="btn-primary" disabled={rollback.isPending} onClick={() => rollback.mutate()}>
                Confirm roll back
              </button>
              <button type="button" className="btn-ghost" onClick={() => setConfirming(false)}>
                Cancel
              </button>
            </div>
          </div>
        )}
        <p className="text-sm text-text-mute">
          From a terminal: <code className="font-mono text-xs">{`stardelt bundle rollback --deployment ${row.id}`}</code>
        </p>
      </section>
    </div>
  )
}
