// API client for the Nova backend.
// Same-origin in production (backend serves static UI), proxied via vite dev server locally.
// Auth is a same-origin HttpOnly session cookie (oidc mode) — no tokens in JS.

import { redirectToLogin } from './auth'
import type { components } from './api-schema'

type Schema<K extends keyof components['schemas']> = components['schemas'][K]

/**
 * `/api/me` — the `oidc` claim set (never tokens). `dev` mode answers the smaller `Me` schema,
 * without the claims below; readers keep their `?.` and `??`, and the narrower union comes with
 * the ticket that migrates the remaining hand-written types.
 */
export type Me = Schema<'OidcMe'>

export type SavedQuery = Schema<'SavedQuery'>

export type Destination = Schema<'Destination'>

export type DestinationTest = Schema<'DestinationTest'>

export type Warehouse = { name: string; id: string }

/**
 * One SQL compute class (`GET /api/warehouses`, stardelt-nova#236): the Trino resource
 * group a query lands in. Not the catalog warehouse of `Warehouse` above — that is the
 * Iceberg REST catalog the SQL addresses (`me.warehouse`), this is the class the engine
 * schedules the query into.
 *
 * `size` is `xs`, `small`, `medium` or `large`; `null` for the synthetic administrator row,
 * which is not a class of its own. `running`, `queued` and `memoryShare` are the numbers
 * the class is rendered with (the class defaults, overridden by `spec.concurrency`).
 * `canUse` is non-authoritative: one of the caller's groups is granted the class. `yours`
 * is the class a query will land in — the largest usable one, or the default class.
 */
export type SqlWarehouse = Schema<'SqlWarehouse'>

/**
 * One catalog the caller may browse (`GET /api/catalog/catalogs`): the name the SQL
 * addresses and the Lakekeeper warehouse id behind it. A catalog a person may not
 * browse is not in the list at all — the engine's `SHOW CATALOGS` filters it.
 *
 * `layer` is the medallion layer Nova knows for the catalog (stardelt-nova#109):
 * `bronze`, `silver` or `gold`, from the `stardelt.io/layer` label on the `Catalog`
 * resource or the catalog's own name. The API omits it for every other catalog, and
 * sends the list already ordered bronze, silver, gold, then alphabetically.
 *
 * `owner`, `description` and `grants` come from the catalog's `Catalog` resource
 * (stardelt-nova#577): the owning group, the resource's description, and one
 * `(group, verb)` pair per declared grant, in the order the resource declares them.
 * Each is `null`/empty when the resource or the field is absent — a member sees no
 * guessed owner.
 */
export type Catalog = {
  name: string
  id: string
  layer?: string | null
  /** `production` when the Catalog carries stardelt.io/environment: production (#103). */
  environment?: string | null
  canWrite?: boolean
  owner?: string | null
  description?: string | null
  grants?: { group: string; verb: string }[]
  /** The newest finished maintenance run of the catalog's `Catalog` resource (operator#476). */
  maintenance?: { lastRun?: string; lastResult?: string; tablesDone?: number; tablesFailed?: number }
}

export type CatalogList = { catalogs: Catalog[]; default: string }

/** One table the catalog search found (`GET /api/catalog/search`). */
export type SearchMatch = Schema<'SearchMatch'>

/** `GET /api/catalog/search`: tables whose `namespace.table` contains every word of `q`. */
export type SearchResult = Schema<'SearchResult'>

/** `POST /api/samples/{name}` answer: `created` is false when the table was already there. */
export type SampleLoaded = Schema<'SampleLoaded'>

/**
 * One catalog's numbers (`GET /api/catalog/metrics`, stardelt-nova#249, #263), read from
 * the catalog's Iceberg metadata rather than counted by a query. `lastCommitMs` is the
 * newest commit among the loaded tables (null when none has one). `truncated` means
 * `records` and `sizeBytes` cover the first 200 tables while `tables` stays exact. `error`
 * is the sentence for a catalog whose last scan failed; its numbers are then the last good
 * ones, or zeros for a catalog that never answered, never omitted. `updatedAt` is when
 * those numbers were read (null while `pending`: no numbers yet, one scan is reading it).
 * `layer` and the row order are the catalog layers of stardelt-nova#109.
 */
export type CatalogStat = {
  name: string
  layer?: string | null
  schemas: number
  tables: number
  records: number
  sizeBytes: number
  lastCommitMs: number | null
  truncated: boolean
  error?: string
  updatedAt: number | null
  pending: boolean
}

/** The sums over the catalogs the caller may see; pending catalogs are left out. */
export type CatalogMetricsTotals = {
  catalogs: number
  schemas: number
  tables: number
  records: number
  sizeBytes: number
}

/** `GET /api/catalog/metrics`: one row per catalog the caller may browse, plus the sums. */
export type CatalogMetrics = {
  catalogs: CatalogStat[]
  totals: CatalogMetricsTotals
  generatedAt: number
}

export type NamespaceList = { namespaces: string[][] }

export type TableIdentifier = { namespace: string[]; name: string; kind?: 'table' | 'view' }
export type TableList = { identifiers: TableIdentifier[] }

export type IcebergField = {
  id: number
  name: string
  required: boolean
  type: string | object
}
export type IcebergSchema = { 'schema-id': number; fields: IcebergField[] }

export type TableMetadata = {
  'metadata-location': string
  metadata: {
    'table-uuid': string
    location: string
    'last-updated-ms': number
    'current-schema-id'?: number
    'current-snapshot-id'?: number | null
    /** The metadata's snapshots; only the current one's `summary` is read by the table page. */
    snapshots?: Array<{
      'snapshot-id': number
      'timestamp-ms'?: number
      summary?: Record<string, string | number>
    }>
    schemas: IcebergSchema[]
    'partition-specs': Array<{
      'spec-id': number
      fields: Array<{ name: string; transform: string; 'source-id': number }>
    }>
    properties?: Record<string, string>
  }
}

/** One column as the table documentation reports it: the current schema's name, type and `doc`. */
export type ColumnDoc = {
  name: string
  type: string
  description: string | null
}

/**
 * `GET /api/catalog/tables/{ns}/{tbl}/documentation` — the Iceberg `comment` as
 * `description`, the `stardelt.tags`/`stardelt.owner`/`stardelt.domain` properties,
 * and every field of the current schema.
 *
 * `canEdit`/`editRule` are Nova's interim rule: until the catalog enforces permissions,
 * Nova decides who may edit these properties. Anyone who reaches Lakekeeper or Trino
 * directly can still change them.
 */
export type TableDocumentation = {
  description: string | null
  tags: string[]
  owner: string | null
  domain: string | null
  /** The `stardelt.guide` property: how to use the table, in plain text, or `null`. */
  guide: string | null
  columns: ColumnDoc[]
  canEdit: boolean
  editRule: string
  canRollback: boolean
  rollbackRule: string
}

/**
 * A `PUT …/documentation` body. An absent key changes nothing; `null` removes the
 * property, and an empty tag list removes the tags.
 */
export type TableDocsUpdate = {
  description?: string | null
  tags?: string[] | null
  owner?: string | null
  domain?: string | null
  guide?: string | null
}

/** The freshness promise and the status it yields (#402 slice 2). */
export type TableFreshness = Schema<'TableFreshness'>
/** `by: null` removes the promise; an absent field changes nothing. */
export type FreshnessUpdate = Schema<'FreshnessUpdate'>

/** Every promised table of one catalog, red first (#402, #1867). */
export type CatalogHealth = Schema<'CatalogHealth'>
export type ListedHealth = Schema<'ListedHealth'>

/** One verified query: a question about a table that its owners answered (#1018, #912). */
export type VerifiedQuery = Schema<'VerifiedQuery'>

export type QueryColumn = { name: string; type: string }
export type QueryResult = Schema<'QueryResult'>

export type Job = Schema<'Job'>

export type UploadRequest = Schema<'UploadRequest'>
export type UploadResult = Schema<'UploadResult'>

export type TransferRequest = Schema<'TransferRequest'>
export type TransferResult = Schema<'TransferResult'>

export type TableRef = { ns: string; name: string }

export type PlatformApp = Schema<'PlatformApp'>

/** A workload in Nova's namespace, as the cluster reports it. */
export type Workload = Schema<'Workload'>

/**
 * `GET /api/workloads` — the namespace as Nova last managed to read it.
 *
 * An empty list alone is ambiguous ("nothing is running" or "nobody answered"), so the
 * backend says which: `ok` (read just now), `stale` (the read failed and `workloads` is
 * the last known good state, `age_seconds` old) or `unreachable` (never read it).
 * `state` is computed by the backend so the UI cannot disagree with the API about it.
 */
export type WorkloadSnapshot = {
  workloads: Workload[]
  /** Why the last read failed; null when it did not. */
  error: string | null
  /** Seconds since the last successful read; null when there never was one. */
  age_seconds: number | null
  state: 'ok' | 'stale' | 'unreachable'
}

/** One observed fact about an app, with where Nova read it. */
export type AppFact = Schema<'Fact'>

export type AppLink = { label: string; url: string }

/**
 * Whether a browser will let Nova frame the app's UI, as the app's own headers
 * report it. `unknown` means Nova could not read them — not the same as allowed.
 */
export type AppEmbed = {
  url: string
  state: 'allowed' | 'blocked' | 'unknown'
  blocked_by?: string
  detail?: string
  /** In-cluster page Nova read the headers from. */
  checked?: string
  /**
   * Sign-in host the app sends visitors without a session to (Keycloak), when
   * that is a different host from the app itself. That page refuses framing, so
   * the frame stays empty until the visitor signs in in a top-level tab.
   */
  sign_in_host?: string
}

/** `GET /api/apps/links` — the header's app switcher, safe for every signed-in user. */
export type AppShortcut = {
  id: string
  name: string
  category: string
  external_url: string
}

/** `GET /api/embeds` — just what a signed-in user needs to use the embedded apps. */
export type EmbedApp = {
  id: string
  name: string
  external_url: string
  status: PlatformApp['status']
  embed: AppEmbed
}

/** `GET /api/apps/:id` — the detail view of one platform app. */
export type AppDetail = Schema<'AppDetail'>

/**
 * One catalog as an administrator sees it (`GET /api/admin/catalogs`, stardelt-nova#251):
 * the `Catalog` resource's identity, owner, managed access, storage key prefix, phase and
 * what the access-sync job last wrote. Administrator-only — everyone else gets 403.
 * The credential reference and the bucket never leave the cluster, so they are not here.
 */
export type AdminCatalog = Schema<'AdminCatalog'>

/** One declared grant: a group and one verb (`browse`, `select`, `write`, `create`, `manage`). */
export type AdminCatalogGrant = { group: string; verb: string }

/** One condition of a catalog's status, reduced to the four fields the page shows. */
export type AdminCatalogCondition = Schema<'CatalogCondition'>

/** One recorded `/mcp` tool call (`GET /api/admin/mcp-audit`, stardelt-nova#970): never an argument, never a result. */
export type McpCall = Schema<'McpCall'>

/** The answer of `GET /api/admin/mcp-audit`: the calls, newest first. */
export type McpCalls = Schema<'McpCalls'>

/** A model endpoint an administrator connected (operator#609). No URL, no keyRef — by design. */
export type ModelEndpoint = {
  name: string
  model: string
  /** `on-premise` | `cloud`. */
  location: string
  purposes: string[]
  sendRows: boolean
  reachable?: boolean | null
  toolCalling?: boolean | null
  jsonOutput?: boolean | null
  contextTokens?: number | null
  checkedAt?: string | null
  message?: string | null
  /** `spec.quota.perGroupMonthlyTokens`: tokens one group may spend a month, if set. */
  perGroupMonthlyTokens?: number | null
  /** `spec.quota.perPersonDailyTokens`: tokens one person may spend a day, if set. */
  perPersonDailyTokens?: number | null
}

/** One table an Ask answer was written for. */
export type AskTable = Schema<'AskTable'>

/** The answer of `POST /api/ask` (design #891 slice a). No rows ever, and the SQL is not run. */
export type AskAnswer = {
  endpoint: string
  model: string
  /** `null` when no table matched. */
  sql: string | null
  sentence: string
  tables: AskTable[]
  /** `passed`, `failed` or `not_run`. */
  explain: string
  error?: string | null
  /** The `model_call` row the answer's last call wrote; the vote's target. */
  callId?: number | null
}

/** One conversation from the person's Ask History (`GET /api/ask/history`, design #891 slice c). */
export type AskConversation = {
  id: number
  question: string
  endpoint: string
  model: string | null
  callId: number | null
  sql: string | null
  sentence: string
  tables: AskTable[]
  explain: string
  error: string | null
  createdAt: string
}

/** The month's tokens for one group and endpoint (`GET /api/admin/model-usage`). */
export type ModelUsageRow = Schema<'UsageRow'>

/** One recorded model call: sizes, durations and hashes, never the conversation. */
export type ModelUsageCall = Schema<'UsageCall'>

/** The answer of `GET /api/admin/model-usage`: the month's totals and the last calls. */
export type ModelUsage = Schema<'ModelUsage'>

/**
 * The body of `PUT /api/admin/catalogs/{name}/access` (stardelt-nova#253): the whole
 * `spec.owner`, `spec.managedAccess` and `spec.grants` of one `Catalog`, plus the
 * `resourceVersion` the page read. A concurrent change answers 409 and never overwrites.
 */
export type CatalogAccessUpdate = Schema<'CatalogAccessUpdate'>

/**
 * The body of `POST /api/admin/teams` (stardelt-nova#596): one new team space. The group
 * must already exist in the company's sign-in provider — stardelt never creates groups;
 * `description` becomes `spec.description` of the production catalog only.
 */
export type NewTeamRequest = {
  /** 2 to 21 lower-case letters and digits, starting with a letter. */
  team: string
  /** The group the three catalogs belong to. */
  group: string
  /** `spec.description` of the production catalog, at most 500 characters. */
  description?: string
}

/**
 * What a created team space answers: `<team>`, `<team>lab` and `<team>ref` in that order,
 * and the pipeline principal `pipeline-<group>` (kept when it already existed).
 */
export type NewTeamResponse = Schema<'NewTeamResponse'>

/**
 * One row of `GET /api/connections` (stardelt-nova#171). A connection is a credential
 * for a system outside the platform; the page lists metadata only. `secretKeys` holds the
 * *names* of the secret fields — a value is never sent, by the store or by Nova, so the
 * UI cannot show one even if it tried.
 */
export type Connection = {
  name: string
  scope: 'personal' | 'group' | 'managed'
  type: string
  /** Group scope only: the group whose members may use and rotate it. */
  ownerGroup?: string
  /** Managed scope only: the groups allowed to use it. */
  usableBy?: string[]
  /** Non-secret settings (host, port, bucket, …) as the store keeps them. */
  config: Record<string, string>
  /** Names of the secret fields; the values are never sent. */
  secretKeys: string[]
  /**
   * Whether the caller belongs to the connection's scope. Always true for personal and managed
   * rows, and for a group row of one of the caller's own groups. `false` only on a group row an
   * administrator sees because they are an administrator (stardelt-nova#203): they may delete it
   * and read it through break-glass, but they cannot rotate it.
   */
  member: boolean
  /** ISO time of the last rotation. */
  lastRotated?: string
  version: number
  /** Group and managed scope only: whether the operator synced it. */
  sync?: { ready: boolean; reason: string; message?: string }
  /**
   * Group scope only: the last break-glass read, until a rotation clears it. The
   * owning group is told that an administrator read the credential and why.
   */
  breakGlass?: { by: string; at: string; reason: string }
}

/**
 * The one answer of a break-glass read (stardelt-nova#183): the credential's fields,
 * and when they were read. Never cached or logged; only the open dialog holds it.
 */
export type BreakGlassAnswer = {
  fields: Record<string, string>
  readAt: string
}

/**
 * The single state the Pipelines page shows (`GET /api/pipelines`, stardelt-nova#188).
 * The backend computes it so the UI cannot disagree with the API about it.
 */
export type PipelineState =
  | 'loading'
  | 'not-loaded'
  | 'paused'
  | 'running'
  | 'failed'
  | 'succeeded'
  | 'idle'

/**
 * One task as the `Pipeline` resource declares it. `sql` is the statement of a `kind: "sql"`
 * task; the read API does not send it yet (stardelt-nova#211 says it should), so the editor
 * treats it as absent and asks the person to type the statement until the API carries it.
 * A `kind: "notebook"` task carries the file name, the version it pins and its parameter
 * values — only on the detail route, as with `sql` (stardelt-nova#237).
 */
export type PipelineTaskSpec = {
  name: string
  kind: string
  sql?: string | null
  notebook?: string | null
  objectVersion?: string | null
  notebookParams?: Record<string, string> | null
  /** The `Connection` a `kind: "notebook"` task names; detail route only. */
  connection?: string | null
  /** The project of a `kind: "dbt"` task (`config.project`); detail route only. */
  dbtProject?: string | null
  /** The selector or the batched selectors, joined with `, `; detail route only. */
  dbtSelect?: string | null
  /** The bundle directory of a `kind: "spark-pipeline"` task; detail route only. */
  project?: string | null
  /** `incremental` or `full` of a `spark-pipeline` task; detail route only. */
  refresh?: string | null
  /** The datasets a full refresh rebuilds; detail route only. */
  fullRefresh?: string[] | null
  /** The tables a `wap` run audits (`catalog.schema.table`); detail route only (#114). */
  writes?: string[]
  /** The task names this task waits for; absent when it waits for nothing. */
  dependsOn?: string[]
}

/**
 * One published notebook, the newest version of it (`GET /api/notebooks`, stardelt-nova#225).
 * A schedule pins `objectVersion` exactly; `uploadedAt` is S3's ISO 8601 time.
 */
export type PublishedNotebook = {
  group: string
  name: string
  objectVersion: string
  size: number
  uploadedAt: string
  uploadedBy: string
  /** Groups that may read it without being members. */
  viewers?: string[]
}

/** Where a published version came from, as the publisher reported it (#819). A claim: Nova
 * stores it and does not check it against a Git server. */
export type NotebookSource = Schema<'NotebookSource'>

/** One immutable published version of a notebook (`GET …/versions`), newest first. */
export type NotebookVersion = {
  objectVersion: string
  size: number
  uploadedAt: string
  uploadedBy: string
  sha256: string
  /** Where the version came from; `null` when the publisher sent no X-Stardelt-Source header. */
  source: NotebookSource | null
}

/** One parameter of a notebook's tagged `parameters` cell: the name and its default as text. */
export type NotebookParameter = Schema<'NotebookParameter'>

/** `GET …/parameters?version=` — the parameters of one pinned version (stardelt-nova#237). */
export type NotebookParameters = Schema<'NotebookParameters'>

/** One pipeline that runs a published notebook (`GET /api/notebooks/{group}/{name}/usage`). */
export type NotebookScheduleRow = Schema<'NotebookScheduleRow'>
export type NotebookUsageResponse = Schema<'NotebookUsageResponse'>

/** A cron and its IANA zone, as `POST`/`PUT /api/pipelines` carries them. */
export type ScheduleSpec = { cron: string; timezone: string }

/**
 * The advanced options of a schedule (stardelt-nova#223). Every field is optional in a request:
 * an omitted one keeps its default (one retry, five minutes between retries, a two-hour time
 * limit, one run at a time). `Replace` is deliberately not offered — the loader runs it as
 * `Forbid`.
 */
export type PipelineOptions = Schema<'PipelineOptions'>

/** Who is told when a run ends (`spec.notify`, nova#815). Names of the group's destinations. */
export type PipelineNotify = Schema<'PipelineNotify'>

/** The options as the pipeline detail carries them, defaults filled in — what edit mode prefills. */
export type PipelineOptionsView = Schema<'PipelineOptionsView'>

/** One of the caller's groups and the pipeline principal that would run its schedules. */
export type PipelinePrincipal = {
  group: string
  /** `pipeline-<group>` when such a principal exists, else null. */
  principal: string | null
  ready: boolean
  /** Why not, when `ready` is false: the sentence the API refuses a schedule with. */
  message: string | null
}

/** One service principal of the caller's groups (`GET /api/service-principals`). */
export type ServicePrincipal = {
  name: string
  ownerGroup: string
  /** `pipeline`, `maintenance` or `collector`; it decides nothing by itself. */
  purpose: string
  /** What the principal's tokens are valid for: `trino`, `lakekeeper`. */
  audiences: string[]
  ready: boolean
  /** `null` when ready; otherwise why not. */
  message: string | null
}

/** A `dbt` run instead of a statement or a notebook (`#112`): exactly one of the three. */
export type DbtRequest = Schema<'DbtRequest'>

/**
 * One task of a task-list pipeline (`#905`): the name the engine uses, exactly one of a
 * statement, a notebook and a dbt run, and the tasks that must succeed first.
 */
export type PipelineTaskRequest = Schema<'PipelineTaskRequest'>

/** `POST /api/pipelines` (`Schedule`, stardelt-nova#210, options stardelt-nova#223). */
export type CreatePipelineRequest = Schema<'CreatePipelineRequest'>

/**
 * `PUT /api/pipelines/{namespace}/{name}`: the statement, the notebook or the task list, the
 * schedule, the parameters and the options. The body is the whole editable state — an omitted
 * `schedule` or `params` removes it and an omitted option keeps its default — so the dialog sends
 * all four. A pipeline never changes kind: exactly one of `sql`, `notebook` and `tasks` is given
 * (#237, #905).
 */
export type UpdatePipelineRequest = Schema<'UpdatePipelineRequest'>

/** The published notebook version a schedule pins (stardelt-nova#237). */
export type NotebookRequest = Schema<'NotebookRequest'>

/** `POST /api/pipelines/check`: what the engine says about a statement, without running it. */
export type CheckStatementRequest = Schema<'CheckStatementRequest'>

/**
 * The engine's verdict. `checked: false` means this kind of statement cannot be checked here
 * (then `ok` is absent). `ok: false` carries the engine's sentence in `message`.
 */
export type CheckStatementView = {
  checked: boolean
  ok?: boolean
  message?: string
}

/** `POST /api/pipelines/preview-schedule`: the next fire times of a cron in its zone. */
export type PreviewScheduleRequest = Schema<'PreviewScheduleRequest'>

/** The next fire times, RFC 3339 with the schedule's offset. */
export type PreviewScheduleView = { runs: string[] }

/**
 * One run, as the list and the detail share it. Times are Airflow's RFC 3339 strings;
 * `durationSeconds` is null until the run has ended and both times parsed.
 */
export type RunView = {
  runId: string
  /** `queued`, `running`, `success`, `failed`, `upstream_failed`, `skipped`, … */
  state: string
  /** Airflow's `run_type`: `scheduled`, `manual`, … */
  type: string
  logicalDate: string | null
  startedAt: string | null
  endedAt: string | null
  durationSeconds: number | null
  triggeredBy: string | null
  note: string | null
  /** The deployment a run came from: its bundle, target and Git revision (#105). */
  bundle?: string
  target?: string
  revision?: string
}

/** One run in the cross-pipeline list (`GET /api/runs`). `RunView`'s fields are flat in the row. */
export type RunsListRow = {
  namespace: string
  name: string
  /** The owning group; null means administrators only. */
  ownerGroup: string | null
  /** `sql`, `notebook`, or `pipeline` for a multi-step pipeline. */
  kind: 'sql' | 'notebook' | 'pipeline'
} & RunView
/** `GET /api/runs` — the runs of the caller's pipelines, newest first. `groups` is every
 *  group the caller can see, sorted; the filters do not narrow it (#750). */
export type RunsListResponse = { runs: RunsListRow[]; groups: string[] }

/** One row of the deployment record (`GET /api/deployments`, design nova#357): one bundle deploy. */
export type DeploymentRecord = {
  id: number
  bundle: string
  ownerGroup: string
  target: string
  /** The commit deployed, `-dirty` appended for uncommitted changes; null when it was not a checkout. */
  revision: string | null
  repository: string | null
  status: 'applied' | 'failed' | string
  /** The pipelines the deploy wrote or removed. */
  pipelines: { namespace: string; name: string; action: string }[]
  /** The rendered Pipeline documents this deploy applied, as sent (`resources`); a failed deploy sends none. */
  resources: unknown[]
  /** The files of the bundle this deploy read: its path inside the bundle and the sha256 of its bytes. */
  files: { path: string; sha256: string }[]
  /** The deployment this row replayed when it is a rollback; null for a deploy that was not one. */
  previousDeploymentId: number | null
  /** The `sub` of the person who recorded the deploy. */
  actor: string
  /** The name the sign-in provided (stardelt-nova#1094); null for a row recorded before it was stored. */
  actorName: string | null
  createdAt: string
}

/** One pipeline in the list: the resource plus what the scheduler says about it. */
/**
 * Where `stardelt bundle deploy` published a pipeline from (stardelt-nova#397): the bundle, the
 * target, the Git commit (`-dirty` appended for uncommitted changes), who deployed and when.
 */
export type DeploymentView = {
  bundle: string
  target: string
  revision?: string
  /** The bundle's repository (`https://host/path`), for the compare link. */
  repository?: string
  deployedBy?: string
  deployedAt?: string
  /** The preview this deploy belongs to (`stardelt bundle deploy --preview ID`), when it is one. */
  preview?: string
  /** When the preview copy is due to be removed, RFC 3339, when it is one. */
  expiresAt?: string
  /** True when the pipeline was changed after its last deploy (stardelt-nova drift check). */
  modified?: boolean
}

/** One deployed pipeline in a stage comparison (`GET /api/stages/compare`, nova#660). */
export type StageRow = {
  bundle: string
  target: string
  name: string
  revision?: string
  /** The bundle's repository (`https://host/path`), for the compare link. */
  repository?: string
  /** True when the pipeline was changed on that stage after its deploy. */
  modified?: boolean
}

/** `status` is `ok`, `unauthorized` (the peer refused this session's token) or `unreachable`. */
export type StageComparison = {
  peer: string
  peerUrl: string
  status: 'ok' | 'unauthorized' | 'unreachable' | string
  detail?: string
  here: StageRow[]
  there: StageRow[]
}

export type PipelineView = {
  name: string
  namespace: string
  /** The owning group; null means administrators only. */
  ownerGroup: string | null
  /** `spec.description`: one line about what the pipeline does; absent for most. */
  description?: string | null
  /** Whether the caller may run it and stop its runs; a viewer of a shared pipeline may run. */
  canRun: boolean
  /** Whether the caller may also pause, resume, change or delete it — owner group and admins. */
  canEdit: boolean
  /** The cron, with the time zone in parentheses when the resource has one. */
  schedule: string | null
  paused: boolean
  /** False while the scheduler has no DAG for the resource yet. */
  loaded: boolean
  nextRun: string | null
  state: PipelineState
  lastRun: RunView | null
  tasks: PipelineTaskSpec[]
  /** The service principal a run acts as. */
  runAs: string | null
  /** `Allow`, `Forbid` or `Replace`. */
  concurrency: string
  /** `direct`, or `wap` — the run writes an audit branch and publishes only when its checks pass. */
  publish: string

  createdAt: string | null
  /** Absent for a pipeline made in the interface or with `stardelt apply`. */
  deployment?: DeploymentView
}

/** One `StreamApp` (D42): a job that reads a Kafka topic and writes an Iceberg table. */
export type StreamView = {
  namespace: string
  name: string
  owner_group: string
  phase: 'Pending' | 'Running' | 'Stopped' | 'Failed' | string
  /** The operator's `Ready` reason; `Restarting` while a crashed job is being retried. */
  reason: string | null
  last_error: string | null
  /** How many times Spark has submitted the application; null until the operator reports it. */
  attempts: number | null
  /** When Spark last submitted the application (RFC 3339); null until the operator reports it. */
  last_submitted_at: string | null
  stopped: boolean
  source_connection: string
  source_topic: string
  sink_catalog: string
  sink_table: string
  sink_mode: string
  run_as: string | null
  /** The groups that may read the stream without being members (`spec.viewers`). */
  viewers: string[]
}

/** The body of PUT /api/streams/{namespace}/{name} (`StreamWrite`); camelCase like the server. */
export type StreamWriteBody = {
  name: string
  ownerGroup: string
  source: { connectionRef?: string | null; topic: string }
  sink: {
    catalog: string
    table: string
    mode?: string | null
    mergeKeys?: string[]
    checkpointPrefix?: string | null
  }
  restartPolicy?: string | null
  viewers?: string[]
}

/** One `SparkCluster`: Spark compute for a team, one person, or everyone (`shared`). */
export type ComputeCluster = Schema<'ComputeCluster'>

/**
 * The caller's own JupyterHub workspace (`GET /api/workspace`, stardelt-nova#653). `unavailable`
 * is a state, not an error: Nova has no hub credentials here or the hub does not answer, and the
 * page still renders. The times are RFC 3339 in UTC, or `null` while the hub reports none.
 */
export type WorkspaceView = Schema<'WorkspaceView'>

/** The body of PUT /api/admin/compute/{name}: everything an administrator may set. */
export type ComputeSpecBody = Schema<'ComputeSpecBody'>

/** `GET /api/pipelines/{namespace}/{name}`: the view plus the latest 20 runs. */
export type PipelineDetail = PipelineView & {
  runs: RunView[]
  params: Record<string, string>
  retries: { attempts: number; backoff: string | null } | null
  /**
   * The advanced options, defaults filled in (stardelt-nova#223). A backend that predates them
   * sends nothing; the dialog then starts from the defaults.
   */
  options?: PipelineOptionsView | null
  /** Who is told when a run ends; absent when the resource carries no `spec.notify`. */
  notify?: PipelineNotify | null
  airflowUrl?: string | null
}

/** The body of `POST …/runs`: a date and parameter values for this run only (nova#817). */
export type RunStart = Schema<'RunStart'>

/** The body of `POST …/backfills`: an inclusive range of days and how to treat dates that already ran (#117). */
export type BackfillStart = Schema<'BackfillStart'>
/** The answer of a start and of a cancel. */
export type BackfillView = Schema<'BackfillView'>
/** The answer of `GET …/backfills/{id}`: the backfill and the runs it created. */
export type BackfillStateView = Schema<'BackfillStateView'>

/** `POST /api/pipelines/validate`: what a create or an update of this document would do, without doing it. */
export type ValidatePipelineResponse = Schema<'ValidatePipelineResponse'>

/**
 * `GET /api/pipelines/{namespace}/{name}/spec`: the editable document of one pipeline, in the shape
 * of the body of `POST /api/pipelines` (stardelt-nova#383). `ownerGroup` and the statement are
 * absent for a pipeline whose shape the form cannot show.
 */
export type PipelineDocument = {
  name: string
  ownerGroup?: string
  sql?: string
  notebook?: NotebookRequest
  /**
   * The whole task list of a task-list pipeline (stardelt-nova#905); absent for a single-task
   * pipeline. An update replaces the list whole.
   */
  tasks?: PipelineTaskRequest[]
  schedule?: ScheduleSpec
  params?: Record<string, string>
  options: PipelineOptionsView
  /** Who is told when a run ends; absent when the resource carries no `spec.notify`. */
  notify?: PipelineNotify | null
}

export type PipelineSpec = {
  document: PipelineDocument
  resourceVersion: string | null
  /** `nova` when Nova wrote the pipeline, `other` for one made with kubectl or by GitOps. */
  managedBy: 'nova' | 'other'
  /** True only for a pipeline Nova wrote that has the shape the form edits. */
  editable: boolean
}

/** One task instance of a run. */
export type TaskView = Schema<'TaskView'>

/** `GET …/runs/{runId}`: one run and its task instances. */
export type RunResponse = { run: RunView; tasks: TaskView[] }

/** One statement a run's task sent to the engine (stardelt-nova#788 S5). */
export type RunQueryView = Schema<'RunQueryView'>

/** One check a run's `quality` task declares, with its verdict (stardelt#829). */
export type RunCheckView = {
  task: string
  name: string
  severity: string
  rows: number | null
  passed: boolean | null
}

/** One output of an executed code cell, as nbformat 4 stores it (#683). */
export type NotebookOutput = {
  output_type: string
  name?: string
  text?: string | string[]
  data?: Record<string, string | string[]>
  ename?: string
  evalue?: string
  traceback?: string[]
}

/** One cell of an executed notebook (`GET …/tasks/{taskId}/notebook`, #682). */
export type NotebookCell = {
  cell_type: string
  source: string | string[]
  outputs?: NotebookOutput[]
  execution_count?: number | null
}

/** The executed copy of a notebook a run wrote, outputs and all (#682). */
export type NotebookDoc = {
  cells: NotebookCell[]
  metadata?: Record<string, unknown>
}

/** One log message. Plain text: it is rendered as text, never as markup. */
export type LogLine = {
  timestamp: string | null
  level: string | null
  text: string
}

/** One page of a task's log; `nextToken` asks for the next page, `truncated` says it was cut. */
export type LogPage = {
  lines: LogLine[]
  nextToken: string | null
  truncated: boolean
}

/**
 * `POST /api/connections` body (stardelt-nova#173). `secret` holds the values; they are
 * written to the store and never returned. Casing matters: the backend reads
 * `ownerGroup`, `usableBy` and `refreshInterval`.
 */
export type NewConnectionRequest = {
  name: string
  type: string
  scope: Connection['scope']
  ownerGroup?: string
  usableBy?: string[]
  config: Record<string, string>
  secret: Record<string, string>
  refreshInterval?: string
  /** `secrets.mode: external`: the path in the enterprise's store; `secret` must then be `{}`. */
  remoteRef?: string
  /** `secrets.mode: external`: the field names the store entry must carry. */
  secretKeys?: string[]
}

/** A non-2xx answer from the backend; `status` lets callers tell 404 from 5xx. */
export class ApiError extends Error {
  status: number
  constructor(message: string, status: number) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

/** The sentence to show for any failed request: the API's own message, never a path or a status code. */
export function errorMessage(e: unknown): string {
  return e instanceof ApiError ? e.message : 'Something went wrong. Try again.'
}

/** The sentence for a status when the body carried none (an HTML error page from a proxy, an empty body). */
function statusSentence(status: number): string {
  if (status === 401) return 'Signing in…'
  if (status === 403) return 'You are not allowed to see this.'
  if (status === 404) return 'Not found.'
  return status >= 500 ? 'Something went wrong on the server. Try again.' : 'The request was refused.'
}

/** The backend answers errors as `{"error": "<message>"}`; any other body (an HTML page, an empty one) carries no sentence. */
function errorText(body: string): string {
  try {
    const parsed: unknown = JSON.parse(body)
    if (parsed && typeof parsed === 'object' && 'error' in parsed && typeof parsed.error === 'string') {
      return parsed.error
    }
  } catch {
    /* not JSON: it carries no sentence */
  }
  return ''
}

/** One request; a refused connection is Nova being unreachable, not the browser's TypeError text. */
async function send(path: string, init?: RequestInit): Promise<Response> {
  try {
    return await fetch(path, init)
  } catch {
    throw new ApiError('Nova is not reachable. Check your connection and try again.', 0)
  }
}

/** Parse a 2xx body as JSON; a non-JSON body (e.g. an HTML fallback page) gets a readable error. */
async function readJson<T>(path: string, r: Response): Promise<T> {
  try {
    return (await r.json()) as T
  } catch {
    console.debug('api', path, r.status)
    throw new ApiError('Nova answered with something unexpected. Try again.', r.status)
  }
}

/** A 401 means the session is missing or expired: go sign in, then come back. */
function checkAuth(path: string, r: Response) {
  if (r.status === 401) {
    console.debug('api', path, r.status)
    redirectToLogin()
    throw new ApiError('Signing in…', 401)
  }
}

async function jget<T>(path: string, keep401 = false): Promise<T> {
  const r = await send(path)
  // `keep401` is for a call whose 401 is an answer about the request, not a missing session:
  // the page shows the backend's sentence instead of leaving it for a sign-in round trip.
  if (!(keep401 && r.status === 401)) checkAuth(path, r)
  if (!r.ok) {
    console.debug('api', path, r.status)
    throw new ApiError(errorText(await r.text().catch(() => '')) || statusSentence(r.status), r.status)
  }
  return readJson<T>(path, r)
}

async function jsend<T>(method: 'POST' | 'PUT' | 'PATCH', path: string, body: unknown): Promise<T> {
  const r = await send(path, {
    method,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  checkAuth(path, r)
  if (!r.ok) {
    console.debug('api', path, r.status)
    throw new ApiError(errorText(await r.text().catch(() => '')) || statusSentence(r.status), r.status)
  }
  return readJson<T>(path, r)
}

async function jpost<T>(path: string, body: unknown): Promise<T> {
  return jsend<T>('POST', path, body)
}

async function jput<T>(path: string, body: unknown): Promise<T> {
  return jsend<T>('PUT', path, body)
}

async function jpatch<T>(path: string, body: unknown): Promise<T> {
  return jsend<T>('PATCH', path, body)
}

/** A delete answers 204 with no body; a failure carries the API's sentence like any other. */
async function jdel(path: string): Promise<void> {
  const r = await send(path, { method: 'DELETE' })
  checkAuth(path, r)
  if (!r.ok) {
    console.debug('api', path, r.status)
    throw new ApiError(errorText(await r.text().catch(() => '')) || statusSentence(r.status), r.status)
  }
}

/** A DELETE whose answer carries a body (a cancelled backfill answers `BackfillView`). */
async function jdelete<T>(path: string): Promise<T> {
  const r = await send(path, { method: 'DELETE' })
  checkAuth(path, r)
  if (!r.ok) {
    console.debug('api', path, r.status)
    throw new ApiError(errorText(await r.text().catch(() => '')) || statusSentence(r.status), r.status)
  }
  return readJson<T>(path, r)
}

/** A POST whose success is a `204` with no body (`jpost` reads JSON and would throw). */
async function jpostNoContent(path: string, body: unknown): Promise<void> {
  const r = await send(path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  checkAuth(path, r)
  if (!r.ok) {
    console.debug('api', path, r.status)
    throw new ApiError(errorText(await r.text().catch(() => '')) || statusSentence(r.status), r.status)
  }
}

/** A POST whose success is `204` and carries no body (the share routes). */
async function jsendNoContent(path: string, body: unknown): Promise<void> {
  const r = await send(path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  checkAuth(path, r)
  if (!r.ok) {
    console.debug('api', path, r.status)
    throw new ApiError(errorText(await r.text().catch(() => '')) || statusSentence(r.status), r.status)
  }
}

const seg = encodeURIComponent

/**
 * A catalog-scoped path. `?catalog=<name>` is appended only when a catalog is named, so a
 * call without one works in the default catalog exactly as it always has. The name is
 * encoded with `seg`: it comes from the API or the URL, never pasted into a request raw.
 */
function catalogPath(path: string, catalog?: string): string {
  return catalog ? `${path}?catalog=${seg(catalog)}` : path
}

/** One row of the query log list (`GET /api/query-log`). Times are epoch milliseconds. */
export type QueryLogItem = Schema<'QueryLogItem'>

export type QueryLogPage = { items: QueryLogItem[]; nextCursor: string | null }

/** One line of `GET /api/query-log/usage`: a person, a warehouse or a day, and what it cost. */
export type UsageRow = {
  key: string
  queries: number
  failed: number
  cpuMs: number
  wallMs: number
  inputBytes: number
  writtenBytes: number
  /** CPU time at the configured price, rounded to cents; `null` when no price is configured. */
  estimatedCost: number | null
}

/** `GET /api/query-log/usage` — the rows of the window and their total (its `key` is `total`). */
export type UsagePage = Schema<'UsagePage'>

/** Everything stored about one query (`GET /api/query-log/{id}`). */
export type QueryLogDetail = {
  id: string
  state: string
  user: string
  /** The person's display name; absent when Nova does not know the engine user. */
  userName?: string | null
  source: string | null
  clientTags: string[]
  warehouse: string
  catalog: string | null
  schema: string | null
  queryType: string | null
  sql: string
  sqlHash: string
  createdAt: number
  startedAt: number | null
  endedAt: number | null
  timing: {
    queuedMs: number | null
    planningMs: number | null
    executionMs: number | null
    wallMs: number | null
    cpuMs: number | null
  }
  data: {
    inputRows: number | null
    inputBytes: number | null
    outputRows: number | null
    outputBytes: number | null
    writtenRows: number | null
    writtenBytes: number | null
    peakMemoryBytes: number | null
  }
  tablesRead: string[]
  tablesWritten: string[]
  error: { code: string; message: string | null } | null
}

/** One operator of the engine's `statistics.operatorSummaries` (stardelt-nova#788 S1/S2). */
export type QueryProfileOperator = {
  operatorType: string
  planNodeId: string
  stageId: number
  pipelineId: number
  inputPositions: number
  outputPositions: number
  /** Engine-formatted durations ("3.74s", "12.59ms", "0.00ns"); parse with parseEngineDuration. */
  addInputCpu: string
  finishCpu: string
  getOutputCpu: string
  /** Engine-formatted sizes ("656B"); shown verbatim. Absent on some operators. */
  peakTotalMemoryReservation?: string
  spilledDataSize?: string
}

/** `GET /api/query-log/{id}/profile` (the stored profile of a finished query). */
export type QueryProfile = {
  plan: string | null
  operators: QueryProfileOperator[]
  spilledBytes: number
  completedSplits: number
  inputs: {
    catalogName?: string
    schema?: string
    table?: string
    physicalInputBytes?: number
    physicalInputRows?: number
  }[]
  /** True when the operator list was too large to keep (the plan may still be present). */
  truncated: boolean
  /** Deterministic hints (optional: a stub or an older backend may omit them). */
  hints?: QueryHint[]
}

/** One deterministic hint from `GET /api/query-log/{id}/profile` (stardelt-nova#788 S3a). */
export type QueryHint = {
  code: string
  title: string
  evidence: string
  /** A section of the query log guide, e.g. https://stardelt.io/docs/guides/query-log/#spill */
  docsUrl: string
}

/** One earlier run of the same statement (stardelt-nova#788 S4). */
export type QueryHistoryRun = {
  id: string
  state: string
  endedAt: number | null
  wallMs: number | null
  executionMs: number | null
  inputBytes: number | null
  inputRows: number | null
}

/** `GET /api/query-log/{id}/history` */
export type QueryHistory = Schema<'QueryHistory'>

export type QueryLogParams = {
  scope: 'mine' | 'team' | 'all'
  state?: string
  q?: string
  /** Epoch milliseconds: only queries created at or after this. */
  from?: number
  cursor?: string
  limit?: number
}

/** One page of a task's log; `tryNumber` and `token` are optional, as the backend's own query. */
export type PipelineLogParams = { tryNumber?: number; token?: string }

export type LineageNodeView = {
  id: string
  namespace: string
  name: string
  visible: boolean
  label?: string | null
  /** 0 for the focus table, negative upstream, positive downstream (visible nodes). */
  level?: number
  /** The newest event time of an edge that wrote this node. */
  lastWrite?: string
  /** True when every flow that touches this node came from an ad-hoc job. */
  handMade: boolean
  /** Read-only queries against this node in the window. */
  readers?: LineageReadersView
  /** The jobs that wrote this table, pipeline first; only on the focus node. */
  writers?: { kind: string; name: string; runs: number; lastRun: string }[]
}

export type LineageReadersView = {
  queries: number
  /** Distinct people (the SQL editor and notebooks), never a BI tool. */
  people: number
  lastRead: string
  /** The tools that read this table, most queries first. */
  tools?: string[]
}

/** One job that wrote an edge of the answer. */
export type LineageJobView = {
  id: string
  kind: string
  name: string
  runs: number
  lastRun: string
  /** True for every kind but a pipeline. */
  handMade: boolean
}

export type LineageEdgeView = {
  from: string
  to: string
  jobNamespace: string
  jobName: string
  runId: string
  eventTime: string
  /** The runs of this job that wrote this edge in the window. */
  runs: number
}

/** One table the caller read most, with its newest write (`GET /api/lineage/read-tables`). */
export type ReadTable = Schema<'ReadTable'>

/** `GET /api/lineage/read-tables`: the person's most-read tables, newest read first. */
export type ReadTables = Schema<'ReadTables'>

export type LineageGraphView = {
  nodes: LineageNodeView[]
  edges: LineageEdgeView[]
  jobs: LineageJobView[]
  /** The whole closure at any number of steps; the drawn graph keeps its depth. */
  impact: { downstream: number; upstream: number; downstreamJobs: number }
}

/** One source column; a source the caller may not read carries only `hidden`. */
export type ColumnSource = Schema<'ColumnSource'>

export type ColumnLineageEntry = Schema<'ColumnLineageEntry'>

export type ColumnLineage = Schema<'ColumnLineage'>

function pipelineLogUrl(
  namespace: string,
  name: string,
  runId: string,
  taskId: string,
  p: PipelineLogParams,
): string {
  const qs = new URLSearchParams()
  // `try=0` is refused by the API; omitting it lets the backend pick the task's last attempt.
  if (p.tryNumber != null && p.tryNumber > 0) qs.set('try', String(p.tryNumber))
  if (p.token) qs.set('token', p.token)
  const query = qs.toString()
  const base = `/api/pipelines/${seg(namespace)}/${seg(name)}/runs/${seg(runId)}/tasks/${seg(taskId)}/logs`
  return query ? `${base}?${query}` : base
}

/** The executed notebook of one task instance; `download` asks for the file, not the document. */
export function runNotebookUrl(
  namespace: string,
  name: string,
  runId: string,
  taskId: string,
  download = false,
): string {
  const base = `/api/pipelines/${seg(namespace)}/${seg(name)}/runs/${seg(runId)}/tasks/${seg(taskId)}/notebook`
  return download ? `${base}?download=true` : base
}

function queryLogUrl(p: QueryLogParams): string {
  const qs = new URLSearchParams({ scope: p.scope })
  if (p.state) qs.set('state', p.state)
  if (p.q) qs.set('q', p.q)
  if (p.from != null) qs.set('from', String(p.from))
  if (p.cursor) qs.set('cursor', p.cursor)
  qs.set('limit', String(p.limit ?? 50))
  return `/api/query-log?${qs.toString()}`
}

function queryUsageUrl(p: { scope: 'mine' | 'team' | 'all'; by: UsagePage['by']; from: number }): string {
  const qs = new URLSearchParams({ scope: p.scope, by: p.by, from: String(p.from) })
  return `/api/query-log/usage?${qs.toString()}`
}

/** One answer of `PUT /api/settings/{key}`: the value as the store now holds it. */
export type SettingAnswer = { key: string; value: unknown }

export const api = {
  me:           ()                            => jget<Me>('/api/me'),
  stagesCompare: (peer: string, keep401 = false) =>
    jget<StageComparison>(`/api/stages/compare?peer=${encodeURIComponent(peer)}`, keep401),
  /** The newest deployment rows the caller may see (`GET /api/deployments`), newest first, at most 50.
   * `keep401` is for dev auth mode: there is no sign-in there, so the 401 is a sentence about the
   * install, not a session to renew (nova#671). */
  deploymentRecords: (keep401 = false) => jget<DeploymentRecord[]>('/api/deployments', keep401),
  /** One recorded deploy (`GET /api/deployments/{id}`, design nova#357); 404 when it is not one of the caller's groups. */
  deployment: (id: number, keep401 = false) => jget<DeploymentRecord>(`/api/deployments/${id}`, keep401),
  /** Replay a recorded deploy (`POST /api/deployments/{id}/rollback`, design nova#357); 201 with the new row. */
  deploymentRollback: (id: number) => jpost<DeploymentRecord>(`/api/deployments/${id}/rollback`, {}),
  /** The caller's personal settings as one JSON object (`{}` when none). */
  settings:     ()                            => jget<Record<string, unknown>>('/api/settings'),
  putSetting:   (key: string, value: unknown) => jput<SettingAnswer>(`/api/settings/${seg(key)}`, value),
  deleteSetting: (key: string)                => jdel(`/api/settings/${seg(key)}`),
  savedQueries: ()                            => jget<SavedQuery[]>('/api/saved-queries'),
  saveQuery:   (req: { title: string; sql: string; warehouse: string }) =>
    jpost<SavedQuery>('/api/saved-queries', req),
  deleteSavedQuery: (id: number)              => jdel(`/api/saved-queries/${id}`),
  shareSavedQuery:   (id: number, group: string) => jsendNoContent(`/api/saved-queries/${id}/share`, { group }),
  unshareSavedQuery: (id: number, group: string) => jdel(`/api/saved-queries/${id}/share/${seg(group)}`),
  destinations: ()                            => jget<Destination[]>('/api/destinations'),
  createDestination: (req: { owner_group: string; name: string; kind: string; config: unknown }) =>
    jpost<Destination>('/api/destinations', req),
  deleteDestination: (id: number)             => jdel(`/api/destinations/${id}`),
  testDestination: (id: number)               => jpost<DestinationTest>(`/api/destinations/${id}/test`, {}),
  connections:  ()                            => jget<{ connections: Connection[]; store: boolean }>('/api/connections'),
  createConnection: (req: NewConnectionRequest) => jpost<Connection>('/api/connections', req),
  rotateConnection: (path: string, secret: Record<string, string>) => jput<Connection>(path, { secret }),
  deleteConnection: (path: string)           => jdel(path),
  /** A group connection read once by an administrator, with a reason the group will see. */
  breakGlassRead: (group: string, name: string, reason: string) =>
    jpost<BreakGlassAnswer>(`/api/connections/group/${seg(group)}/${seg(name)}/break-glass`, { reason }),
  trinoCluster: ()                            => jget<unknown>('/api/trino/cluster'),
  warehouse:    ()                            => jget<Warehouse>('/api/catalog/warehouse'),
  /** The SQL compute classes and the one the caller lands in (#236). */
  warehouses:   ()                            => jget<SqlWarehouse[]>('/api/warehouses'),
  /** Wake the engine for one class ahead of the first query (#1053): 202, `phase: "Starting"`. */
  warehouseStart: (name: string) => jpost<{ name: string; phase: string }>(`/api/warehouses/${encodeURIComponent(name)}/start`, {}),
  /** The catalogs the caller may browse and the one a request without `?catalog=` uses. */
  catalogs:     ()                            => jget<CatalogList>('/api/catalog/catalogs'),
  /** Tables whose name contains every word of `q`, in the default catalog. */
  searchTables: (q: string, limit = 8) =>
    jget<SearchResult>(`/api/catalog/search?q=${encodeURIComponent(q)}&limit=${limit}`),
  /** Schemas, tables, rows, bytes and the newest commit per catalog, plus the totals (#249). */
  catalogMetrics: ()                          => jget<CatalogMetrics>('/api/catalog/metrics'),
  /** Read every catalog again now (administrators; 429 when a minute has not passed). */
  refreshCatalogMetrics: ()                   => jpost<{ accepted: boolean }>('/api/catalog/metrics/refresh', {}),
  namespaces:   (catalog?: string)            => jget<NamespaceList>(catalogPath('/api/catalog/namespaces', catalog)),
  /** Load the one sample table as the caller into a catalog (#1816). `trips` is the only name. */
  loadSample:   (name: string, catalog?: string) =>
    jpost<SampleLoaded>(catalogPath(`/api/samples/${seg(name)}`, catalog), {}),
  tables:       (ns: string, catalog?: string) =>
    jget<TableList>(catalogPath(`/api/catalog/namespaces/${seg(ns)}/tables`, catalog)),
  tableMeta:    (ns: string, tbl: string, catalog?: string) =>
    jget<TableMetadata>(catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}`, catalog)),
  tableDocs:    (ns: string, tbl: string, catalog?: string) =>
    jget<TableDocumentation>(catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}/documentation`, catalog)),
  putTableDocs: (ns: string, tbl: string, body: TableDocsUpdate, catalog?: string) =>
    jput<TableDocumentation>(catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}/documentation`, catalog), body),
  tableFreshness: (ns: string, tbl: string, catalog?: string) =>
    jget<TableFreshness>(catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}/health`, catalog)),
  putTableFreshness: (ns: string, tbl: string, body: FreshnessUpdate, catalog?: string) =>
    jput<TableFreshness>(catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}/health`, catalog), body),
  /** Every promised table of one catalog, red first (#402, #1867). */
  catalogHealth: (catalog?: string) =>
    jget<CatalogHealth>(catalogPath('/api/catalog/health', catalog)),
  putColumnDoc: (ns: string, tbl: string, column: string, description: string | null, catalog?: string) =>
    jput<TableDocumentation>(
      catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}/columns/${seg(column)}/description`, catalog),
      { description },
    ),
  verifiedQueries: (ns: string, tbl: string, catalog?: string) =>
    jget<VerifiedQuery[]>(catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}/verified-queries`, catalog)),
  createVerifiedQuery: (
    ns: string,
    tbl: string,
    body: { question: string; sql: string; expected?: unknown[][] },
    catalog?: string,
  ) =>
    jpost<VerifiedQuery>(catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}/verified-queries`, catalog), body),
  deleteVerifiedQuery: (id: number) => jdel(`/api/verified-queries/${id}`),
  /** Run one verified query as the caller and compare it with its expected result (#912). */
  checkVerifiedQuery: (id: number) => jpost<VerifiedQuery>(`/api/verified-queries/${id}/check`, {}),
  /** Run every verified query of one table in one call, one after another (#912 slice 1b). */
  checkAllVerifiedQueries: (ns: string, tbl: string, catalog?: string) =>
    jpost<{ checked: number; passed: number; failed: number; items: VerifiedQuery[] }>(
      catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}/verified-queries/check`, catalog),
      {},
    ),
  tableHistory: (ns: string, tbl: string, catalog?: string) =>
    jget<QueryResult>(catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}/history`, catalog)),
  rollback:     (ns: string, tbl: string, snapshotId: string, catalog?: string) =>
    jpost<QueryResult>(catalogPath(`/api/catalog/tables/${seg(ns)}/${seg(tbl)}/rollback`, catalog), { snapshot_id: snapshotId }),
  query:        (sql: string)                 => jpost<QueryResult>('/api/query', { sql }),
  jobs:         ()                            => jget<Job[]>('/api/jobs'),
  queryLog:     (p: QueryLogParams)           => jget<QueryLogPage>(queryLogUrl(p)),
  queryUsage:   (p: { scope: 'mine' | 'team' | 'all'; by: UsagePage['by']; from: number }) =>
    jget<UsagePage>(queryUsageUrl(p)),
  queryLogDetail: (id: string)                => jget<QueryLogDetail>(`/api/query-log/${seg(id)}`),
  queryLogProfile: (id: string)               => jget<QueryProfile>(`/api/query-log/${seg(id)}/profile`),
  queryLogHistory: (id: string)               => jget<QueryHistory>(`/api/query-log/${seg(id)}/history`),
  pipelines:    ()                            => jget<{ pipelines: PipelineView[] }>('/api/pipelines'),
  streams:       ()                            => jget<{ namespace: string; streams: StreamView[] }>('/api/streams'),
  /** Stop or resume one stream; answers the updated view (`spec.stopped`, D42). */
  setStreamState: (namespace: string, name: string, stopped: boolean) =>
    jpatch<StreamView>(`/api/streams/${seg(namespace)}/${seg(name)}`, { stopped }),
  /** The refusal sentences of the body, without writing anything (`POST /api/streams/validate`). */
  validateStream: (body: StreamWriteBody) =>
    jpost<{ ok: boolean; errors: { message: string }[] }>('/api/streams/validate', body),
  /** Create or update one stream as its owner group (`PUT /api/streams/{ns}/{name}`). */
  saveStream: (namespace: string, body: StreamWriteBody) =>
    jput<StreamView>(`/api/streams/${seg(namespace)}/${seg(body.name)}`, body),
  /** The Spark clusters this caller may use (internal#161). */
  compute:        ()                            => jget<{ clusters: ComputeCluster[] }>('/api/compute'),
  startCompute:   (name: string)                => jpost<ComputeCluster>(`/api/compute/${seg(name)}/start`, {}),
  stopCompute:    (name: string)                => jpost<ComputeCluster>(`/api/compute/${seg(name)}/stop`, {}),
  ensureCompute:  (group: string)               => jpost<ComputeCluster>('/api/compute/ensure', { group }),
  saveCompute:    (name: string, body: ComputeSpecBody) => jput<ComputeCluster>(`/api/admin/compute/${seg(name)}`, body),
  deleteCompute:  (name: string)                => jdel(`/api/admin/compute/${seg(name)}`),
  /** The caller's own JupyterHub workspace (stardelt-nova#653). */
  workspace:      ()                            => jget<WorkspaceView>('/api/workspace'),
  startWorkspace: ()                            => jpost<WorkspaceView>('/api/workspace/start', {}),
  stopWorkspace:  ()                            => jpost<WorkspaceView>('/api/workspace/stop', {}),
  /** The caller's own groups and their principals (`Schedule`, #210). */
  pipelinePrincipals: ()                      => jget<{ groups: PipelinePrincipal[] }>('/api/pipelines/principals'),
  /**
   * Every run of the caller's pipelines, newest first; the server applies the filters.
   * `notebook` is `<group>/<name>`: the runs of every schedule that runs that notebook (#756).
   */
  runs: (
    opts: { limit?: number; state?: string; kind?: string; group?: string; notebook?: string } = {},
  ) => {
    const qs = new URLSearchParams()
    if (opts.limit !== undefined) qs.set('limit', String(opts.limit))
    if (opts.state) qs.set('state', opts.state)
    if (opts.kind) qs.set('kind', opts.kind)
    if (opts.group) qs.set('group', opts.group)
    if (opts.notebook) qs.set('notebook', opts.notebook)
    const query = qs.toString()
    return jget<RunsListResponse>(`/api/runs${query ? `?${query}` : ''}`)
  },
  servicePrincipals: ()                       => jget<{ principals: ServicePrincipal[] }>('/api/service-principals'),
  /** The latest published versions of the notebooks in the caller's groups (#225). */
  notebooks:    ()                            => jget<PublishedNotebook[]>('/api/notebooks'),
  /** One notebook's published versions, newest first, at most 50. */
  notebookVersions: (group: string, name: string) =>
    jget<NotebookVersion[]>(`/api/notebooks/${seg(group)}/${seg(name)}/versions`),
  /** The parameters of one published version; no `version` means the latest (#237). */
  notebookParameters: (group: string, name: string, version?: string) => {
    const qs = version ? `?${new URLSearchParams({ version }).toString()}` : ''
    return jget<NotebookParameters>(`/api/notebooks/${seg(group)}/${seg(name)}/parameters${qs}`)
  },
  /** The stored bytes of one published version, for the preview; no `version` means the latest (#684). */
  notebookContent: (group: string, name: string, version?: string) => {
    const qs = version ? `?${new URLSearchParams({ version }).toString()}` : ''
    return jget<NotebookDoc>(`/api/notebooks/${seg(group)}/${seg(name)}/content${qs}`)
  },
  /** The pipelines that run one published notebook — its schedules and pinned versions (#517). */
  notebookUsage: (group: string, name: string) =>
    jget<NotebookUsageResponse>(`/api/notebooks/${seg(group)}/${seg(name)}/usage`),
  /** Fill the placeholders and ask the engine to plan the statement: nothing runs (#223). */
  checkStatement: (req: CheckStatementRequest) => jpost<CheckStatementView>('/api/pipelines/check', req),
  /** The next fire times of a cron in its zone, at most ten (#223). */
  previewSchedule: (req: PreviewScheduleRequest) => jpost<PreviewScheduleView>('/api/pipelines/preview-schedule', req),
  createPipeline: (req: CreatePipelineRequest) => jpost<PipelineView>('/api/pipelines', req),
  updatePipeline: (ns: string, name: string, req: UpdatePipelineRequest) =>
    jput<PipelineView>(`/api/pipelines/${seg(ns)}/${seg(name)}`, req),
  deletePipeline: (ns: string, name: string)  => jdel(`/api/pipelines/${seg(ns)}/${seg(name)}`),
  pipeline:     (ns: string, name: string)    => jget<PipelineDetail>(`/api/pipelines/${seg(ns)}/${seg(name)}`),
  validatePipeline: (req: CreatePipelineRequest) => jpost<ValidatePipelineResponse>('/api/pipelines/validate', req),
  pipelineSpec: (ns: string, name: string)    => jget<PipelineSpec>(`/api/pipelines/${seg(ns)}/${seg(name)}/spec`),
  startPipelineRun: (ns: string, name: string, start: RunStart = {}) =>
    jpost<RunView>(`/api/pipelines/${seg(ns)}/${seg(name)}/runs`, start),
  startBackfill: (ns: string, name: string, body: BackfillStart) =>
    jpost<BackfillView>(`/api/pipelines/${seg(ns)}/${seg(name)}/backfills`, body),
  backfillState: (ns: string, name: string, id: number) =>
    jget<BackfillStateView>(`/api/pipelines/${seg(ns)}/${seg(name)}/backfills/${id}`),
  cancelBackfill: (ns: string, name: string, id: number) =>
    jdelete<BackfillView>(`/api/pipelines/${seg(ns)}/${seg(name)}/backfills/${id}`),
  pausePipeline: (ns: string, name: string)   => jpost<PipelineView>(`/api/pipelines/${seg(ns)}/${seg(name)}/pause`, {}),
  resumePipeline: (ns: string, name: string)  => jpost<PipelineView>(`/api/pipelines/${seg(ns)}/${seg(name)}/resume`, {}),
  pipelineRun:  (ns: string, name: string, runId: string) =>
    jget<RunResponse>(`/api/pipelines/${seg(ns)}/${seg(name)}/runs/${seg(runId)}`),
  pipelineRunQueries: (namespace: string, name: string, runId: string) =>
    jget<{ queries: RunQueryView[] }>(`/api/pipelines/${seg(namespace)}/${seg(name)}/runs/${seg(runId)}/queries`),
  /** The checks the run's `quality` tasks declare (#829); `rows` is null until the task's log has the line. */
  pipelineRunChecks: (namespace: string, name: string, runId: string) =>
    jget<{ checks: RunCheckView[]; auditBranch?: string; auditedTables?: string[] }>(
      `/api/pipelines/${seg(namespace)}/${seg(name)}/runs/${seg(runId)}/checks`,
    ),
  stopPipelineRun: (ns: string, name: string, runId: string) =>
    jpost<RunView>(`/api/pipelines/${seg(ns)}/${seg(name)}/runs/${seg(runId)}/stop`, {}),
  retryPipelineRun: (ns: string, name: string, runId: string) =>
    jpost<RunView>(`/api/pipelines/${seg(ns)}/${seg(name)}/runs/${seg(runId)}/retry`, {}),
  pipelineLog:  (ns: string, name: string, runId: string, taskId: string, p: PipelineLogParams = {}) =>
    jget<LogPage>(pipelineLogUrl(ns, name, runId, taskId, p)),
  /** The executed copy of a notebook task's run (#682), outputs and traceback included. */
  runNotebook: (ns: string, name: string, runId: string, taskId: string) =>
    jget<NotebookDoc>(runNotebookUrl(ns, name, runId, taskId)),
  apps:         ()                            => jget<PlatformApp[]>('/api/apps'),
  embeds:       ()                            => jget<EmbedApp[]>('/api/embeds'),
  links:        ()                            => jget<AppShortcut[]>('/api/apps/links'),
  app:          (id: string)                  => jget<AppDetail>(`/api/apps/${seg(id)}`),
  /** Every catalog of the platform namespace with its grants (administrators only, #251). */
  adminCatalogs: ()                           => jget<AdminCatalog[]>('/api/admin/catalogs'),
  /** The last `/mcp` tool calls, newest first (administrators only, #970). */
  mcpAudit: (p: { limit?: number; sub?: string; tool?: string } = {}) => {
    const qs = new URLSearchParams()
    if (p.limit != null) qs.set('limit', String(p.limit))
    if (p.sub) qs.set('sub', p.sub)
    if (p.tool) qs.set('tool', p.tool)
    return jget<McpCalls>(`/api/admin/mcp-audit${qs.size ? `?${qs}` : ''}`)
  },
  /** The model endpoints an administrator connected, without their URL or key. */
  modelEndpoints: () => jget<ModelEndpoint[]>('/api/model-endpoints'),
  /** A question in, checked SQL out (design nova#891). Metadata only ever leaves the platform. */
  ask:          (question: string)                => jpost<AskAnswer>('/api/ask', { question }),
  /** The caller's own last 20 Ask conversations, newest first. The body is a bare array. */
  askHistory:   ()                                => jget<AskConversation[]>('/api/ask/history'),
  /** 👍 or 👎 on one recorded call; the caller's own row only. */
  askFeedback:  (callId: number, vote: 'up' | 'down') =>
    jpostNoContent('/api/ask/feedback', { callId, vote }),
  /** The month's tokens per group and endpoint, and the last calls (admins only). */
  modelUsage: (p: { limit?: number; group?: string } = {}) => {
    const qs = new URLSearchParams()
    if (p.limit != null) qs.set('limit', String(p.limit))
    if (p.group) qs.set('group', p.group)
    return jget<ModelUsage>(`/api/admin/model-usage${qs.size ? `?${qs}` : ''}`)
  },
  /** Probe one endpoint now; 503 carries `Nova has no cluster connection`. */
  probeModelEndpoint: (name: string) => jpost<ModelEndpoint>(`/api/model-endpoints/${seg(name)}/probe`, {}),
  /** Change one catalog's owner, managed access and grants (#253); the resource stays the source. */
  putCatalogAccess: (name: string, body: CatalogAccessUpdate) =>
    jput<AdminCatalog>(`/api/admin/catalogs/${seg(name)}/access`, body),
  /** Create a new team space: three catalogs and the group's pipeline principal (#596). */
  createTeam: (body: NewTeamRequest) => jpost<NewTeamResponse>('/api/admin/teams', body),
  workloads:    ()                            => jget<WorkloadSnapshot>('/api/workloads'),
  createNamespace: (namespace: string, catalog?: string) =>
    jpost<{ namespace: string }>(catalogPath('/api/catalog/namespaces', catalog), { namespace }),
  upload:       (req: UploadRequest)          => jpost<UploadResult>('/api/ingest/upload', req),
  transfer:     (req: TransferRequest)        => jpost<TransferResult>('/api/transfer', req),
  lineage:      (params: { catalog?: string; schema?: string; table?: string; depth?: number; days?: number }) => {
    const qs = new URLSearchParams()
    if (params.catalog) qs.set('catalog', params.catalog)
    if (params.schema) qs.set('schema', params.schema)
    if (params.table) qs.set('table', params.table)
    if (params.depth != null) qs.set('depth', String(params.depth))
    if (params.days != null) qs.set('days', String(params.days))
    const query = qs.toString()
    return jget<LineageGraphView>(query ? `/api/lineage?${query}` : '/api/lineage')
  },
  /** Where each column of one table comes from (stardelt-nova#650). */
  lineageColumns: (catalog: string, schema: string, table: string) =>
    jget<ColumnLineage>(`/api/lineage/columns?${new URLSearchParams({ catalog, schema, table }).toString()}`),
  /** The tables this person read most in the window, with their newest write (nova#510). */
  readTables: (days = 30) =>
    jget<ReadTables>(`/api/lineage/read-tables?days=${encodeURIComponent(String(days))}`),
}

/**
 * Every table across every namespace of one catalog (small catalogs; fine for the MVP).
 * Without a catalog the API's default is used, exactly as before several catalogs existed.
 */
export async function allTables(catalog?: string): Promise<TableRef[]> {
  const { namespaces } = await api.namespaces(catalog)
  const refs: TableRef[] = []
  await Promise.all(
    namespaces.map(async parts => {
      const ns = parts.join('.')
      const { identifiers } = await api.tables(ns, catalog)
      for (const id of identifiers) refs.push({ ns, name: id.name })
    }),
  )
  return refs.sort((a, b) => `${a.ns}.${a.name}`.localeCompare(`${b.ns}.${b.name}`))
}

export function formatCell(c: unknown): string {
  if (c === null || c === undefined) return '∅'
  if (typeof c === 'string') return c
  return JSON.stringify(c)
}

export function downloadCsv(filename: string, columns: string[], rows: unknown[][]) {
  const escape = (v: unknown) => {
    // Arrays / ROWs / maps from the engine would otherwise export as "[object Object]".
    const s = v === null || v === undefined ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v)
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
  }
  const body = [columns.map(escape).join(','), ...rows.map(r => r.map(escape).join(','))].join('\n')
  const blob = new Blob([body], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  // Attached while clicking (older Firefox ignores detached anchors); the URL is
  // revoked on a later tick so the browser has started the download first.
  document.body.appendChild(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}
