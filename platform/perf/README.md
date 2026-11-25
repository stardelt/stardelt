# Trino baseline (perf)

Measures end-to-end **engine** wall time for three fixed TPC-H queries against
the Trino coordinator: the `POST /v1/statement` plus all `nextUri` paging, timed
with `time.monotonic()`. It is a repeatable baseline for the current node, not a
benchmark suite.

## What it measures / does not measure

- **Measures:** Trino statement latency on the `tpch` connector (planning,
  scheduling, execution, result paging) for `q1_aggregate`, `q6_filter`,
  `q3_join`.
- **Does not measure:** permission overhead, Iceberg/Lakekeeper, Spark, or the
  experience of a restricted user. The `tpch`/`tpcds` catalogs are
  **administrator-only by design** (`helm-values/trino-identity.yaml`), so the
  live run uses an administrator token and the numbers are for the engine only.

## Run

Mint an administrator token with the existing script (credentials stay with the
maintainer; never commit them):

```bash
MINT_ONLY=1 TOKEN_FILE=/tmp/perf.token \
  ISSUER=https://id.alpha.dev.stardelt.io/realms/stardelt \
  USER=<admin user> PASSWORD=<its password> \
  CLIENT_SECRET_FILE=<(kubectl -n stardelt get secret stardelt-oidc-clients -o jsonpath='{.data.trino}' | base64 -d) \
  python3 scripts/verify-trino-auth.py
TRINO_URL=https://trino.alpha.dev.stardelt.io TRINO_TOKEN="$(cat /tmp/perf.token)" \
  python3 perf/baseline.py
```

Or `make perf-baseline` with `TRINO_URL` and `TRINO_TOKEN` in the environment.

`PERF_CATALOG` (default `tpch`), `PERF_SCHEMA` (default `sf1`) and `PERF_RUNS`
(default `3`) are overridable; `sf10` needs only `PERF_SCHEMA=sf10` and no code
change. The first run of each query is discarded (cold JVM/cache/S3 listing);
with `PERF_RUNS=1` the single run is reported as-is.

`PERF_CONCURRENCY` (default `1,2,4,8`) adds a second table: N people each run
`q6_filter` `PERF_PER_WORKER` times (default 5) at the same moment, with
p50/p95/max and errors per level. `PERF_CONCURRENCY=0` skips it. The level where
p95 bends is the number the sizing guide uses.

Results are written to `perf/results/<UTC timestamp>.json` (git-ignored). Tables
for a release belong in `stardelt-internal`.

## The interactive path: `perf-smoke` and `perf-compare`

`make perf-smoke` measures the path a person actually feels — sign in, submit a
statement through Nova, poll, cancel — as the visitor `demo`, and writes one JSON
per run to `~/.cache/stardelt-perf/<UTC timestamp>.json`. It needs no `tpch`
catalog; the five fixed statements use the demo's `bronze` table and Trino's own
system views.

`perf/perf-baseline.json` is the committed reference: one recorded run whose
statement `p50`s a later run is compared against. `make perf-compare` reads the
newest JSON in that cache directory and prints one line per statement with the
ratio to the baseline (`--baseline` and `--latest` point it elsewhere).
Re-record the baseline only after a deliberate platform change that should move
the numbers, in its own pull request, with the reason in the commit message —
never to make a slow run look normal. A ratio in the tens while the node's load
average is high is a host fact, not a platform regression.

The comparison is advisory: a statement above `PERF_MAX_RATIO` (default `3.0`)
is printed as `SLOW` and the command still exits 0, because the dev node is
noisy. Set `PERF_STRICT=1` to make a slow statement fail the run; that is the
form a release gate would take.

## The Spark workload: `perf-spark`

`make perf-spark` is the fourth Sizing workload (#53 decisions 1(d) and 4):
**one fixed Spark Connect workload**, as the pod shape a notebook client has.
It starts a session against the shared Connect server and times five rows —
`session` (build to first use), `aggregate` (`count`, `sum`), `shuffle`
(`GROUP BY` + `ORDER BY` + `LIMIT`), `join` (self-join on a 1/50 slice) and
`write` (parquet, overwrite). Each row is `{"id", "ms", "rows", "ok"}`, and
`rowsPerSecond` is the generated row count over the slowest operation.

The data is **generated** with `spark.range`: a bare session on the shared
server has only `spark_catalog` and cannot address `gold.shop`, whose wiring is
the notebook helper's per-catalog config (`docs/spark.md`, "Catalog wiring"). An
Iceberg read therefore needs a session with that config and is not measured
here; that belongs to the session-identity work.

The write goes to `/tmp/perf-write` **inside the Connect server's pod**, and is
overwritten each run. It is never `saveAsTable`: the session catalog is shared
with every notebook on the server, so a table would be a leftover.

| variable | default | meaning |
|---|---|---|
| `NS` | `stardelt` | the namespace the client pod runs in |
| `TIMEOUT` | `240s` | how long the client pod may take |
| `PERF_SPARK_ROWS` | `5000000` | the row count of the generated data |
| `PERF_OUT_DIR` | `$HOME/.cache/stardelt-perf` | where the JSON goes |

The JSON goes to `<PERF_OUT_DIR>/spark-<UTC timestamp>.json` and carries the
operations and the row count (`perf-row:` lines are the reader's progress,
`perf-json:` the document, `perf-verdict:` the judgement):

```text
perf-row: session 540ms
perf-row: aggregate 805ms
perf-row: shuffle 2110ms
perf-row: join 1637ms
perf-row: write 2128ms
perf-spark: PASS (5 operations, 5000000 rows, 2370 rows/s, JSON at /home/<user>/.cache/stardelt-perf/spark-2026-11-25T00:12:34Z.json)
```

Exit 0 means all five operations finished; exit 1 names the ones that did not
(`perf-verdict: FAIL join`); exit 3 is `UNVERIFIED` — the client pod never
started or never reached the server, so nothing was measured. Timings are the
dev node's and vary with its load.

## Pipelines at once: `perf-pipelines`

`make perf-pipelines` is the third Sizing workload (#53 decisions 1(e) and 4): **N
different pipelines started at the same moment**, as one person, through Nova. It times
start to terminal state with `time.monotonic()`, from the `POST /api/pipelines/{namespace}/{name}/runs`
to the poll that saw `success`/`failed` — client wall-clock, not Airflow's own
`durationSeconds`. It needs no `tpch` catalog and writes nothing to the cluster beyond the
runs' own task pods; a DagRun is history, so there is no cleanup step.

Each slot of a level is a *different* pipeline, because a second start of one `concurrency:
Forbid` pipeline is refused: Nova answers `409 a run is already active` (measured
2026-11-25), which would measure its gate and not the platform. A level therefore never
repeats a name, and a duplicate in `PERF_PIPELINES` is an error.

| variable | default | meaning |
|---|---|---|
| `STARDELT_NOVA_URL` | `https://alpha.dev.stardelt.io` | Nova (a trailing `/` is trimmed) |
| `ISSUER` | `https://id.alpha.dev.stardelt.io/realms/stardelt` | token issuer |
| `PERF_PERSON` | `demo` | the person the run is started as (its password is `demo`, public on purpose) |
| `PERF_NAMESPACE` | `stardelt` | the pipeline's namespace |
| `PERF_PIPELINES` | `shop-1-bronze,shop-2-silver,shop-3-gold,shop-dbt` | the pipelines, one per slot of a level |
| `PERF_LEVELS` | `1,2` | runs at once, comma separated, each 1..10 (`0` runs nothing) |
| `PERF_TIMEOUT` | `900` | seconds one run may take before it counts as an error |
| `PERF_POLL` | `2` | seconds between polls |
| `PERF_OUT_DIR` | `$HOME/.cache/stardelt-perf` | where the JSON goes |

One line per level (`runs` started, `ok` reached `success`, then p50/p95/max of the
successful runs):

```text
level 1  runs=1  ok=1  p50=38.9s  p95=38.9s  max=38.9s
level 2  runs=2  ok=2  p50=41.2s  p95=44.7s  max=44.7s
perf-pipelines: PASS (3 run(s) as demo on shop-1-bronze, shop-2-silver)
```

The JSON goes to `<PERF_OUT_DIR>/pipelines-<UTC timestamp>.json` and never overwrites an
earlier one (`-2`, `-3` are added). It carries `"pipelines": […]` at the top level and, per
level, the same `"pipelines"` list aligned with `"states"`. Exit 0 means every run reached
`success`; exit 1 names the level, the state and the pipeline that did not, e.g.
`perf-pipelines: FAIL level 4: 1 run(s) did not succeed (failed on shop-dbt)`.

**No level above ten:** Nova refuses more than ten starts per user per minute (`429`), so a
bigger level measures the refusal, not the platform. A refused start is counted as an error
(`start-429`), not a crash. The level where p95 bends under a real team's schedule is the
number the sizing guide uses; tables for a release belong in `stardelt-internal`, like the
baseline's.

`python3 perf/pipelines.py --self-test` proves the measurement against a stub HTTP server:
no cluster, no token.

## Concurrent people: `perf-concurrency`

`make perf-concurrency` is the third Sizing workload (#53 decisions 1(a) and 4): **N statements
in flight through Nova as the visitor**, so the token check, the statement API and the engine
queue are all inside the measurement. That is the number neither the Trino baseline (an
administrator token straight against Trino) nor `perf-smoke` (one statement at a time) shows.

One level = `repeats` rounds; in a round, `level` threads start at the same instant on a
`threading.Barrier`, each submits `perf-smoke.sh`'s `cpu` statement (`POST /api/statements`,
`SELECT count(*), sum(…) FROM UNNEST(sequence(1, 1000)) × UNNEST(sequence(1, 1000))`, no table
and about 0.5 s alone) and polls `GET /api/statements/{id}?waitSeconds=10` until the state is
terminal. The clock is `time.monotonic()` from the POST to the terminal poll — **client
wall-clock**, like `perf-smoke.sh`, not Trino's own queue time. Every statement is freed with
`DELETE /api/statements/{id}` afterwards: a person holds at most ten.

**What it does not measure: N distinct people.** Every level is one token and one person, so
the per-person directory read happens once; a level of four is four statements, not four
sign-ins. A team of N people adds that one directory read per person and Nova's own
per-user limits (ten statements held, ten pipeline starts per minute). The JSON's `note` says
this too.

| variable | default | meaning |
|---|---|---|
| `STARDELT_NOVA_URL` | `https://alpha.dev.stardelt.io` | Nova (a trailing `/` is trimmed) |
| `ISSUER` | `https://id.alpha.dev.stardelt.io/realms/stardelt` | token issuer |
| `PERF_PERSON` | `demo` | the person every level is measured as (password `demo`, public on purpose) |
| `PERF_LEVELS` | `1,2,4,8` | statements in flight, comma separated, each 1..10 |
| `PERF_REPEATS` | `5` | rounds per level |
| `PERF_TIMEOUT` | `180` | seconds one statement may take before it counts as an error |
| `PERF_OUT_DIR` | `$HOME/.cache/stardelt-perf` | where the JSON goes |

The same values are flags (`--nova`, `--issuer`, `--person`, `--levels`, `--repeats`,
`--out-dir`); a flag wins over the variable. One line per level, over all its rounds:

```text
perf-concurrency: demo, cpu, 5 round(s)
level 1  p50=480ms p95=610ms max=640ms errors=0
level 4  p50=1120ms p95=1980ms max=2240ms errors=0
perf-concurrency: JSON at /home/<user>/.cache/stardelt-perf/concurrency-2026-11-25T00:12:34Z.json
```

`p50=-` means every run of that level failed. A `429` (`you already hold ten statements`), a
timeout and a failed statement are counted as `errors=N` for the level and do not abort the
run: the number is advisory, like `perf-compare`, and the command still exits 0. The JSON goes
to `<PERF_OUT_DIR>/concurrency-<UTC timestamp>.json` and never overwrites an earlier one (`-2`,
`-3` are added):

```json
{"timestamp": "2026-11-25T00:12:34Z", "nova": "https://alpha.dev.stardelt.io", "person": "demo",
 "statement": "cpu", "repeats": 5,
 "levels": [{"level": 1, "runs": 5, "p50": 480, "p95": 610, "max": 640, "errors": 0}],
 "note": "client wall-clock per statement through Nova; all levels are one signed-in person"}
```

`make perf-report` picks it up as the `## concurrency` block. Tables for a release belong in
`stardelt-internal`, like the baseline's.

## Memory high-water: `perf-highwater`

`make perf-highwater` answers what a workload **cost the node**: it wraps a command
(`perf/highwater.py -- python3 perf/pipelines.py`, so the default `1,2` levels — one run on
`shop-1-bronze`, then one each on `shop-1-bronze` and `shop-2-silver`) and samples
`kubectl -n <PERF_NAMESPACE> top pods` every `PERF_SAMPLE`
seconds (default `2`) until the child exits, then takes one final sample and the pods'
restart counts. One row per pod, sorted by peak memory:

```text
peak  pod                                              CPU(m)  MEM(MiB)  restarts  OOM
1590  spark-connect-6d9f8c9f7f-abcde                      40      1590         0  no
812   trino-coordinator-55c95f5bdd-26mwc                 120       812         0  no
highwater: 2 pods, 13 samples, JSON at ~/.cache/stardelt-perf/highwater-2026-11-25T00:20:00Z.json
```

| column | meaning |
|---|---|
| `peak` | the pod's peak memory in MiB — the order of the table |
| `CPU(m)` | peak CPU over the samples, in millicores |
| `MEM(MiB)` | peak memory over the samples, in MiB (MiB and KiB rows round up) |
| `restarts` | the sum of the containers' `restartCount` at the end |
| `OOM` | `yes` when a container's `lastState.terminated.reason` is `OOMKilled` |

A peak is the maximum over the samples, so a pod that disappears (a finished task pod)
keeps its last row instead of falling to zero. The JSON goes to
`<PERF_OUT_DIR>/highwater-<UTC timestamp>.json` with `samples` counting only the samples
that returned data, and the command exits with the child's exit code.

`kubectl top` needs metrics-server: a pod whose row reads `<unknown>` is skipped, not
counted as zero. `PERF_SAMPLE`, `PERF_LEVELS` and `PERF_OUT_DIR` pass through to the
harness and the child. `python3 perf/highwater.py --self-test` proves the parsing with
canned text: no cluster.

## Ingest rows per second: `perf-ingest`

`make perf-ingest` is the fifth Sizing workload (#53 decision 1(c) and 4): **rows per second
into Iceberg** as `demo`, through the path a pipeline writes with. A `%sql` task in a pipeline
runs on Trino, so the Trino write path is the product's ingest path today. One level = R rows
inserted into one Iceberg table, wall-clock with `time.monotonic()` from the first insert's
`POST /api/statements` to the last insert's terminal answer — the table reset and the
`count(*)` check are outside it.

Five measured facts shape the loop (lab, 2026-11-25):

- **`demo` may `CREATE TABLE`, `INSERT` and `DELETE FROM` in `playground.scratch`, but not
  `DROP TABLE`** (`PERMISSION_DENIED: Access Denied: Cannot drop table …`). The table is created
  if absent, emptied at the start and at the end, and left in place for `demo-reset`.
- **`sequence()` has a hard cap of 10 000 entries** (`INVALID_FUNCTION_ARGUMENT: result of
  sequence function must not have more than 10000 entries`). A level of R rows is therefore
  `ceil(R / 10000)` statements of at most 10 000 rows each, each with an `i + <offset>` shift.
- **A person holds at most ten statements**: an eleventh submit answers `429 you already hold
  ten statements; read or cancel one first`. Every statement is freed with
  `DELETE /api/statements/{id}` (204) before the next one.
- **The statement API is Nova's**: `POST /api/statements {"sql": …, "waitSeconds": 180}` →
  a `StatementView` with `statementId`, `state`, `rowCount`, `error` (a string) and `rows`;
  polls are `GET /api/statements/{id}?waitSeconds=10`; `state` is
  `PENDING|RUNNING|SUCCEEDED|FAILED`.
- **The token is minted exactly as `perf-pipelines` does it**: the public `demo` password in a
  0600 temp file through `scripts/mint-user-token.py`.

| variable | default | meaning |
|---|---|---|
| `STARDELT_NOVA_URL` | `https://alpha.dev.stardelt.io` | Nova (a trailing `/` is trimmed) |
| `ISSUER` | `https://id.alpha.dev.stardelt.io/realms/stardelt` | token issuer |
| `PERF_PERSON` | `demo` | the person (its password is `demo`, public on purpose) |
| `PERF_TABLE` | `playground.scratch.perf_ingest` | the Iceberg table (created if absent) |
| `PERF_ROWS` | `10000,50000` | rows per level, comma separated |
| `PERF_TIMEOUT` | `180` | seconds one statement may take |
| `PERF_OUT_DIR` | `$HOME/.cache/stardelt-perf` | where the JSON goes |

One line per level (`ok` is 1 only when every statement succeeded and `count` is `rows`),
then one summary line:

```text
rows=10000   ok=1  batches=1  count=10000  p50=…ms  rows/s=…
rows=50000   ok=1  batches=5  count=50000  p50=…ms  rows/s=…
perf-ingest: PASS (60000 rows in 3.1 s, 19354 rows/s as demo into playground.scratch.perf_ingest)
```

`p50` is the nearest rank of the individual batch durations (`pipelines.py::percentile`); the
`rows/s` of a level is `rows` over the whole level. The JSON goes to
`<PERF_OUT_DIR>/ingest-<UTC timestamp>.json` and never overwrites an earlier one (`-2`, `-3` are
added):

```json
{"timestamp": "2026-11-25T00:06:00Z", "nova": "https://alpha.dev.stardelt.io", "person": "demo",
 "table": "playground.scratch.perf_ingest",
 "levels": [{"level": 1, "rows": 10000, "ok": 1, "batches": 1, "count": 10000,
             "seconds": 0.476, "rowsPerSecond": 21008, "p50": 476}]}
```

Exit 0 means every level's count matched its rows; exit 1 names the level and the reason, e.g.
`perf-ingest: FAIL level 2: 1 statement(s) failed (INVALID_FUNCTION_ARGUMENT…)`. The table is
left empty for `demo-reset` (03:00 UTC, which clears `playground.scratch`).

`python3 perf/ingest.py --self-test` proves the batching, the time arithmetic, the
free-after-each-statement and the count check against a stub HTTP server: no cluster, no token.)

## The release table: `perf-report`

`make perf-report` reads the newest run of each workload from
`${PERF_OUT_DIR:-$HOME/.cache/stardelt-perf}` and prints one block per workload, so a
release's numbers are one command instead of six files opened by hand.

| writer | file | block |
|---|---|---|
| `scripts/perf-smoke.sh` | `<UTC>.json` | `## smoke` |
| `perf/pipelines.py` | `pipelines-<UTC>.json` | `## pipelines` |
| `perf/concurrency.py` | `concurrency-<UTC>.json` | `## concurrency` |
| `perf/ingest.py` | `ingest-<UTC>.json` | `## ingest` |
| `perf/spark.sh` | `spark-<UTC>.json` | `## spark` |
| `perf/highwater.py` | `highwater-<UTC>.json` | `## highwater` |

"Newest" is the timestamp in the file name; a tie goes to the later mtime, then the name. A
`pipelines-*.json` from before #1533 (one `pipeline` string instead of a list) is read too. Only
`PERF_OUT_DIR` is read: `perf/baseline.py` writes to `perf/results/` and needs an administrator
token, so it is not part of this table.

```text
$ make perf-report
## smoke  2026-11-24T21:11:57Z  person=demo  repeats=3
scan         runs=3  p50=460ms  p95=810ms  rows=1  SUCCEEDED
…
## ingest  2026-11-25T00:15:59Z  table=playground.scratch.perf_ingest
rows=10000  batches=1  count=10000  seconds=0.4s  rowsPerSecond=22321  p50=430ms
…
## pipelines  2026-11-24T23:41:06Z  person=demo
level 1  runs=1  ok=1  p50=15.0s  p95=15.0s  max=15.0s  shop-1-bronze
…
## highwater  2026-11-24T23:22:17Z  namespace=stardelt  samples=30
trino-coordinator-55c95f5bdd-l67rd  69m  1892Mi (!)
…
```

A budget line keeps its writer's own unit — milliseconds for `perf-smoke`,
`perf-concurrency` and the Spark
operations, seconds with one decimal for `perf-pipelines`, MiB and millicores for
`perf-highwater` — and a number that is `None` (a level where nobody succeeded) prints `-`.
`highwater` prints at most five pods, sorted by peak memory, and marks a row with a restart or an
`OOMKilled` with ` (!)`.

| flag | meaning |
|---|---|
| `--dir PATH` | where the runs are (default `$PERF_OUT_DIR` or `~/.cache/stardelt-perf`) |
| `--json PATH` | write the combined document as indented JSON, last line `perf-report: JSON at <PATH>` |
| `--self-test` | prove the readers against five fixtures in a temp dir; no cluster, no token |

A source with no run prints `## <name>: no run in <dir>` and is not an error on its own — the
table is as complete as the cache is. When no source has a run at all the command exits 1 with
`perf-report: FAIL no perf run in <dir> — run make perf-smoke (or another workload) first`.
The combined JSON from `--json` is what a release copies into `stardelt-internal`.
