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

## Pipelines at once: `perf-pipelines`

`make perf-pipelines` is the third Sizing workload (#53 decisions 1(e) and 4): **N
pipeline runs started at the same moment**, as one person, through Nova. It times
start to terminal state with `time.monotonic()`, from the `POST /api/pipelines/{namespace}/{name}/runs`
to the poll that saw `success`/`failed` — client wall-clock, not Airflow's own
`durationSeconds`. It needs no `tpch` catalog and writes nothing to the cluster beyond the
runs' own task pods; a DagRun is history, so there is no cleanup step.

| variable | default | meaning |
|---|---|---|
| `STARDELT_NOVA_URL` | `https://alpha.dev.stardelt.io` | Nova (a trailing `/` is trimmed) |
| `ISSUER` | `https://id.alpha.dev.stardelt.io/realms/stardelt` | token issuer |
| `PERF_PERSON` | `demo` | the person the run is started as (its password is `demo`, public on purpose) |
| `PERF_NAMESPACE` | `stardelt` | the pipeline's namespace |
| `PERF_PIPELINE` | `shop-1-bronze` | the pipeline |
| `PERF_LEVELS` | `1,2` | runs at once, comma separated, each 1..10 (`0` runs nothing) |
| `PERF_TIMEOUT` | `900` | seconds one run may take before it counts as an error |
| `PERF_POLL` | `2` | seconds between polls |
| `PERF_OUT_DIR` | `$HOME/.cache/stardelt-perf` | where the JSON goes |

One line per level (`runs` started, `ok` reached `success`, then p50/p95/max of the
successful runs):

```text
level 1  runs=1  ok=1  p50=38.9s  p95=38.9s  max=38.9s
level 2  runs=2  ok=2  p50=41.2s  p95=44.7s  max=44.7s
perf-pipelines: PASS (3 run(s) as demo on shop-1-bronze)
```

The JSON goes to `<PERF_OUT_DIR>/pipelines-<UTC timestamp>.json` and never overwrites an
earlier one (`-2`, `-3` are added). Exit 0 means every run reached `success`; exit 1 names
the level and the state that did not, e.g. `perf-pipelines: FAIL level 4: 1 run(s) did not
succeed (failed)`.

**No level above ten:** Nova refuses more than ten starts per user per minute (`429`), so a
bigger level measures the refusal, not the platform. A refused start is counted as an error
(`start-429`), not a crash. The level where p95 bends under a real team's schedule is the
number the sizing guide uses; tables for a release belong in `stardelt-internal`, like the
baseline's.

`python3 perf/pipelines.py --self-test` proves the measurement against a stub HTTP server:
no cluster, no token.
