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
