# dbt tool image

`ghcr.io/stardelt/dbt:demo` — dbt-core with the Trino adapter, for running a dbt project
against the platform's engine. Nothing is linked into stardelt code: a pipeline mounts its
dbt project and runs this image, and the sample project ships inside it (below).

- `dbt-core==1.11.17`, `dbt-trino==1.10.6`, in their own virtualenv at `/opt/dbt`
  (`1.12.x` needs an sdist-only parser, so `--only-binary=:all:` refuses it; revisit when dbt
  ships wheels for `dbt-core-experimental-parser`).
- Base `python:3.12-slim`, pinned by digest. Measured image size **386,043,978 bytes**
  (368 MiB, with the sample project; 2026-11-19); the budget is **450 MiB** (471,859,200 bytes)
  and the CI build is not size-gated, so keep the number here in step with reality after a bump.
- `dbt`'s `logs/` and `target/` are redirected to `/tmp` (`DBT_LOG_PATH`, `DBT_TARGET_PATH`):
  a pipeline mounts its project read-only, or root-owned.
- Anonymous usage stats are off, colours off, the container runs as uid 1000 with `/work` as
  its working directory.

## The sample project in the image

`/opt/dbt/projects/shop` is a copy of `demos/examples/shop`, so a `dbt` task of a `Pipeline`
names it with `config.project: shop` and the demo needs no checkout. It is root-owned and
read-only for the pod (dbt writes only `/tmp`). Customer projects from Git or a pinned archive
are not part of this image; the image digest is the pin for the sample.

## The wrapper, and the four variables

The entrypoint is `/usr/local/bin/stardelt-dbt`. It requires, and never prints a value of:

| Variable | Meaning |
|---|---|
| `PIPELINE_TOKEN_URL` | the identity provider's token endpoint (public HTTPS) |
| `PIPELINE_CLIENT_ID` | the pipeline's service-principal client id |
| `PIPELINE_CLIENT_SECRET` | and its secret |
| `PIPELINE_TRINO_URL` | the engine's base URL, e.g. `https://trino:8443` |

A missing one prints its name to stderr and exits 2. The other two are optional or derived:

- `PIPELINE_TRINO_CA_FILE` — mounted CA bundle; it becomes `DBT_TRINO_CA`. Without it,
  `profiles.yml` keeps its default `/etc/stardelt/trino-ca/ca.crt`.
- `DBT_TRINO_JWT` and `DBT_TRINO_HOST` are set by the wrapper: a token is minted per
  invocation (client-credentials grant, the same contract as
  `dags/stardelt_pipelines/trino.py`), because a token lives five minutes; the host is
  `PIPELINE_TRINO_URL` without scheme and without port (the port stays in `profiles.yml`).

Then it `exec`s `dbt` with the caller's arguments, so a task runs e.g.

```console
$ docker run --rm -v "$PWD/examples/shop:/work" -w /work ghcr.io/stardelt/dbt:demo \
    run --profiles-dir . --project-dir .
```

## Licence gate

`check-licenses.py` reads `pip-licenses --format=json` and fails the build on any package
whose licence is not OSI-permissive (Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC,
PSF-2.0, Python-2.0, Unlicense, CC0-1.0 after normalising the classifier spellings
`pip-licenses` prints; `;` and `AND` are conjunctions, `OR` alternatives). The image ships
`/opt/dbt/licenses.json`, `/opt/dbt/LICENSES.txt` (the human-readable dump) and the file the
gate reads.

`license-exceptions.txt` is the only exception list — one package per line plus the licence
string **exactly as `pip-licenses` prints it**, so a version bump that changes a licence fails
the build instead of passing silently. Four lines, all unmodified dependencies of a tool image:

| package | licence | why |
|---|---|---|
| `pathspec` | MPL-2.0 | approved 2026-11-18 |
| `text-unidecode` | Artistic / GPL / GPLv2+ | approved 2026-11-18 |
| `certifi` | MPL-2.0 | the Mozilla CA bundle, through `dbt-trino` → `requests` |
| `orjson` | `MPL-2.0 AND (Apache-2.0 OR MIT)` | dual-licensed wheel; the MPL arm is the same licence |

A fifth non-permissive package fails the build; so does a removed exception, which is how the
gate is tested.

## Build and check

```console
$ cd demos && docker build -t stardelt-dbt:test -f images/dbt/Dockerfile .
$ docker run --rm --entrypoint /opt/dbt/bin/dbt stardelt-dbt:test --version
$ docker run --rm --entrypoint python stardelt-dbt:test \
    /opt/dbt/check-licenses.py /opt/dbt/licenses.json /opt/dbt/license-exceptions.txt
$ hadolint images/dbt/Dockerfile
```

`--entrypoint` is needed for every direct `dbt` probe: the image's own entrypoint is the
wrapper, which exits 2 without the four `PIPELINE_*` variables.
