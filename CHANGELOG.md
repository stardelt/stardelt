# Changelog

One section per release, newest first; the `## v<version>` section is what `gh release create` publishes.

## v0.0.4

### Breaking

- The Spark runtime is `spark-4.1-py3.13`; there is no alias for `spark-4.1-py3.11`, so a `SparkCluster` or `Pipeline` that names the old runtime is refused (`unknown runtime`) until it is edited (#558).
- The notebook image and the Spark workers run Python 3.13 (the notebook base is `quay.io/jupyter/scipy-notebook:2026-11-16`); a notebook that pinned 3.11-only wheels must be updated (#78's decision, delivered in stardelt/stardelt-platform#1776).

### Highlights

- The operator installs the platform: `PlatformInstance` + `Lakehouse`, and `operator/parity/steps.tsv` records every platform install step it covers (stardelt/stardelt-operator#6, stardelt/stardelt-operator#547).
- Nova: Pipelines (SQL, notebook, dbt), Runs with logs, Reports (`/reports/<namespace>/<pipeline>`), Notebooks with `viewers`, Lineage, Connections, Destinations, Compute (`SparkCluster`), Streams and Ask (stardelt/stardelt-nova#188, stardelt/stardelt-nova#240, stardelt/stardelt-nova#469, stardelt/stardelt-nova#772, stardelt/stardelt-nova#1007).
- Grants are enforced in the catalog: a `Catalog` declares `grants`, `access-sync` writes them to Lakekeeper, and Trino is the trusted engine, so `SECURITY DEFINER` views work (#486/#487).
- Storage credentials are vended: new catalogs carry `sts-enabled`, so a client gets a table-scoped, read-only credential instead of the platform key (#484, #490).
- Workloads can sign in with a Kubernetes ServiceAccount token (authentication only; grants decide) (#562/#538).
- dbt runs as a pipeline task (`kind: dbt`), with its own image (#10/#11/#112) and the demo's shop project.
- The public demo (`demo`/`demo`, group `visitors`) shows pipelines, a stream, a notebook report, a Connection, a destination and a sandboxed Spark (stardelt/stardelt-platform#904, #477).
- The CLI, the `stardelt` Python package and the MCP server ship as wheels next to `install.yaml`, `platform-minimal.yaml` and `versions.yaml` (#448).

### Upgrade

- This is the first release: there is nothing to upgrade from, and no data migration.
- From the next release on, a release upgrades the one before it (N−1); the `0.0.x` line makes no API-stability promise (see "Upgrade and versions" in the documentation).
