<p align="center">
  <img src="https://raw.githubusercontent.com/stardelt/.github/main/profile/logo-wordmark.svg" alt="stardelt" height="64" />
</p>

<h3 align="center">The open-source data platform you actually own.</h3>

<p align="center">
SQL over Iceberg tables, notebooks, scheduled pipelines and dashboards behind one sign-in, on any Kubernetes cluster.<br/>
Permissive licences only. No phone-home. No licence server.
</p>

<p align="center">
  <a href="https://stardelt.io/demo/"><b>Try the demo</b></a> ·
  <a href="https://stardelt.io/docs/getting-started/quickstart">Quickstart</a> ·
  <a href="https://stardelt.io">Documentation</a> ·
  <a href="https://stardelt.io/docs/roadmap">Roadmap</a>
</p>

![Nova, the stardelt interface: the catalog of a team's tables](https://stardelt.io/img/nova/catalog.png)

## Why stardelt

- **Yours.** It runs in your cluster, on your storage, with your sign-in. Your data goes nowhere you did not configure.
- **Open.** Every component carries a permissive licence: Apache Iceberg, Trino, Lakekeeper, Airflow, JupyterHub, Superset.
- **One platform.** A catalog with table permissions by group, a SQL editor, notebooks, pipelines, lineage and a query log, in one interface with one API.

## Try it

- **In the browser:** [the public demo](https://stardelt.io/demo/), sign in with `demo` / `demo`.
- **From the latest release:** `kubectl apply --server-side --force-conflicts -f https://github.com/stardelt/stardelt/releases/latest/download/install.yaml` installs the operator; then apply the release's `platform-minimal.yaml` with your domain in it. The [release notes](https://github.com/stardelt/stardelt/releases/latest) show both steps and the matching command line.
- **The core only, from a checkout:** the [quickstart](https://stardelt.io/docs/getting-started/quickstart) installs storage, the catalog, SQL and Nova, loads a file and queries it.

## Follow the project

- ⭐ **Star this repository** to help other data teams find stardelt.
- 👀 **Watch → Custom → Releases** to hear about new versions.
- 🗺 The [roadmap](https://stardelt.io/docs/roadmap) says what works today and what comes next.

[![stardelt stars over time](https://api.star-history.com/svg?repos=stardelt/stardelt&type=Date)](https://star-history.com/#stardelt/stardelt&Date)

## Where things live

The product is one repository. Each part has its own folder, its own build and its own CI workflows (`.github/workflows/<folder>-*.yml`).

| Folder | What it is |
|---|---|
| [`nova/`](nova) | The interface and API |
| [`operator/`](operator) | The Kubernetes operator and its resources |
| [`platform/`](platform) | Helm values, manifests and the developer install |
| [`python/`](python) | The Python package, command line and MCP server |
| [`demos/`](demos) | Sample pipelines and the Airflow image |

The documentation at stardelt.io lives in [stardelt-docs](https://github.com/stardelt/stardelt-docs). The old per-part repositories are archived. Their history is part of this repository.

Not sure where something belongs? [Open an issue here](https://github.com/stardelt/stardelt/issues/new/choose).

## Status

stardelt is alpha (`0.0.x`). The [latest release](https://github.com/stardelt/stardelt/releases/latest) is proven on a fresh single-node cluster before it is published. Resources and APIs can still change before `0.1.0`.

## Licence

Apache 2.0. See the [licence policy](https://stardelt.io/docs/architecture/licenses) for every component.
