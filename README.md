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

- **Yours.** It runs in your cluster, on your storage, with your sign-in. Your data never leaves.
- **Open.** Every component carries a permissive licence: Apache Iceberg, Trino, Lakekeeper, Airflow, JupyterHub, Superset.
- **One platform.** A catalog with table permissions by group, a SQL editor, notebooks, pipelines, lineage and a query log, in one interface with one API.

## Try it

- **In the browser:** [the public demo](https://stardelt.io/demo/), sign in with `demo` / `demo`.
- **On your cluster:** the [quickstart](https://stardelt.io/docs/getting-started/quickstart) installs the core, loads a file and queries it.

## Follow the project

- ⭐ **Star this repository** to help other data teams find stardelt.
- 👀 **Watch → Custom → Releases** to hear about new versions.
- 🗺 The [roadmap](https://stardelt.io/docs/roadmap) says what works today and what comes next.

## Where things live

| Repository | What it is |
|---|---|
| [stardelt](https://github.com/stardelt/stardelt) | This page: releases, the install manifest, questions and reports |
| [stardelt-nova](https://github.com/stardelt/stardelt-nova) | The interface and API |
| [stardelt-operator](https://github.com/stardelt/stardelt-operator) | The Kubernetes operator and its resources |
| [stardelt-platform](https://github.com/stardelt/stardelt-platform) | Helm values, manifests and the developer install |
| [stardelt-python](https://github.com/stardelt/stardelt-python) | The Python package, command line and MCP server |
| [stardelt-demos](https://github.com/stardelt/stardelt-demos) | Sample pipelines and the Airflow image |
| [stardelt-docs](https://github.com/stardelt/stardelt-docs) | The documentation at stardelt.io |

Not sure where something belongs? [Open an issue here](https://github.com/stardelt/stardelt/issues/new/choose).

## Status

stardelt runs end to end, but there is no tagged release yet: resources and APIs can still change.

## Licence

Apache 2.0. See the [licence policy](https://stardelt.io/docs/architecture/licenses) for every component.
