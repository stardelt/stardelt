# AGENTS.md — stardelt (the product repository)

Since 2026-11-18 (decision D49) the product is **one repository**. The former `stardelt-operator`,
`-platform`, `-nova`, `-python` and `-demos` repositories are folders here. They are archived and
read-only, with their history merged in. Each folder keeps its own build and its own `AGENTS.md`:
read the one for the folder you change.

```
stardelt/                       ← this repository (checkout: <workspace>/stardelt)
├── operator/                   kube-rs CRD operator (Rust; Cargo workspace root here)
├── platform/                   Helm values, manifests, scripts, dev/hetzner-k3s (make, bash, python)
├── nova/                       Rust axum backend (Cargo workspace root) + React UI in nova/frontend
├── python/                     the `stardelt` Python package, CLI, MCP server; python/jupyterlab extension
├── demos/                      sample pipelines and the Airflow loader image
├── .github/workflows/          <folder>-<name>.yml: each workflow runs in its folder and only for changes there
└── .github/dependabot.yml      one config, directories per folder
```

Outside this repository, next to it in the workspace: `stardelt-docs/` (public docs site),
`stardelt-internal/` (private design and build plane), `stardelt-slides/`.

## Working here

- **Run commands from the folder.** `cd operator && cargo check`, `cd nova/frontend && npm run build`,
  `cd platform && make …`. CI does the same: `defaults.run.working-directory: <folder>`.
- **Paths in tickets.** New tickets write paths from this root (`operator/crates/stardelt-operator/src/keycloak.rs`).
  A ticket that starts with "> Moved from stardelt-<name>#N … relative to `<name>/`" was written for the old
  repository: its paths and commands are relative to that folder.
- **Sibling folders replace sibling checkouts.** Use `../platform` and `../operator`, never
  `../stardelt-platform`. The operator's checks read `PLATFORM_MAKEFILE`, `PLATFORM_JOB` and
  `PLATFORM_NOVA_FILE`; the platform's read `OPERATOR_DIR`. They default to the sibling folder.
- **One change, one PR.** A change across folders (a CRD, the operator, the platform values, Nova) is one
  branch and one PR, not a chain of "Land after" tickets.
- **Worktrees.** `git worktree add .wt-<slug> -b <branch> origin/main` from this root. Never commit on `main`.
- **Issues.** Issues and PRs are `#N` here, labelled `area/<folder>`. Something left behind in an archived
  repository is `stardelt/stardelt-<name>#N`.

## Images and releases

- **Image names did not change:** `ghcr.io/stardelt/stardelt-operator`, `nova`, `superset`,
  `jupyter-singleuser`, `spark`, `stardelt-cli`, `airflow`. Every pinned digest stays valid; pin by digest.
- **Release tags carry the folder:** `operator/v*`, `nova/v*`, `platform/v*`, `python/v*`,
  `python/jupyterlab-v*`. Image tags drop the prefix (`stardelt-cli:v0.4.1`).
- **Until #128 lands**, the notebook library is still installed from the archived `stardelt-python` tags.
  Do not cut a library release before then.
- **Pin rollup.** `platform-pin-rollup.yml` refreshes the pins in one PR (`chore/pin-rollup`) after each
  `platform / Image` run, with auto-merge.

## Licence

Apache 2.0, permissive dependencies only, plus the documented exceptions in
`../stardelt-docs/docs/architecture/licenses.md`. No build-plane tooling (agent prompts, runners) belongs in
this repository.
