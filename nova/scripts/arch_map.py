#!/usr/bin/env python3
"""Nova architecture map, rebuilt from the code (Python 3 standard library only).

Lives in the repository as `nova/scripts/arch_map.py`.

Reads `backend/src/*.rs`, `frontend/src/{App.tsx,nav.ts,api.ts,routes/*.tsx}` and
`openapi/nova.json` under a Nova checkout and writes:

  components.mmd     Mermaid flowchart: frontend nav groups -> backend areas (modules) -> external systems
  modules.md         one row per backend module: production lines, test lines, routes, external systems
  routes.tsv         every served (method, path, handler module, router, gate)
  findings.txt       measured facts: file-size budget, OpenAPI method drift, duplicated helpers
  nova-architecture.html  one self-contained page: both diagrams (Mermaid from a CDN), the module
                     table and the findings; `request-flow.mmd` (hand-written) is read from --out

Usage:
  python3 scripts/arch_map.py [--nova <path to nova/>] [--out <dir>] [--doc <file.md>]   write the files
  python3 scripts/arch_map.py [--nova <path to nova/>] [--out <dir>] [--doc <file.md>] --check
                     exit 1 when a written file is stale or a module has no area in AREAS

--doc: a Markdown file whose block between `<!-- BEGIN arch_map -->` and `<!-- END arch_map -->`
is replaced by the component diagram (a ```mermaid block, which GitHub renders).

Nothing is executed or compiled: every fact is a text scan, so it runs in a second anywhere.
The scan rules are deliberately simple and listed next to each function; when the code grows a
new pattern, change the rule here rather than editing the generated files by hand.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------------------------
# The one hand-kept table: which backend module belongs to which area. A module that is not
# listed makes `--check` fail, so a new module cannot slip out of the picture.
# ---------------------------------------------------------------------------------------------
AREAS: dict[str, list[str]] = {
    "Core & identity": [
        "main", "auth", "session_store", "seal", "api_json", "error", "rfc3339", "openapi",
        "metrics", "audit",
        "db", "people", "user_settings", "shared_accounts", "demo", "embed", "token_exchange",
        "catalog_token",
    ],
    "Catalog": [
        "catalog", "catalog_docs", "catalog_doc_store", "catalog_metrics", "catalog_search",
        "access_requests", "verified_queries", "admin_catalogs",
    ],
    "SQL & query log": [
        "trino", "statements", "saved_queries", "jobs", "warehouses", "query_log",
        "query_log_api", "query_log_collector", "query_log_event", "query_log_store",
        "query_hints", "data",
    ],
    "Pipelines & delivery": [
        "pipelines", "airflow", "pipeline_notice", "streams", "deployments", "stages",
        "migrations", "destinations", "superset", "superset_archive",
    ],
    "Notebooks & compute": ["notebooks", "workspace", "compute"],
    "Connections": ["connections", "connection_routes", "secret_store"],
    "Lineage": ["lineage_api", "lineage_event", "lineage_store"],
    "AI (Ask, MCP, models)": [
        "ask", "mcp", "mcp_audit", "model_chat", "model_calls", "model_endpoints",
        "model_probe", "model_usage",
    ],
    "Platform view": ["apps", "cluster"],
}

# External systems and the code signal that means "this module talks to it directly".
# Signals are searched in production code with comments and the test module removed.
EXTERNAL: dict[str, tuple[str, str]] = {
    "k8s": ("Kubernetes API (CRDs)", r"\bkube::|\bApi::(?:namespaced|all|default_namespaced)|\.kube\b|crate::cluster::"),
    "trino": ("Trino", r"\btrino_url\b|crate::trino::"),
    "lakekeeper": ("Lakekeeper (Iceberg REST)", r"\blakekeeper_url\b"),
    "airflow": ("Airflow REST", r"\bAirflowClient\b|crate::airflow::|\.airflow\b"),
    "pg": ("PostgreSQL state store", r"\bsqlx::|\bPgPool\b"),
    "s3": ("S3 (SeaweedFS)", r"\bNotebookStore\b|\bs3_url\b|x-amz-"),
    "superset": ("Superset", r"\bsuperset_api_url\b|\bsuperset_url\b|crate::superset::"),
    "hub": ("JupyterHub", r"\bjupyterhub_url\b|\bWorkspaceConfig\b"),
    "vault": ("Secret store (OpenBao)", r"\bStoreClient\b|crate::secret_store::"),
    "idp": ("OIDC provider", r"\bjwks\b|\bdiscovery\b|\btoken_endpoint\b"),
    "llm": ("Model endpoints (external)", r"crate::model_chat::|chat/completions"),
}

ROUTE_FUNCS = {
    # file stem -> (function marker, mount prefix, router label)
    "main": [("fn public_router()", "/api", "public"),
             ("fn api_router()", "/api", "signed-in"),
             ("fn admin_router()", "/api", "admin")],
    "pipelines": [("fn router()", "/api", "signed-in")],
    "notebooks": [("fn router()", "/api", "signed-in")],
    "statements": [("fn router()", "/api", "signed-in")],
    "stages": [("fn router()", "/api", "signed-in")],
    "deployments": [("fn router()", "/api", "signed-in")],
    "streams": [("fn router()", "/api", "signed-in")],
    "migrations": [("fn router()", "/api", "signed-in")],
    "auth": [("pub fn router<", "/api/auth", "sign-in")],
    "mcp": [("pub fn router()", "", "mcp (bearer)")],
    "query_log_collector": [("pub fn router(", "", "internal listener")],
}

METHODS = ("get", "post", "put", "delete", "patch")
SCRIPT = "scripts/arch_map.py"  # the name the generated files credit
SIZE_BUDGET = 1500  # production lines per backend file (GUIDELINES.md rule B1)


# ---------------------------------------------------------------------------------------------
# Source helpers
# ---------------------------------------------------------------------------------------------
def split_tests(text: str) -> tuple[str, str]:
    """Production part and test part of a Rust file: the test part starts at the first
    `#[cfg(test)]` that is directly followed by `mod <name> {`."""
    m = re.search(r"^#\[cfg\(test\)\]\s*\n\s*mod \w+ \{", text, re.M)
    if not m:
        return text, ""
    return text[: m.start()], text[m.start():]


def strip_comments(code: str) -> str:
    """Drop `//` comments (doc comments included). A `//` inside a string literal is cut too,
    which only shortens URLs in strings; the signals above never depend on them."""
    return re.sub(r"//[^\n]*", "", code)


def balanced(text: str, start: int) -> str:
    """The text from `start` (just after an opening parenthesis) to its matching `)`,
    skipping string literals."""
    depth, i, out = 1, start, []
    in_str = False
    while i < len(text) and depth:
        c = text[i]
        if in_str:
            if c == "\\":
                out.append(text[i:i + 2]); i += 2; continue
            if c == '"':
                in_str = False
        elif c == '"':
            in_str = True
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                break
        out.append(c)
        i += 1
    return "".join(out)


def function_body(src: str, marker: str) -> str:
    start = src.find(marker)
    if start < 0:
        return ""
    end = src.find("\n}\n", start)
    return src[start:end if end > 0 else len(src)]


def routes_of(src: str, own_module: str, marker: str, prefix: str, gate: str):
    """Every `.route("<path>", <method>(<handler>)...)` in one router function. A handler
    written `module::name` belongs to that module, a bare name to the file's own module."""
    body = strip_comments(function_body(src, marker))
    # `use axum::routing::{patch as patch_route}`: an alias still names its method.
    alias = {b: a for a, b in re.findall(r"\b(" + "|".join(METHODS) + r") as (\w+)", src)}
    words = "|".join(list(METHODS) + list(alias))
    for m in re.finditer(r"\.route\(\s*\"([^\"]+)\"\s*,", body):
        args = balanced(body, m.end())
        for mm in re.finditer(r"(?<![\w:])(" + words + r")\(\s*([A-Za-z_][\w:]*)", args):
            handler = mm.group(2)
            module = handler.split("::")[-2] if "::" in handler else own_module
            if module in ("crate", "super"):
                module = own_module
            yield (alias.get(mm.group(1), mm.group(1)).upper(), prefix + m.group(1), module, own_module, gate)


# ---------------------------------------------------------------------------------------------
# Scans
# ---------------------------------------------------------------------------------------------
def scan_backend(src_dir: Path):
    modules = {}
    for path in sorted(src_dir.glob("*.rs")):
        text = path.read_text(encoding="utf-8")
        prod, tests = split_tests(text)
        code = strip_comments(prod)
        # openapi.rs only lists other modules' handlers and main.rs only builds the clients
        # (wiring); neither is where a system is used, so they draw no edge.
        ext = [] if path.stem in ("openapi", "main") else sorted(
            k for k, (_, rx) in EXTERNAL.items() if re.search(rx, code))
        modules[path.stem] = {
            "prod": prod.count("\n"),
            "tests": tests.count("\n"),
            "ext": ext,
            "code": code,
        }
    # A test module moved to `<module>/tests.rs` still counts as that module's tests (B1).
    for path in sorted(src_dir.glob("*/tests.rs")):
        if path.parent.name in modules:
            modules[path.parent.name]["tests"] += path.read_text(encoding="utf-8").count("\n")
    # Declared modules: what main.rs compiles. A file that is not declared is dead.
    main = (src_dir / "main.rs").read_text(encoding="utf-8")
    declared = set(re.findall(r"^\s*(?:pub(?:\(crate\))?\s+)?mod (\w+);", main, re.M))
    routes = []
    for stem, specs in ROUTE_FUNCS.items():
        f = src_dir / f"{stem}.rs"
        if not f.exists():
            continue
        text = f.read_text(encoding="utf-8")
        for marker, prefix, gate in specs:
            routes.extend(routes_of(text, stem, marker, prefix, gate))
    return modules, declared, sorted(set(routes))


def scan_frontend(fe: Path):
    src = fe / "src"
    app = (src / "App.tsx").read_text(encoding="utf-8")
    imports = dict(re.findall(r"import (\w+) from '\./routes/(\w+)'", app))
    page_of_path = {}
    for path, elem in re.findall(r'<Route path="([^"]+)"\s+element=\{(.+?)\}\s*/>', app):
        names = [n for n in re.findall(r"<(\w+)", elem) if n in imports]
        if names:
            page_of_path[path] = imports[names[-1]]
    nav = (src / "nav.ts").read_text(encoding="utf-8")
    groups: dict[str, list[str]] = {}
    for block in re.split(r"\n  \{\n", nav)[1:]:
        label = re.search(r"label: '([^']+)'", block)
        if not label:
            continue
        pages = [page_of_path.get(to) for to in re.findall(r"to: '([^']+)'", block)]
        groups[label.group(1)] = [p for p in pages if p]
    in_nav = {p for ps in groups.values() for p in ps}
    rest = sorted(set(page_of_path.values()) - in_nav)
    if rest:
        groups["Detail pages"] = rest

    # api.ts: `name: (...) => jget<...>('/api/...')` → name → path template
    api_ts = (src / "api.ts").read_text(encoding="utf-8")
    block = api_ts[api_ts.find("export const api = {"):]
    api_paths = {}
    for m in re.finditer(r"^  (\w+):", block, re.M):
        nxt = re.search(r"^  \w+:", block[m.end():], re.M)
        chunk = block[m.end(): m.end() + (nxt.start() if nxt else 400)]
        lit = re.search(r"[`'](/api/[^`'?]*)", chunk)
        if lit:
            api_paths[m.group(1)] = re.sub(r"\$\{[^}]*\}", "{}", lit.group(1))

    # Which api functions each page uses: the page file plus the components it imports.
    page_calls = {}
    comp_dir = src / "components"
    for page in set(page_of_path.values()):
        text = (src / "routes" / f"{page}.tsx").read_text(encoding="utf-8")
        for comp in re.findall(r"from '\.\./components/(\w+)'", text):
            cf = comp_dir / f"{comp}.tsx"
            if cf.exists():
                text += cf.read_text(encoding="utf-8")
        page_calls[page] = sorted(set(re.findall(r"\bapi\.(\w+)\b", text)) & set(api_paths))
    sizes = {p.relative_to(src).as_posix(): p.read_text(encoding="utf-8").count("\n")
             for p in src.rglob("*.ts*")}
    return groups, api_paths, page_calls, sizes


def path_regex(template: str) -> re.Pattern:
    parts = re.split(r"\{[^}]+\}", template)
    return re.compile("^" + "[^/]+".join(re.escape(p) for p in parts) + "$")


def area_of(module: str) -> str | None:
    for area, mods in AREAS.items():
        if module in mods:
            return area
    return None


def ident(text: str) -> str:
    return re.sub(r"\W+", "_", text).strip("_").lower()


# ---------------------------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------------------------
def components_mmd(modules, routes, groups, api_paths, page_calls) -> str:
    out = [f"%% Generated by {SCRIPT} from the Nova sources; do not edit by hand.",
           "flowchart LR"]
    out.append('  browser(["Browser: Nova UI (React SPA)"])')
    out.append('  agents(["Agents and CLI: MCP, REST, SDK"])')
    # Frontend nav groups
    out.append('  subgraph FE["frontend/src (pages by nav group)"]')
    out.append("    direction TB")
    for g, pages in groups.items():
        out.append(f'    fe_{ident(g)}["{g}<br/><small>{", ".join(pages)}</small>"]')
    out.append("  end")
    # Backend areas with their modules (production lines)
    out.append('  subgraph BE["backend/src (axum): areas and modules, production lines"]')
    out.append("    direction TB")
    for area, mods in AREAS.items():
        present = [m for m in mods if m in modules]
        label = "<br/>".join(
            f"{m} {modules[m]['prod']}" + (" ⚠" if modules[m]["prod"] > SIZE_BUDGET else "")
            for m in sorted(present, key=lambda m: -modules[m]["prod"]))
        out.append(f'    be_{ident(area)}["<b>{area}</b><br/><small>{label}</small>"]')
    out.append("  end")
    # External systems
    out.append('  subgraph EXT["external systems"]')
    out.append("    direction TB")
    for key, (name, _) in EXTERNAL.items():
        out.append(f'    x_{key}[("{name}")]')
    out.append("  end")
    out.append("  browser --> FE")
    out.append('  agents -->|"/mcp, /api"| be_ai_ask_mcp_models')
    # page group -> backend area, from api.ts usage
    compiled = [(path_regex(p), p, m) for (_, p, m, _, _) in routes]
    edges = defaultdict(int)
    for g, pages in groups.items():
        for page in pages:
            for fn in page_calls.get(page, []):
                template = api_paths[fn]
                for rx, _, module in compiled:
                    if rx.match(template.rstrip("/")) or rx.match(template):
                        a = area_of(module)
                        if a:
                            edges[(g, a)] += 1
                        break
    for (g, a), n in sorted(edges.items()):
        out.append(f'  fe_{ident(g)} -->|{n}| be_{ident(a)}')
    # area -> external system
    ext_edges = defaultdict(set)
    for m, info in modules.items():
        a = area_of(m)
        if a:
            for x in info["ext"]:
                ext_edges[(a, x)].add(m)
    for (a, x), mods in sorted(ext_edges.items()):
        out.append(f'  be_{ident(a)} -.->|{len(mods)}| x_{x}')
    return "\n".join(out) + "\n"


def modules_md(modules, routes) -> str:
    per_module = defaultdict(list)
    for method, path, module, _, gate in routes:
        per_module[module].append(f"{method} {path}")
    lines = [f"<!-- Generated by {SCRIPT}; do not edit by hand. -->",
             "| module | area | production lines | test lines | routes | talks to |",
             "|---|---|---:|---:|---:|---|"]
    for m, info in sorted(modules.items(), key=lambda kv: -kv[1]["prod"]):
        flag = " ⚠" if info["prod"] > SIZE_BUDGET else ""
        ext = ", ".join(EXTERNAL[x][0] for x in info["ext"]) or "-"
        lines.append(f"| `{m}.rs`{flag} | {area_of(m) or '**unmapped**'} | {info['prod']} | "
                     f"{info['tests']} | {len(per_module.get(m, []))} | {ext} |")
    total_p = sum(i["prod"] for i in modules.values())
    total_t = sum(i["tests"] for i in modules.values())
    lines.append(f"| **total ({len(modules)} files)** | | **{total_p}** | **{total_t}** | **{len(routes)}** | |")
    return "\n".join(lines) + "\n"


def findings_txt(modules, declared, routes, openapi, fe_sizes) -> str:
    out = []
    over = sorted(((i["prod"], m) for m, i in modules.items() if i["prod"] > SIZE_BUDGET), reverse=True)
    out.append(f"B1 file-size budget ({SIZE_BUDGET} production lines): {len(over)} backend files over")
    out += [f"  {m}.rs {n}" for n, m in over]
    fe_over = sorted(((n, f) for f, n in fe_sizes.items() if n > 800), reverse=True)
    out.append(f"F-size frontend files over 800 lines: {len(fe_over)}")
    out += [f"  {f} {n}" for n, f in fe_over]
    undeclared = sorted(set(modules) - declared - {"main"})
    out.append(f"Undeclared .rs files (dead): {undeclared or 'none'}")
    # OpenAPI drift by (method, path), not only by path.
    documented = {(m.upper(), p) for p, v in openapi.get("paths", {}).items() for m in v if m in METHODS}
    served = {(m, p) for (m, p, _, _, g) in routes
              if p.startswith("/api/") and g != "internal listener"}
    skip = {"/api/auth/login", "/api/auth/callback", "/api/auth/logout"}
    missing = sorted(x for x in served - documented if x[1] not in skip)
    ghost = sorted(x for x in documented - served if x[1] not in skip)
    out.append(f"OpenAPI: served /api operations not in nova.json: {len(missing)}")
    out += [f"  {m} {p}" for m, p in missing]
    out.append(f"OpenAPI: nova.json operations not served by a scanned router: {len(ghost)}")
    out += [f"  {m} {p}" for m, p in ghost]
    # Duplicated helpers (names that should exist once).
    dup_rx = {
        "dev-mode administrator Identity built by hand": r"is_admin:\s*true",
        "group-membership test written inline": r"groups\.iter\(\)\.any\(",
        "hand-written GroupVersionKind": r"GroupVersionKind::gvk\(",
        "inline error JSON json!({\"error\"": r"json!\(\{\s*\"error\"",
        "own fail()/error_response() helper": r"\bfn (?:fail|error_response)\(",
        "module-wide #![allow(dead_code)]": r"#!\[allow\(dead_code\)\]",
        "hand-written calendar (civil_from_days)": r"\bfn civil_from_days\(",
    }
    for name, rx in dup_rx.items():
        hits = {m: len(re.findall(rx, i["code"])) for m, i in modules.items()}
        hits = {m: n for m, n in hits.items() if n}
        out.append(f"{name}: {sum(hits.values())} in {len(hits)} files "
                   + ", ".join(f"{m}({n})" for m, n in sorted(hits.items(), key=lambda kv: -kv[1])))
    return "\n".join(out) + "\n"


def page_html(components: str, flow: str, modules: str, findings: str) -> str:
    """One page that renders the Mermaid sources in the browser (light and dark)."""
    rows = []
    for line in modules.splitlines()[3:]:
        cells = [c.strip() for c in line.strip("|").split("|")]
        rows.append("<tr>" + "".join(
            f"<td>{html.escape(c).replace('`', '')}</td>" for c in cells) + "</tr>")
    flow_block = (f'<h2>One request: Run now on a pipeline</h2>\n<pre class="mermaid">{html.escape(flow)}</pre>'
                  if flow else "")
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Nova architecture map</title>
<style>
:root {{ --bg:#ffffff; --fg:#1d2330; --mute:#5b6475; --line:#d9dee7; --card:#f6f8fb; --warn:#b3261e; }}
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{ --bg:#12151b; --fg:#e6e9ef; --mute:#9aa3b2; --line:#2a303b; --card:#1a1f27; --warn:#ff8a80; }}
}}
:root[data-theme="dark"] {{ --bg:#12151b; --fg:#e6e9ef; --mute:#9aa3b2; --line:#2a303b; --card:#1a1f27; --warn:#ff8a80; }}
body {{ background:var(--bg); color:var(--fg); font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif; margin:0; padding:24px 16px; }}
main {{ max-width:1200px; margin:0 auto; }}
h1 {{ font-size:1.5rem; margin:0 0 4px; }} h2 {{ font-size:1.15rem; margin:32px 0 8px; }}
p.lead {{ color:var(--mute); margin:0 0 16px; }}
.diagram, pre.mermaid {{ background:var(--card); border:1px solid var(--line); border-radius:8px; padding:12px; overflow-x:auto; }}
table {{ border-collapse:collapse; width:100%; font-size:13px; display:block; overflow-x:auto; }}
th, td {{ border-bottom:1px solid var(--line); padding:4px 8px; text-align:left; white-space:nowrap; }}
td:nth-child(3), td:nth-child(4), td:nth-child(5) {{ text-align:right; font-variant-numeric:tabular-nums; }}
pre.findings {{ background:var(--card); border:1px solid var(--line); border-radius:8px; padding:12px; overflow-x:auto; font-size:13px; }}
code {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }}
</style>
</head>
<body>
<main>
<h1>Nova architecture map</h1>
<p class="lead">Generated by <code>{SCRIPT}</code> from the Nova sources: frontend pages (by navigation group) call
backend areas (numbers on arrows: API functions used), backend areas call external systems (numbers: modules
that do). Module sizes are production lines; ⚠ is over the 1500-line budget.</p>
<h2>Components</h2>
<pre class="mermaid">{html.escape(components)}</pre>
{flow_block}
<h2>Modules</h2>
<table><thead><tr><th>module</th><th>area</th><th>production</th><th>tests</th><th>routes</th><th>talks to</th></tr></thead>
<tbody>{"".join(rows)}</tbody></table>
<h2>Measured findings</h2>
<pre class="findings">{html.escape(findings)}</pre>
</main>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.min.js"></script>
<script>
  const dark = document.documentElement.dataset.theme === 'dark' ||
    (document.documentElement.dataset.theme !== 'light' && window.matchMedia('(prefers-color-scheme: dark)').matches);
  mermaid.initialize({{ startOnLoad: true, theme: dark ? 'dark' : 'default', securityLevel: 'strict',
    flowchart: {{ htmlLabels: true, useMaxWidth: true }}, sequence: {{ useMaxWidth: true }} }});
</script>
</body>
</html>
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    here = Path(__file__).resolve().parent
    ap.add_argument("--nova", type=Path, default=here.parent,
                    help="path to the nova/ folder (default: the folder above this script)")
    ap.add_argument("--out", type=Path, default=None,
                    help="where the generated files go (default: <nova>/docs/architecture)")
    ap.add_argument("--doc", type=Path, help="Markdown file with the arch_map markers")
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    if a.out is None:
        a.out = a.nova / "docs" / "architecture"

    src = a.nova / "backend" / "src"
    modules, declared, routes = scan_backend(src)
    groups, api_paths, page_calls, fe_sizes = scan_frontend(a.nova / "frontend")
    openapi = json.loads((a.nova / "openapi" / "nova.json").read_text(encoding="utf-8"))

    unmapped = sorted(m for m in modules if not area_of(m))
    flow_file = a.out / "request-flow.mmd"
    flow = flow_file.read_text(encoding="utf-8") if flow_file.exists() else ""
    files = {
        "components.mmd": components_mmd(modules, routes, groups, api_paths, page_calls),
        "modules.md": modules_md(modules, routes),
        "routes.tsv": "method\tpath\thandler_module\trouter_file\tgate\n"
                      + "".join("\t".join(r) + "\n" for r in routes),
        "findings.txt": findings_txt(modules, declared, routes, openapi, fe_sizes),
    }
    files["nova-architecture.html"] = page_html(
        files["components.mmd"], flow, files["modules.md"], files["findings.txt"])
    targets = {a.out / n: body for n, body in files.items()}
    if a.doc:
        doc = a.doc.read_text(encoding="utf-8")
        begin, end = "<!-- BEGIN arch_map -->", "<!-- END arch_map -->"
        if begin not in doc or end not in doc:
            print(f"{a.doc} has no {begin} ... {end} block")
            return 1
        head, rest = doc.split(begin, 1)
        _, tail = rest.split(end, 1)
        block = "\n".join(l for l in files["components.mmd"].splitlines() if not l.startswith("%%"))
        targets[a.doc] = (f"{head}{begin}\n<!-- generated by {SCRIPT}; do not edit -->\n"
                          f"```mermaid\n{block}\n```\n{end}{tail}")
    if a.check:
        stale = [str(p) for p, body in targets.items()
                 if not p.exists() or p.read_text(encoding="utf-8") != body]
        if unmapped:
            print(f"modules without an area in AREAS: {', '.join(unmapped)}")
        if stale:
            print(f"stale: {', '.join(stale)} (run it again without --check)")
        if unmapped or stale:
            return 1
        print("architecture map is fresh")
        return 0
    a.out.mkdir(parents=True, exist_ok=True)
    for path, body in targets.items():
        path.write_text(body, encoding="utf-8")
    print(f"wrote {', '.join(p.name for p in targets)}; {len(modules)} modules, {len(routes)} routes")
    if unmapped:
        print(f"WARNING modules without an area: {', '.join(unmapped)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
