"""Turn a `Pipeline` resource (stardelt.io/v1alpha1) into a plan the loader builds a DAG from.

Pure: no Airflow, no network, no clock. The rules mirror `PipelineSpec::validate` in the
operator (unique task names, known dependencies, no cycles, a `sql` task has a statement, a
`notebook` task names its file and its published object version, a `dbt` task names a project of
the tool image), because the API server's schema rules cannot see a dependency cycle.

The same holds for the principal: a pipeline that names one is checked against the
`ServicePrincipal` resources of its namespace (the caller passes them in), so no pipeline can
run as another group's principal. A notebook task runs as that principal, so a pipeline that
carries one must have an `ownerGroup` and a `runAs`.

A kind without a runtime is refused at parse time; the API accepts it so manifests can be
written ahead of its runtime.

A task that names a `connection` is checked against the namespace's `Connection` resources
(scope and owner group); its keys become `CONN_<NAME>_<KEY>` in the task pod.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace

from stardelt_pipelines.quality import SEVERITIES, Check

_DURATION = re.compile(r"^(\d+)([smhd])$")
_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
# `Replace` has no Airflow equivalent; it is treated like `Forbid` (one run at a time).
_ACTIVE_RUNS = {"Allow": 16, "Forbid": 1, "Replace": 1}
DEFAULT_RETRY_DELAY_S = 60
# The platform's shared pipeline principal (phase J0). It belongs to no group, so it is never the
# principal a pipeline with an `ownerGroup` may name.
DEFAULT_PRINCIPAL = "pipeline-runner"
# A `Connection` name: 1 to 40 lower-case letters, digits and dashes, starting with a letter (the
# operator's CRD pattern; the length limit is the API server's).
_CONNECTION_NAME = re.compile(r"^[a-z]([a-z0-9-]*[a-z0-9])?$")
# A connection's secret key becomes part of an environment variable (`CONN_<NAME>_<KEY>`), so it
# must be a legal environment variable name after upper-casing.
_CONNECTION_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# The `notebook` task's config rules, the same the operator's CEL rule and `validate_notebook_task`
# enforce (a published object). The API server cannot see that `params` is JSON, so this module
# repeats all of it.
_NOTEBOOK_FILE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._ -]{0,127}\.ipynb$")
_OBJECT_VERSION = re.compile(r"^\S{1,128}$")
NOTEBOOK_CONFIG_KEYS = ("notebook", "objectVersion", "params")
MAX_NOTEBOOK_PARAMS = 32
NOTEBOOK_NEEDS_PRINCIPAL = (
    "a notebook task runs as the group principal of its pipeline: set ownerGroup and runAs"
)
NOTEBOOK_NEEDS_FILE = (
    "a notebook task needs config.notebook (a .ipynb file name) and config.objectVersion "
    "(the published version)"
)
NOTEBOOK_ONLY_KEYS = (
    "notebook tasks accept only notebook, objectVersion and params (found `{key}`)"
)
NOTEBOOK_BAD_PARAMS = (
    "notebook params must be a JSON object string of string values, at most 32 keys"
)
# The `dbt` task's config: a project of the tool image, one selector or a chain of batches, the
# threads and the dbt `--vars`. The plan mints one task per batch, so `build` with two batches is
# `build-1` and `build-2` in the DAG.
DBT_CONFIG_KEYS = ("project", "select", "batches", "threads", "vars")
DBT_ONLY_KEYS = (
    "dbt tasks accept only project, select, batches, threads and vars (found `{key}`)"
)
DBT_NEEDS_PROJECT = (
    "a dbt task needs config.project (a directory under /opt/dbt/projects in the tool image)"
)
DBT_NEEDS_SELECT = (
    "a dbt task needs exactly one of config.select (one dbt selector) and config.batches "
    "(a JSON array of 1 to 8 selectors, run in order)"
)
DBT_BAD_THREADS = "dbt threads must be a digit string from 1 to 16 (default `4`)"
DBT_BAD_VARS = "dbt vars must be a JSON object string of string values"
DBT_NEEDS_PRINCIPAL = (
    "a dbt task runs as the group principal of its pipeline: set ownerGroup and runAs"
)
MAX_DBT_BATCHES = 8
MAX_DBT_THREADS = 16
# A project is one directory name under the tool image's /opt/dbt/projects.
_DBT_PROJECT = re.compile(r"^[a-z][a-z0-9-]{0,38}$")
# A `quality` task's checks: SQL assertions that pass when they return no rows. The bounds are
# the operator's `QualityCheck` schema; this module repeats them because the API server cannot see
# a duplicate name or a placeholder the runtime has no branch for.
CHECK_KEYS = ("name", "sql", "severity")
MAX_CHECKS = 50
MAX_CHECK_SQL = 20000
_CHECK_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$")
QUALITY_NEEDS_CHECKS = (
    "quality task '{task}' needs checks (a list of 1 to 50 SQL assertions)"
)
QUALITY_CHECK_ONLY_KEYS = (
    "check '{check}' in quality task '{task}' accepts only name, sql and severity "
    "(found `{key}`)"
)
QUALITY_CHECK_NEEDS_NAME = (
    "check {index} in quality task '{task}' needs a name (1 to 63 letters, digits, dashes or "
    "underscores, starting with a letter or a digit)"
)
QUALITY_CHECK_NAME_TWICE = "check name '{check}' is used twice in quality task '{task}'"
QUALITY_CHECK_NEEDS_SQL = (
    "check '{check}' in quality task '{task}': sql must be a non-empty string of at most 20000 "
    "characters"
)
QUALITY_CHECK_BAD_SEVERITY = (
    "check '{check}' in quality task '{task}': severity must be error or warn (found `{severity}`)"
)
QUALITY_CHECK_AUDIT_TABLE = (
    "check '{check}' in quality task '{task}' uses ${{table}}: that placeholder is the audit "
    "branch (publish: wap), which has no runtime yet"
)


class PlanError(ValueError):
    """The resource cannot become a DAG; the message names the reason."""


@dataclass(frozen=True)
class PrincipalRef:
    """The `ServicePrincipal` a pipeline runs as: its name and the Secret holding its credentials."""

    name: str
    secret_name: str


@dataclass(frozen=True)
class TaskPlan:
    name: str
    kind: str
    depends_on: tuple[str, ...]
    sql: str | None
    warehouse: str | None
    # `notebook` tasks only: the published file, its immutable object version, and the task's own
    # parameters (task wins over the pipeline's). Defaults keep every other kind unchanged.
    notebook: str | None = None
    object_version: str | None = None
    notebook_params: tuple[tuple[str, str], ...] = ()
    # The `Connection` a task names and the secret keys its pod gets as `CONN_<NAME>_<KEY>`.
    connection: str | None = None
    connection_keys: tuple[str, ...] = ()
    # `dbt` tasks only: the project under the tool image's /opt/dbt/projects, its selectors, the
    # threads and the `--vars` text. Defaults keep every other kind unchanged.
    dbt_project: str | None = None
    dbt_select: tuple[str, ...] = ()
    dbt_threads: str = "4"
    dbt_vars: str | None = None
    # `quality` tasks only: the SQL assertions the run is gated on. Defaults keep every other kind
    # unchanged.
    checks: tuple[Check, ...] = ()


@dataclass(frozen=True)
class PipelinePlan:
    dag_id: str
    name: str
    cron: str | None
    timezone: str
    retries: int
    retry_delay_s: int
    timeout_s: int | None
    max_active_runs: int
    params: dict[str, str]
    run_as: str | None
    owner_group: str | None
    principal: PrincipalRef | None
    tasks: tuple[TaskPlan, ...]


def parse_duration(text: str | None, default: int | None) -> int | None:
    """`5m` -> 300. `None` gives `default`. Anything else than digits plus s, m, h or d is an error."""
    if text is None:
        return default
    match = _DURATION.match(text)
    if not match:
        raise PlanError(f"`{text}` is not a duration (use for example 30s, 5m, 2h, 1d)")
    return int(match.group(1)) * _SECONDS[match.group(2)]


def dag_id(namespace: str, name: str) -> str:
    return f"{namespace}__{name}"


def plan(
    resource: dict,
    namespace: str,
    principals: dict[str, dict] | None = None,
    connections: dict[str, dict] | None = None,
) -> PipelinePlan:
    """Validate `resource` and return its plan, or raise `PlanError`.

    `principals` are the `ServicePrincipal` resources of the namespace by name; `None` means the
    namespace has none, so a pipeline that names one is refused. `connections` are its
    `Connection` resources by name, the same way.
    """
    name = resource.get("metadata", {}).get("name")
    if not name:
        raise PlanError("the resource has no name")
    spec = resource.get("spec") or {}

    raw_tasks = spec.get("tasks") or []
    if not raw_tasks:
        raise PlanError("the pipeline has no tasks")
    tasks: list[TaskPlan] = []
    seen: set[str] = set()
    # A dbt task's batches become `<name>-1` … `<name>-N`; a later task that depends on the
    # batched name waits for the last batch instead.
    last_batch: dict[str, str] = {}
    for raw in raw_tasks:
        task_name = raw.get("name")
        if not task_name:
            raise PlanError("a task has no name")
        if task_name in seen:
            raise PlanError(f"task name `{task_name}` is used twice")
        seen.add(task_name)
        kind = raw.get("kind")
        if kind not in ("sql", "notebook", "dbt", "quality"):
            raise PlanError(
                f"task `{task_name}` has kind `{kind}`, which has no runtime yet "
                "(only sql, notebook, dbt and quality run today)"
            )
        if kind == "sql" and not raw.get("sql"):
            raise PlanError(f"sql task `{task_name}` needs a sql statement")
        notebook = object_version = None
        notebook_params: tuple[tuple[str, str], ...] = ()
        if kind == "notebook":
            if not (spec.get("ownerGroup") and spec.get("runAs")):
                raise PlanError(NOTEBOOK_NEEDS_PRINCIPAL)
            notebook, object_version, notebook_params = _notebook_config(raw.get("config"))
        dbt_project = None
        dbt_select: tuple[str, ...] = ()
        dbt_threads = "4"
        dbt_vars = None
        if kind == "dbt":
            if not (spec.get("ownerGroup") and spec.get("runAs")):
                raise PlanError(DBT_NEEDS_PRINCIPAL)
            dbt_project, dbt_select, dbt_threads, dbt_vars = _dbt_config(raw.get("config"))
        checks: tuple[Check, ...] = ()
        if kind == "quality":
            checks = _quality_checks(task_name, raw.get("checks"))
        connection = raw.get("connection")
        connection_keys: tuple[str, ...] = ()
        if connection is not None:
            if not isinstance(connection, str) or not _CONNECTION_NAME.match(connection):
                raise PlanError(
                    f"task `{task_name}` names a connection that is not a connection name "
                    "(1 to 40 lower-case letters, digits and dashes, starting with a letter)"
                )
            if kind in ("sql", "quality"):
                raise PlanError(
                    f"{kind} task `{task_name}` cannot name a connection: it runs inside the "
                    "engine. A connection is used by a notebook task."
                )
            found = (connections or {}).get(connection)
            if found is None:
                raise PlanError(
                    f"task `{task_name}` names connection `{connection}`, which does not exist in "
                    f"namespace `{namespace}`"
                )
            connection_keys = _connection_for_pipeline(connection, found, spec.get("ownerGroup"))
        depends_on = tuple(raw.get("dependsOn") or ())
        if kind == "dbt" and len(dbt_select) > 1:
            # One task per batch, chained in order. The first batch carries what the task
            # declared; every later batch waits for the one before it.
            names = [f"{task_name}-{index + 1}" for index in range(len(dbt_select))]
            for index, batch_name in enumerate(names):
                tasks.append(
                    TaskPlan(
                        name=batch_name,
                        kind=kind,
                        depends_on=((names[index - 1],) if index else depends_on),
                        sql=None,
                        warehouse=raw.get("warehouse"),
                        notebook=None,
                        object_version=None,
                        notebook_params=(),
                        connection=connection,
                        connection_keys=connection_keys,
                        dbt_project=dbt_project,
                        dbt_select=(dbt_select[index],),
                        dbt_threads=dbt_threads,
                        dbt_vars=dbt_vars,
                        checks=(),
                    )
                )
            last_batch[task_name] = names[-1]
            continue
        tasks.append(
            TaskPlan(
                name=task_name,
                kind=kind,
                depends_on=depends_on,
                sql=raw.get("sql"),
                warehouse=raw.get("warehouse"),
                notebook=notebook,
                object_version=object_version,
                notebook_params=notebook_params,
                connection=connection,
                connection_keys=connection_keys,
                dbt_project=dbt_project,
                dbt_select=dbt_select,
                dbt_threads=dbt_threads,
                dbt_vars=dbt_vars,
                checks=checks,
            )
        )
    for index, task in enumerate(tasks):
        rewritten = tuple(last_batch.get(dep, dep) for dep in task.depends_on)
        if rewritten != task.depends_on:
            tasks[index] = replace(task, depends_on=rewritten)
    names = [task.name for task in tasks]
    duplicate = next((name for name in names if names.count(name) > 1), None)
    if duplicate is not None:
        raise PlanError(f"task name `{duplicate}` is used twice")
    for task in tasks:
        for dep in task.depends_on:
            if dep == task.name or dep not in names:
                raise PlanError(
                    f"task `{task.name}` depends on `{dep}`, which is not another task of this pipeline"
                )
    _reject_cycles(tasks)

    schedule = spec.get("schedule") or {}
    retries = spec.get("retries") or {}
    return PipelinePlan(
        dag_id=dag_id(namespace, name),
        name=name,
        cron=schedule.get("cron"),
        timezone=schedule.get("timezone") or "UTC",
        retries=int(retries.get("attempts", 0)),
        retry_delay_s=parse_duration(retries.get("backoff"), DEFAULT_RETRY_DELAY_S),
        timeout_s=parse_duration(spec.get("timeout"), None),
        max_active_runs=_ACTIVE_RUNS[spec.get("concurrency") or "Forbid"],
        params=dict(spec.get("params") or {}),
        run_as=spec.get("runAs"),
        owner_group=spec.get("ownerGroup"),
        principal=_principal(spec, principals or {}),
        tasks=tuple(tasks),
    )


def _notebook_config(config: dict | None) -> tuple[str, str, tuple[tuple[str, str], ...]]:
    """The file, object version and parameters of a `notebook` task, or a `PlanError`.

    The same three sentences the operator uses: only `notebook`, `objectVersion` and `params`;
    the file is a `.ipynb` name, the version a non-empty token without whitespace, and the params
    a JSON object of string values (at most 32 keys).
    """
    if not isinstance(config, dict):
        raise PlanError(NOTEBOOK_NEEDS_FILE)
    for key in config:
        if key not in NOTEBOOK_CONFIG_KEYS:
            raise PlanError(NOTEBOOK_ONLY_KEYS.format(key=key))
    notebook = config.get("notebook")
    version = config.get("objectVersion")
    if not isinstance(notebook, str) or not _NOTEBOOK_FILE.match(notebook):
        raise PlanError(NOTEBOOK_NEEDS_FILE)
    if not isinstance(version, str) or not _OBJECT_VERSION.match(version):
        raise PlanError(NOTEBOOK_NEEDS_FILE)
    return notebook, version, _notebook_params(config.get("params"))


def _dbt_config(config: dict | None) -> tuple[str, tuple[str, ...], str, str | None]:
    """The project, selectors, threads and `--vars` of a `dbt` task, or a `PlanError`.

    One selector (`select`) or a chain of batches (`batches`, 1 to 8 selectors in a JSON array),
    never both and never neither. The project is one directory name under the tool image's
    `/opt/dbt/projects`, so it can never name a directory outside it.
    """
    if not isinstance(config, dict):
        raise PlanError(DBT_NEEDS_PROJECT)
    for key in config:
        if key not in DBT_CONFIG_KEYS:
            raise PlanError(DBT_ONLY_KEYS.format(key=key))
    project = config.get("project")
    if not isinstance(project, str) or not _DBT_PROJECT.match(project):
        raise PlanError(DBT_NEEDS_PROJECT)
    select = config.get("select")
    batches = config.get("batches")
    if (select is None) == (batches is None):
        raise PlanError(DBT_NEEDS_SELECT)
    if select is not None:
        if not isinstance(select, str) or not select.strip():
            raise PlanError(DBT_NEEDS_SELECT)
        selectors = (select.strip(),)
    else:
        selectors = _dbt_batches(batches)
    threads = config.get("threads", "4")
    if (
        not isinstance(threads, str)
        or not threads.isascii()
        or not threads.isdigit()
        or not 1 <= int(threads) <= MAX_DBT_THREADS
    ):
        raise PlanError(DBT_BAD_THREADS)
    vars_text = config.get("vars")
    if vars_text is not None:
        _dbt_vars(vars_text)
    return project, selectors, threads, vars_text


def _quality_checks(task_name: str, raw) -> tuple[Check, ...]:
    """The SQL assertions of a `quality` task, or a `PlanError`.

    One to 50 mappings, each only `name`, `sql` and `severity` (absent is `error`); a name is
    unique within the task, the statement non-empty, and `${table}` is refused: it is the audit
    branch of WAP, which has no runtime yet.
    """
    if not isinstance(raw, list) or not 1 <= len(raw) <= MAX_CHECKS:
        raise PlanError(QUALITY_NEEDS_CHECKS.format(task=task_name))
    checks: list[Check] = []
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise PlanError(
                QUALITY_CHECK_NEEDS_NAME.format(index=index, task=task_name)
            )
        name = item.get("name")
        if not isinstance(name, str) or not _CHECK_NAME.match(name):
            raise PlanError(
                QUALITY_CHECK_NEEDS_NAME.format(index=index, task=task_name)
            )
        for key in item:
            if key not in CHECK_KEYS:
                raise PlanError(
                    QUALITY_CHECK_ONLY_KEYS.format(check=name, task=task_name, key=key)
                )
        if any(check.name == name for check in checks):
            raise PlanError(QUALITY_CHECK_NAME_TWICE.format(check=name, task=task_name))
        sql = item.get("sql")
        if not isinstance(sql, str) or not sql or len(sql) > MAX_CHECK_SQL:
            raise PlanError(QUALITY_CHECK_NEEDS_SQL.format(check=name, task=task_name))
        if "${table}" in sql:
            raise PlanError(QUALITY_CHECK_AUDIT_TABLE.format(check=name, task=task_name))
        severity = item.get("severity", "error")
        if severity not in SEVERITIES:
            raise PlanError(
                QUALITY_CHECK_BAD_SEVERITY.format(
                    check=name, task=task_name, severity=severity
                )
            )
        checks.append(Check(name=name, sql=sql, severity=severity))
    return tuple(checks)


def _dbt_batches(raw) -> tuple[str, ...]:
    """`batches` as the selectors it names: a JSON array of 1 to 8 non-empty strings."""
    if not isinstance(raw, str):
        raise PlanError(DBT_NEEDS_SELECT)
    try:
        value = json.loads(raw)
    except ValueError:
        raise PlanError(DBT_NEEDS_SELECT) from None
    if (
        not isinstance(value, list)
        or not 1 <= len(value) <= MAX_DBT_BATCHES
        or not all(isinstance(item, str) and item.strip() for item in value)
    ):
        raise PlanError(DBT_NEEDS_SELECT)
    return tuple(item.strip() for item in value)


def _dbt_vars(raw) -> None:
    """`--vars` must be a JSON object of string values; the text is handed to dbt unchanged."""
    if not isinstance(raw, str):
        raise PlanError(DBT_BAD_VARS)
    try:
        value = json.loads(raw)
    except ValueError:
        raise PlanError(DBT_BAD_VARS) from None
    if not isinstance(value, dict) or not all(isinstance(item, str) for item in value.values()):
        raise PlanError(DBT_BAD_VARS)


def _connection_for_pipeline(
    name: str, connection: dict, owner_group: str | None
) -> tuple[str, ...]:
    """The secret keys a pipeline may hand to its task pod, or a `PlanError`.

    `group` connections belong to one group and the pipeline must run as that group's principal;
    `managed` connections are for platform-run work and name the groups in `usableBy`; `personal`
    connections belong to a person and are never handed to a pipeline. The Kubernetes Secret is
    `conn-<name>`; a connection with no `secret` block has no keys and needs no pod variable.

    A connection whose `status.ready` is not true has no Secret to mount; it is refused with the
    Ready condition's sentence, not with a pod that never starts. A connection with no `secret`
    block needs no Secret and is accepted.
    """
    spec = connection.get("spec") or {}
    scope = spec.get("scope") or "personal"
    if scope == "personal":
        raise PlanError(
            f"connection `{name}` is personal; a pipeline runs as a service principal and may use "
            "a group or managed connection"
        )
    if scope == "group" and spec.get("ownerGroup") != owner_group:
        raise PlanError(
            f"connection `{name}` belongs to group `{spec.get('ownerGroup')}`, not to this "
            "pipeline's ownerGroup"
        )
    if scope == "managed" and owner_group not in (spec.get("usableBy") or []):
        raise PlanError(
            f"connection `{name}` is managed and does not list group `{owner_group}` in usableBy"
        )
    keys = tuple((spec.get("secret") or {}).get("keys") or ())
    status = connection.get("status") or {}
    if keys and status.get("ready") is not True:
        ready = next(
            (c for c in status.get("conditions") or [] if c.get("type") == "Ready"), {}
        )
        detail = ready.get("message") or ready.get("reason") or "the sync has not reported yet"
        raise PlanError(
            f"connection `{name}` has no Kubernetes Secret yet: {detail}. "
            "A task pod mounts the synced Secret, so the run would wait for ever"
        )
    for key in keys:
        if not _CONNECTION_KEY.match(key):
            raise PlanError(
                f"connection `{name}` has the secret key `{key}`, which cannot become an "
                "environment variable: use letters, digits and underscores, starting with a "
                "letter or an underscore"
            )
    return keys


def _notebook_params(raw: str | None) -> tuple[tuple[str, str], ...]:
    """The task's parameters as pairs; absent is none, anything else must meet the contract."""
    if raw is None:
        return ()
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        raise PlanError(NOTEBOOK_BAD_PARAMS) from None
    if (
        not isinstance(value, dict)
        or len(value) > MAX_NOTEBOOK_PARAMS
        or not all(isinstance(item, str) for item in value.values())
    ):
        raise PlanError(NOTEBOOK_BAD_PARAMS)
    return tuple(value.items())


def _principal(spec: dict, principals: dict[str, dict]) -> PrincipalRef | None:
    """Which principal the tasks run as; `None` is the platform's default.

    Every mixture that would let one group run as another group's principal is refused: the
    principal must be a `ServicePrincipal` of the namespace, owned by the pipeline's group and
    ready, and its credentials Secret must exist.
    """
    run_as = spec.get("runAs")
    owner_group = spec.get("ownerGroup")
    if run_as is None and owner_group is None:
        return None
    if run_as is None:
        raise PlanError(
            "a pipeline with an owner needs runAs: the principal of its group "
            f"(pipeline-{owner_group})"
        )
    if owner_group is None:
        raise PlanError("runAs needs ownerGroup: say which group owns this pipeline")
    # The shared default principal belongs to no group, so naming it can never make it one's own
    # principal — even if a resource of that name existed.
    resource = None if run_as == DEFAULT_PRINCIPAL else principals.get(run_as)
    if resource is None:
        raise PlanError(f"runAs {run_as} is not a ServicePrincipal of this namespace")
    principal_group = (resource.get("spec") or {}).get("ownerGroup")
    if principal_group != owner_group:
        raise PlanError(
            f"runAs {run_as} belongs to the group {principal_group}, "
            f"this pipeline is owned by {owner_group}"
        )
    status = resource.get("status") or {}
    if status.get("ready") is not True:
        raise PlanError(f"runAs {run_as} is not ready: {_ready_message(status)}")
    secret_name = status.get("secretName")
    if not secret_name:
        raise PlanError(f"runAs {run_as} has no credentials Secret yet")
    return PrincipalRef(run_as, secret_name)


def _ready_message(status: dict) -> str:
    """The message of the principal's `Ready` condition, or the sentence for an empty status."""
    for condition in status.get("conditions") or ():
        if condition.get("type") == "Ready" and condition.get("message"):
            return condition["message"]
    return "no status yet"


def _reject_cycles(tasks: list[TaskPlan]) -> None:
    """Kahn's algorithm: whatever cannot be ordered lies on or behind a cycle."""
    waiting = {t.name: len(t.depends_on) for t in tasks}
    ready = [n for n, count in waiting.items() if count == 0]
    done = 0
    while ready:
        current = ready.pop()
        done += 1
        for t in tasks:
            if current in t.depends_on:
                waiting[t.name] -= 1
                if waiting[t.name] == 0:
                    ready.append(t.name)
    if done != len(tasks):
        raise PlanError("the tasks depend on each other in a circle")
