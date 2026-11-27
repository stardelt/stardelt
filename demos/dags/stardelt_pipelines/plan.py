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

import yaml

from stardelt_pipelines import wap
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
SPARK_PIPELINE_NEEDS_PRINCIPAL = (
    "a spark-pipeline task needs the pipeline's ownerGroup and runAs: the graph's catalog "
    "token is the principal's"
)
SPARK_PIPELINE_NEEDS_FILES = (
    "spark-pipeline task `{task}` needs files: the graph's spark-pipeline.yml and its sources"
)
SPARK_PIPELINE_NEEDS_WRITES = (
    "spark-pipeline task `{task}` needs writes: the catalog and schema the graph writes, as "
    "catalog.schema.table — stardelt sets the graph's catalog and database from it"
)
SPARK_PIPELINE_TWO_TARGETS = (
    "spark-pipeline task `{task}` writes into more than one catalog/schema ({targets}): a graph "
    "has one target; split the task"
)
SPARK_PIPELINE_OWNS_THE_KEYS = (
    "spark-pipeline task `{task}`: spark-pipeline.yml must not set catalog, database or storage — "
    "the task's writes and the group's bucket set them at run time (#80)"
)
SPARK_PIPELINE_BAD_YAML = "spark-pipeline task `{task}`: spark-pipeline.yml is not a YAML map ({reason})"
SPARK_PIPELINE_BAD_LIBRARIES = "spark-pipeline task `{task}` libraries must be a list of glob entries"
SPARK_PIPELINE_NEEDS_GLOB = (
    "spark-pipeline task `{task}` libraries entries need glob.include: <path or pattern>"
)
SPARK_PIPELINE_BAD_GLOB = (
    "spark-pipeline task `{task}` libraries glob.include must be a file path or a folder "
    "ending in /**: {pattern}"
)
SPARK_PIPELINE_NEEDS_BLOCK = (
    "spark-pipeline task `{task}`: catalog `{catalog}` has no `spark.sql.catalog.{catalog}.type` "
    "under `configuration:` — the Connect session would not know the catalog"
)
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
    "check '{check}' in quality task '{task}' uses ${{table}}, which needs publish: wap in the "
    "pipeline"
)
# `publish: wap` (#114): the run writes an `audit_<hash>` branch, the quality task checks that
# branch and `main` moves only when the checks pass.
PUBLISH_VALUES = ("direct", "wap")
PUBLISH_BAD = "publish must be direct or wap (found `{publish}`)"
WRITES_NOT_TABLE = "task `{task}` writes `{table}`, which is not catalog.schema.table"
WRITES_ONLY_NOTEBOOK = (
    "task `{task}` cannot write a table: only a sql, notebook or spark-pipeline task declares writes"
)
WAP_NEEDS_WRITER = "publish: wap needs a task that writes at least one table"
WAP_PLACEHOLDER_NEEDS_ONE_TABLE = (
    "${{table}} names the audit branch of one table, and this pipeline writes {count}: "
    "name the table in the check, or split the pipeline"
)
# `spec.triggers` (#66): the tables whose update starts a run. Airflow 3.2.2 has one schedule per
# DAG, so a trigger and a cron exclude each other, and one trigger entry is all there is today.
TRIGGERS_MAX_ONE = "a pipeline has one trigger today"
TRIGGERS_NOT_A_LIST = "triggers must be a list"
TRIGGERS_WITH_SCHEDULE = "a pipeline has spec.schedule or spec.triggers, not both"
TRIGGER_NEEDS_TABLES = "a trigger needs at least one table in tablesUpdated"
TRIGGER_ONLY_KEYS = "a trigger accepts only tablesUpdated and mode (found `{key}`)"
TRIGGER_BAD_MODE = "mode must be `all` or `any`"
TRIGGER_BAD_TABLE = "trigger table `{table}` is not catalog.schema.table"
TRIGGER_SELF = (
    "trigger table `{table}` is written by this pipeline: a pipeline cannot be "
    "triggered by its own output"
)


class PlanError(ValueError):
    """The resource cannot become a DAG; the message names the reason."""


@dataclass(frozen=True)
class PrincipalRef:
    """The `ServicePrincipal` a pipeline runs as: its name and the Secret holding its credentials."""

    name: str
    secret_name: str


@dataclass(frozen=True)
class TriggerPlan:
    """The tables whose update starts the pipeline, and whether every or any one counts."""

    tables: tuple[str, ...]
    mode: str  # "all" | "any"


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
    # The connection's non-secret metadata, so a notebook pod can name it without the store.
    connection_type: str | None = None
    connection_config: tuple[tuple[str, str], ...] = ()
    # `dbt` tasks only: the project under the tool image's /opt/dbt/projects, its selectors, the
    # threads and the `--vars` text. Defaults keep every other kind unchanged.
    dbt_project: str | None = None
    dbt_select: tuple[str, ...] = ()
    dbt_threads: str = "4"
    dbt_vars: str | None = None
    # `spark-pipeline` tasks only: the graph's (relative path, text) pairs, the refresh mode
    # (`None` or `full`) and the datasets to rebuild. Defaults keep every other kind unchanged.
    graph: tuple[tuple[str, str], ...] = ()
    refresh: str | None = None
    full_refresh: tuple[str, ...] = ()
    # `quality` tasks only: the SQL assertions the run is gated on. Defaults keep every other kind
    # unchanged.
    checks: tuple[Check, ...] = ()
    # The tables a `notebook` task writes, as `catalog.schema.table`; the audit branch of WAP
    # (`publish: wap`) is cut for each of them.
    writes: tuple[str, ...] = ()


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
    publish: str
    principal: PrincipalRef | None
    tasks: tuple[TaskPlan, ...]
    trigger: TriggerPlan | None = None


def _trigger(raw, written: set[str]) -> TriggerPlan:
    """One `spec.triggers` entry, or a `PlanError`. Only tablesUpdated and mode exist."""
    if not isinstance(raw, dict):
        raise PlanError(TRIGGER_NEEDS_TABLES)
    for key in raw:
        if key not in ("tablesUpdated", "mode"):
            raise PlanError(TRIGGER_ONLY_KEYS.format(key=key))
    tables = raw.get("tablesUpdated")
    if not isinstance(tables, list) or not 1 <= len(tables) <= 10:
        raise PlanError(TRIGGER_NEEDS_TABLES)
    for table in tables:
        if not isinstance(table, str) or not wap.TABLE.match(table):
            raise PlanError(TRIGGER_BAD_TABLE.format(table=table))
        if table in written:
            raise PlanError(TRIGGER_SELF.format(table=table))
    mode = raw.get("mode") or "all"
    if mode not in ("all", "any"):
        raise PlanError(TRIGGER_BAD_MODE)
    return TriggerPlan(tables=tuple(tables), mode=mode)


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


def asset_uri(table: str) -> str:
    """The Airflow asset uri of a table: `stardelt://<catalog>/<schema>/<table>`."""
    return "stardelt://" + table.replace(".", "/")


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
    publish = spec.get("publish") or "direct"
    if publish not in PUBLISH_VALUES:
        raise PlanError(PUBLISH_BAD.format(publish=publish))

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
        if kind not in ("sql", "notebook", "dbt", "quality", "spark-pipeline"):
            raise PlanError(
                f"task `{task_name}` has kind `{kind}`, which has no runtime yet "
                "(only sql, notebook, dbt, quality and spark-pipeline run today)"
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
        graph: tuple[tuple[str, str], ...] = ()
        refresh = None
        full_refresh: tuple[str, ...] = ()
        if kind == "spark-pipeline":
            if not (spec.get("ownerGroup") and spec.get("runAs")):
                raise PlanError(SPARK_PIPELINE_NEEDS_PRINCIPAL)
            graph, refresh, full_refresh = _spark_pipeline_config(task_name, raw)
        checks: tuple[Check, ...] = ()
        if kind == "quality":
            checks = _quality_checks(task_name, raw.get("checks"), publish)
        writes: tuple[str, ...] = ()
        if raw.get("writes"):
            writes = _writes(task_name, kind, raw.get("writes"))
        connection = raw.get("connection")
        connection_keys: tuple[str, ...] = ()
        connection_type: str | None = None
        connection_config: tuple[tuple[str, str], ...] = ()
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
            connection_keys, connection_type, connection_config = _connection_for_pipeline(
                connection, found, spec.get("ownerGroup")
            )
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
                        connection_type=connection_type,
                        connection_config=connection_config,
                        dbt_project=dbt_project,
                        dbt_select=(dbt_select[index],),
                        dbt_threads=dbt_threads,
                        dbt_vars=dbt_vars,
                        graph=(),
                        refresh=None,
                        full_refresh=(),
                        checks=(),
                        writes=(),
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
                connection_type=connection_type,
                connection_config=connection_config,
                dbt_project=dbt_project,
                dbt_select=dbt_select,
                dbt_threads=dbt_threads,
                dbt_vars=dbt_vars,
                graph=graph,
                refresh=refresh,
                full_refresh=full_refresh,
                checks=checks,
                writes=writes,
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
    if publish == "wap" and not any(
        task.writes for task in tasks if task.kind in ("notebook", "spark-pipeline")
    ):
        raise PlanError(WAP_NEEDS_WRITER)

    schedule = spec.get("schedule") or {}
    raw_triggers = spec.get("triggers") or []
    if not isinstance(raw_triggers, list):
        raise PlanError(TRIGGERS_NOT_A_LIST)
    if len(raw_triggers) > 1:
        raise PlanError(TRIGGERS_MAX_ONE)
    if raw_triggers and schedule:
        raise PlanError(TRIGGERS_WITH_SCHEDULE)
    written = {table for task in tasks for table in task.writes}
    trigger = _trigger(raw_triggers[0], written) if raw_triggers else None
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
        publish=publish,
        principal=_principal(spec, principals or {}),
        tasks=tuple(tasks),
        trigger=trigger,
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


def _spark_glob_include_problem(include):
    """Spark's CLI accepts a file path or a folder ending in `/**`; anything else dies in the pod (#1031).

    Measured in #1025: `include: "*.sql"` answers
    `[PIPELINE_SPEC_INVALID_GLOB_PATTERN] Invalid glob pattern … Only file paths, or folder paths
    ending with /** are allowed.` The rule is checked here so the deploy refuses it, not the run.
    """
    if include.endswith("/**") and include.count("*") == 2 and len(include) > len("/**"):
        return None
    if "*" not in include and not include.endswith("/"):
        return None
    return include


def _spark_pipeline_config(
    task_name: str, raw: dict
) -> tuple[tuple[tuple[str, str], ...], str | None, tuple[str, ...]]:
    """The graph's files, its refresh mode and its full-refresh datasets, or a `PlanError`.

    The graph must not set `catalog`, `database` or `storage` (the bundle refuses those too):
    stardelt sets them from the task's `writes` — one catalog and one schema per graph — and
    from the group's bucket. `spark-pipeline.yml` must be present and be a map; its
    `configuration:` must carry the `spark.sql.catalog.<catalog>.type` block the loader gives
    the principal's token to (#957 Q2/Q3).
    """
    files = raw.get("files")
    if not isinstance(files, dict) or "spark-pipeline.yml" not in files:
        raise PlanError(SPARK_PIPELINE_NEEDS_FILES.format(task=task_name))
    text = files["spark-pipeline.yml"]
    if not isinstance(text, str):
        raise PlanError(SPARK_PIPELINE_NEEDS_FILES.format(task=task_name))
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as error:
        reason = " ".join(str(error).split())[:200]
        raise PlanError(SPARK_PIPELINE_BAD_YAML.format(task=task_name, reason=reason)) from None
    if not isinstance(document, dict):
        raise PlanError(SPARK_PIPELINE_BAD_YAML.format(task=task_name, reason="not a map"))
    owned = [key for key in ("catalog", "database", "storage") if key in document]
    if owned:
        raise PlanError(SPARK_PIPELINE_OWNS_THE_KEYS.format(task=task_name))
    libraries = document.get("libraries")
    if libraries is not None:
        if not isinstance(libraries, list) or not libraries:
            raise PlanError(SPARK_PIPELINE_BAD_LIBRARIES.format(task=task_name))
        for entry in libraries:
            include = None
            if isinstance(entry, dict) and isinstance(entry.get("glob"), dict):
                include = entry["glob"].get("include")
            if not isinstance(include, str) or not include:
                raise PlanError(SPARK_PIPELINE_NEEDS_GLOB.format(task=task_name))
            if _spark_glob_include_problem(include):
                raise PlanError(
                    SPARK_PIPELINE_BAD_GLOB.format(task=task_name, pattern=include)
                )
    writes = raw.get("writes")
    if not isinstance(writes, list) or not writes:
        raise PlanError(SPARK_PIPELINE_NEEDS_WRITES.format(task=task_name))
    if any(
        not isinstance(table, str) or not wap.TABLE.match(table) for table in writes
    ):
        raise PlanError(WRITES_NOT_TABLE.format(task=task_name, table=writes))
    targets = {table.rsplit(".", 1)[0] for table in writes}
    if len(targets) != 1:
        raise PlanError(SPARK_PIPELINE_TWO_TARGETS.format(
            task=task_name, targets=", ".join(sorted(targets))
        ))
    catalog = next(iter(targets)).split(".")[0]
    configuration = document.get("configuration") or {}
    key = f"spark.sql.catalog.{catalog}.type"
    if not isinstance(configuration, dict) or key not in configuration:
        raise PlanError(
            SPARK_PIPELINE_NEEDS_BLOCK.format(task=task_name, catalog=catalog)
        )
    for path, content in files.items():
        if not isinstance(path, str) or not isinstance(content, str):
            raise PlanError(SPARK_PIPELINE_NEEDS_FILES.format(task=task_name))
    refresh = raw.get("refresh")
    if refresh is not None and refresh not in ("incremental", "full"):
        raise PlanError(
            f"spark-pipeline task `{task_name}` sets refresh: incremental or full"
        )
    full_refresh = raw.get("fullRefresh") or ()
    if not isinstance(full_refresh, (list, tuple)) or any(
        not isinstance(name, str) or not name for name in full_refresh
    ):
        raise PlanError(
            f"spark-pipeline task `{task_name}` sets fullRefresh as a list of dataset names"
        )
    return tuple(sorted(files.items())), refresh, tuple(full_refresh)


def refuse_unrunnable(p: PipelinePlan) -> None:
    """Refuse a plan the loader cannot run as declared; called by the DAG, not by `plan`."""
    if p.publish != "wap":
        return
    tables = sorted({table for task in p.tasks for table in task.writes})
    placeholder = any("${table}" in check.sql for task in p.tasks for check in task.checks)
    if placeholder and len(tables) > 1:
        raise PlanError(WAP_PLACEHOLDER_NEEDS_ONE_TABLE.format(count=len(tables)))


def _writes(task_name: str, kind: str, raw) -> tuple[str, ...]:
    """The tables a task declares it writes, or a `PlanError`.

    Only a `sql`, `notebook` or `spark-pipeline` task writes a table, and every table is
    `catalog.schema.table`.
    """
    if kind not in ("sql", "notebook", "spark-pipeline"):
        raise PlanError(WRITES_ONLY_NOTEBOOK.format(task=task_name))
    if not isinstance(raw, list):
        raise PlanError(WRITES_NOT_TABLE.format(task=task_name, table=raw))
    for table in raw:
        if not isinstance(table, str) or not wap.TABLE.match(table):
            raise PlanError(WRITES_NOT_TABLE.format(task=task_name, table=table))
    return tuple(raw)


def _quality_checks(task_name: str, raw, publish: str) -> tuple[Check, ...]:
    """The SQL assertions of a `quality` task, or a `PlanError`.

    One to 50 mappings, each only `name`, `sql` and `severity` (absent is `error`); a name is
    unique within the task, the statement non-empty, and `${table}` is accepted only with
    `publish: wap`: it is the run's audit branch.
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
        if "${table}" in sql and publish != "wap":
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
) -> tuple[tuple[str, ...], str | None, tuple[tuple[str, str], ...]]:
    """The secret keys a pipeline may hand to its task pod, or a `PlanError`.

    `group` connections belong to one group and the pipeline must run as that group's principal;
    `managed` connections are for platform-run work and name the groups in `usableBy`; `personal`
    connections belong to a person and are never handed to a pipeline. The Kubernetes Secret is
    `conn-<name>`; a connection with no `secret` block has no keys and needs no pod variable.
    The connection's type and settings ride to the pod too, so the notebook reads them without
    the platform's secret store.

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
    return keys, spec.get("type"), tuple(sorted((spec.get("config") or {}).items()))


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
