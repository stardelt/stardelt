"""One Airflow DAG per `Pipeline` resource in this namespace (scheduled jobs, phase J0).

The resource is the source of truth; this file only reads it (one LIST per parse, through a
read-only ServiceAccount) and builds DAGs from it, so nobody edits DAGs by hand. The pure
parts live in `stardelt_pipelines` and are unit-tested without Airflow.

A pipeline with an `ownerGroup` runs as that group's `ServicePrincipal` (`runAs`): the plan checks
it against the namespace's `ServicePrincipal` resources, and each of the pipeline's task pods gets
the principal's credentials through a `secretKeyRef` (see `stardelt_pipelines.principal`). The
loader itself never reads a Secret.

A stopped run (the executor sends SIGTERM) cancels the running statement: the task deletes the
statement's `nextUri`, so the next poll reports `USER_CANCELED` and the task fails with that
engine error instead of leaving the query running behind it.

Environment of the dag-processor and the task pods:
  PIPELINE_TRINO_URL        base URL of the query engine, e.g. https://trino:8443
  PIPELINE_TRINO_CA_FILE    CA bundle that signed the engine's certificate (optional; the token POST uses the public trust store)
  PIPELINE_CATALOG_URL      base URL of the catalog, e.g. http://lakekeeper.stardelt.svc.cluster.local:8181
  PIPELINE_TOKEN_URL        token endpoint of the identity provider
  PIPELINE_CLIENT_ID        the default principal, `pipeline-runner` (a pipeline with a group
                            principal overrides it in its own task pods)
  PIPELINE_CLIENT_SECRET    its secret (from a Kubernetes Secret, never in a file)
  PIPELINE_NOTEBOOK_IMAGE   the notebook image a `notebook` task's own pod runs (platform#316)
  PIPELINE_NOTEBOOK_BUCKET  the versioned bucket of published notebooks and executed copies
  PIPELINE_NOTEBOOK_S3_CONN the Airflow connection that pre-signs the notebook's GET and PUT
  PIPELINE_NOTEBOOK_TRINO_CA_SECRET  the Secret mounted into the notebook pod for the CA
  PIPELINE_NOTEBOOK_KERNEL_TIMEOUT  seconds a notebook pod waits for its kernel (optional, 30 to 900, default 180)
  PIPELINE_DBT_IMAGE        the dbt tool image a `dbt` task's own pod runs (default `ghcr.io/stardelt/dbt:demo`)
  PIPELINE_DBT_TRINO_CA_SECRET  the Secret mounted into the dbt pod for the CA (default `trino-ca`)
  PIPELINE_SPARK_IMAGE      the Spark image a `spark-pipeline` task's own pod runs (must equal
                            chart/files/spark-runtimes.json; scripts/check-spark-image-pin.sh
                            checks it)
  PIPELINE_SPARK_REMOTE     the Connect URL the graph runs on (default: the owner group's Spark
                            cluster, the short name `sc://spc-<group>-server:15002`)
  PIPELINE_SPARK_BUCKET     the bucket under which a graph's checkpoints live (default lakehouse)
  PIPELINE_SPARK_PULL_SECRET the image pull secret of that pod (default `ghcr-creds`)
  PIPELINE_API_GROUP / PIPELINE_API_PLURAL   defaults `stardelt.io` / `pipelines` (tests override)
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import httpx
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset, DAG
from airflow.timetables.trigger import CronTriggerTimetable

# The support package sits next to this file. Airflow does not put the DAGs folder on the
# import path in every process (dag-processor, task pod, tests), so do it here.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from stardelt_pipelines import quality, trino, wap  # noqa: E402
from stardelt_pipelines.access import dag_access  # noqa: E402
from stardelt_pipelines.dbt import DbtPodOperator  # noqa: E402
from stardelt_pipelines.expand import expand, run_values  # noqa: E402
from stardelt_pipelines.notebook import NotebookPodOperator  # noqa: E402
from stardelt_pipelines.plan import (  # noqa: E402
    DEFAULT_PRINCIPAL,
    PlanError,
    asset_uri,
    plan,
    refuse_unrunnable,
)
from stardelt_pipelines.principal import principal_pod_override  # noqa: E402
from stardelt_pipelines.spark_pipeline import SparkPipelinePodOperator  # noqa: E402

log = logging.getLogger(__name__)

API_GROUP = os.environ.get("PIPELINE_API_GROUP", "stardelt.io")
API_PLURAL = os.environ.get("PIPELINE_API_PLURAL", "pipelines")
API_VERSION = "v1alpha1"
PRINCIPAL_PLURAL = "serviceprincipals"
CONNECTION_PLURAL = "connections"
NAMESPACE_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
TRINO_SOURCE = "stardelt-pipeline"


def _namespace() -> str:
    with open(NAMESPACE_FILE) as handle:
        return handle.read().strip()


def _list_resources(
    namespace: str,
) -> tuple[list[dict], dict[str, dict], dict[str, dict]]:
    """Every `Pipeline` of the namespace, plus its `ServicePrincipal` and `Connection` resources by
    name."""
    from kubernetes import client, config

    config.load_incluster_config()
    api = client.CustomObjectsApi()
    pipelines = api.list_namespaced_custom_object(
        API_GROUP, API_VERSION, namespace, API_PLURAL
    ).get("items", [])
    principals = api.list_namespaced_custom_object(
        API_GROUP, API_VERSION, namespace, PRINCIPAL_PLURAL
    ).get("items", [])
    connections = api.list_namespaced_custom_object(
        API_GROUP, API_VERSION, namespace, CONNECTION_PLURAL
    ).get("items", [])
    return (
        pipelines,
        {
            resource["metadata"]["name"]: resource
            for resource in principals
            if resource.get("metadata", {}).get("name")
        },
        {
            resource["metadata"]["name"]: resource
            for resource in connections
            if resource.get("metadata", {}).get("name")
        },
    )


def _provenance(resource: dict) -> dict[str, str]:
    """The deployment a Pipeline was written by: its two labels and the revision annotation."""
    meta = resource.get("metadata") or {}
    labels = meta.get("labels") or {}
    annotations = meta.get("annotations") or {}
    found = {}
    if isinstance(labels.get("stardelt.io/bundle"), str):
        found["bundle"] = labels["stardelt.io/bundle"]
    if isinstance(labels.get("stardelt.io/target"), str):
        found["target"] = labels["stardelt.io/target"]
    if isinstance(annotations.get("stardelt.io/revision"), str):
        found["revision"] = annotations["stardelt.io/revision"]
    return found


def _run_task(
    task: dict, params: dict, provenance: dict[str, str] | None = None, **context
) -> None:
    """Executes inside the task pod. `sql` tasks run as the service principal, never logging it."""
    if task["kind"] != "sql":
        # A silent no-op for a kind that has no runtime yet would hide that nothing ran.
        raise NotImplementedError(f"task kind `{task['kind']}` has no runtime yet")
    dag_run = context["dag_run"]
    fallback = (context.get("logical_date") or dag_run.run_after).date().isoformat()
    run_date, overrides = run_values(dag_run.conf, fallback)
    sql = expand(
        task["sql"],
        run_date=run_date,
        run_id=dag_run.run_id,
        params={**params, **overrides},
    )
    tags = trino.lineage_tags(
        context["dag"].dag_id, context["ti"].task_id, dag_run.run_id, provenance
    )

    ca_file = os.environ.get("PIPELINE_TRINO_CA_FILE")
    with trino.identity_client() as identity, httpx.Client(
        verify=ca_file or True, timeout=60.0
    ) as http:
        token = trino.client_credentials_token(
            os.environ["PIPELINE_TOKEN_URL"],
            os.environ.get("PIPELINE_CLIENT_ID", DEFAULT_PRINCIPAL),
            os.environ["PIPELINE_CLIENT_SECRET"],
            client=identity,
        )
        # What a stop needs: the id as soon as the engine has it, and the URI to DELETE. Without
        # the cancel the statement outlives the task (and can still commit a write).
        current: dict[str, str | None] = {"query_id": None, "next_uri": None}

        def _started(query_id: str, next_uri: str | None) -> None:
            if current["query_id"] != query_id:
                log.info("query %s submitted", query_id)
            current["query_id"] = query_id
            current["next_uri"] = next_uri

        # The executor SIGTERMs the task when the run is stopped; the handler cancels the
        # statement and the next poll turns it into a real `USER_CANCELED` error.
        handler = trino.cancel_handler(
            http, token=token, current=current, source=TRINO_SOURCE, tags=tags
        )
        with trino.cancel_on_termination(handler):
            outcome = trino.run_statement(
                sql,
                base_url=os.environ["PIPELINE_TRINO_URL"],
                token=token,
                client=http,
                source=TRINO_SOURCE,
                tags=tags,
                on_started=_started,
            )
    log.info(
        "task %s finished: query %s, %s rows, update count %s",
        task["name"],
        outcome.query_id,
        outcome.rows,
        outcome.update_count,
    )


def _catalog_http(token: str) -> httpx.Client:
    """The catalog's REST API with the run principal's bearer token (its audience must
    include `lakekeeper`; a principal without it gets the catalog's 401)."""
    return httpx.Client(headers={"Authorization": f"Bearer {token}"}, timeout=60.0)


def _run_quality(
    checks: list[dict], params: dict, publish: str = "direct", audit_table: str | None = None,
    provenance: dict[str, str] | None = None, **context,
) -> None:
    """Executes inside the task pod. Runs each check as the service principal, never logging it."""
    dag_run = context["dag_run"]
    fallback = (context.get("logical_date") or dag_run.run_after).date().isoformat()
    run_date, overrides = run_values(dag_run.conf, fallback)
    tags = trino.lineage_tags(
        context["dag"].dag_id, context["ti"].task_id, dag_run.run_id, provenance
    )

    ca_file = os.environ.get("PIPELINE_TRINO_CA_FILE")
    with trino.identity_client() as identity:
        token = trino.client_credentials_token(
            os.environ["PIPELINE_TOKEN_URL"],
            os.environ.get("PIPELINE_CLIENT_ID", DEFAULT_PRINCIPAL),
            os.environ["PIPELINE_CLIENT_SECRET"],
            client=identity,
        )
    with httpx.Client(
        verify=ca_file or True, timeout=60.0
    ) as http, _catalog_http(token) as catalog:

        def run_one(sql: str) -> int:
            if "${table}" in sql:
                sql = wap.resolve_table(
                    sql,
                    audit_table,
                    wap.branch_snapshot(
                        catalog,
                        os.environ["PIPELINE_CATALOG_URL"],
                        audit_table,
                        wap.branch_name(dag_run.run_id),
                    ),
                )
            statement = expand(
                sql,
                run_date=run_date,
                run_id=dag_run.run_id,
                params={**params, **overrides},
            )
            outcome = trino.run_statement(
                statement,
                base_url=os.environ["PIPELINE_TRINO_URL"],
                token=token,
                client=http,
                source=TRINO_SOURCE,
                tags=tags,
            )
            return outcome.rows

        failed = quality.evaluate([quality.Check(**raw) for raw in checks], run_one)
    if failed:
        # One run tells the person everything the checks found, never only the first failure.
        raise RuntimeError(_quality_failure_sentence(failed))
    log.info("quality gate passed: %d checks", len(checks))


def _quality_failure_sentence(failed: list[tuple[str, int]]) -> str:
    """`2 quality checks failed: no_null_ids (3 rows), unique_ids (1 row)`."""
    count = len(failed)
    named = ", ".join(
        f"{name} ({rows} row{'' if rows == 1 else 's'})" for name, rows in failed
    )
    return f"{count} quality check{'s' if count != 1 else ''} failed: {named}"


WAP_PREPARE_TASK = "wap-prepare"
WAP_PUBLISH_TASK = "wap-publish"


def _wap_prepare(tables: list[str], **context) -> dict:
    """Create this run's audit branch on every written table, before any writer runs."""
    dag_run = context["dag_run"]
    branch = wap.branch_name(dag_run.run_id)
    with trino.identity_client() as identity:
        token = trino.client_credentials_token(
            os.environ["PIPELINE_TOKEN_URL"],
            os.environ.get("PIPELINE_CLIENT_ID", DEFAULT_PRINCIPAL),
            os.environ["PIPELINE_CLIENT_SECRET"],
            client=identity,
        )
    with _catalog_http(token) as catalog:
        main = wap.prepare(catalog, os.environ["PIPELINE_CATALOG_URL"], tables, branch)
    log.info("wap: branch %s from main %s", branch, main)
    return {"branch": branch, "main": main}


def _wap_publish(tables: list[str], **context) -> None:
    """Move `main` to the branch after the checks passed, then drop the branch."""
    dag_run = context["dag_run"]
    prepared = context["ti"].xcom_pull(task_ids=WAP_PREPARE_TASK) or {}
    branch = prepared.get("branch") or wap.branch_name(dag_run.run_id)
    main = prepared.get("main") or {}
    with trino.identity_client() as identity:
        token = trino.client_credentials_token(
            os.environ["PIPELINE_TOKEN_URL"],
            os.environ.get("PIPELINE_CLIENT_ID", DEFAULT_PRINCIPAL),
            os.environ["PIPELINE_CLIENT_SECRET"],
            client=identity,
        )
    with _catalog_http(token) as catalog:
        for table in tables:
            wap.publish(catalog, os.environ["PIPELINE_CATALOG_URL"], table, branch, int(main[table]))
            wap.drop_branch(catalog, os.environ["PIPELINE_CATALOG_URL"], table, branch)
    log.info("wap: published %s, dropped branch %s", ", ".join(tables), branch)


def _asset(table: str) -> Asset:
    """The asset an outlet raises and a trigger waits for; one helper, one uri."""
    return Asset(asset_uri(table))


def _schedule(p):
    """The DAG's schedule: `[Asset]` for `all`, `Asset | Asset` for `any`, the cron otherwise."""
    if not p.trigger:
        return CronTriggerTimetable(p.cron, timezone=p.timezone) if p.cron else None
    assets = [_asset(table) for table in p.trigger.tables]
    if p.trigger.mode == "all":
        return assets
    schedule = assets[0]
    for asset in assets[1:]:
        schedule = schedule | asset
    return schedule


def _build_dag(p, provenance: dict[str, str] | None = None) -> DAG:
    refuse_unrunnable(p)
    access = dag_access(p.owner_group)
    dag = DAG(
        dag_id=p.dag_id,
        schedule=_schedule(p),
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        is_paused_upon_creation=False,
        max_active_runs=p.max_active_runs,
        dagrun_timeout=timedelta(seconds=p.timeout_s) if p.timeout_s else None,
        default_args={
            "owner": access["owner"],
            "retries": p.retries,
            "retry_delay": timedelta(seconds=p.retry_delay_s),
        },
        tags=["stardelt", "pipeline", *access["tags"]],
        access_control=access["access_control"],
    )
    # The credentials of a group's principal reach the pods of that pipeline only: every other
    # DAG keeps the default principal the deployment sets.
    executor_config = (
        principal_pod_override(p.principal.secret_name) if p.principal is not None else None
    )
    operators = {}
    wap_tables = sorted({table for t in p.tasks for table in t.writes})
    wap_prepare = wap_publish = None
    if p.publish == "wap":
        # retries=0: a retried prepare would move an existing branch back to a newer `main`.
        wap_prepare = PythonOperator(
            task_id=WAP_PREPARE_TASK,
            python_callable=_wap_prepare,
            op_kwargs={"tables": wap_tables},
            retries=0,
            executor_config=executor_config,
            dag=dag,
        )
        wap_publish = PythonOperator(
            task_id=WAP_PUBLISH_TASK,
            python_callable=_wap_publish,
            op_kwargs={"tables": wap_tables},
            retries=0,
            executor_config=executor_config,
            dag=dag,
        )
    for t in p.tasks:
        if t.kind == "notebook":
            # A notebook runs in a pod of its own: the worker only orchestrates it, so the pod
            # override that hands the worker its credentials is not this task's business.
            if p.principal is None:  # the plan refuses this; a silent default would run as nobody
                raise PlanError(f"notebook task `{t.name}` has no principal")
            operators[t.name] = NotebookPodOperator(
                task_id=t.name,
                pipeline=p.name,
                owner_group=p.owner_group,
                notebook=t.notebook,
                object_version=t.object_version,
                task_params=dict(t.notebook_params),
                pipeline_params=p.params,
                principal_secret=p.principal.secret_name,
                # The ServiceAccount the operator creates for the principal (`sp-<name>`); the
                # worker may `use` it (#1260). The pod's catalog credential is its projected token.
                principal_sa=f"sp-{p.principal.name}",
                connection_name=t.connection,
                connection_keys=t.connection_keys,
                connection_type=t.connection_type,
                connection_config=t.connection_config,
                wap=p.publish == "wap",
                outlets=[_asset(table) for table in t.writes],
                namespace=namespace,
                dag=dag,
            )
            continue
        if t.kind == "dbt":
            # A dbt run is a pod of its own too: the tool image has no Airflow, and the pod
            # mints its own token from the principal's Secret, so the worker's pod override is
            # not this task's business.
            if p.principal is None:  # the plan refuses this; a silent default would run as nobody
                raise PlanError(f"dbt task `{t.name}` has no principal")
            operators[t.name] = DbtPodOperator(
                task_id=t.name,
                pipeline=p.name,
                owner_group=p.owner_group,
                project=t.dbt_project,
                select=t.dbt_select[0],
                threads=t.dbt_threads,
                vars_text=t.dbt_vars,
                principal_secret=p.principal.secret_name,
                namespace=namespace,
                dag=dag,
            )
            continue
        if t.kind == "spark-pipeline":
            # The graph runs in a pod of the Spark image as a thin Connect client (#957): the worker
            # has no Spark, and the principal's token must not pass through it either.
            if p.principal is None:  # the plan refuses this; a silent default would run as nobody
                raise PlanError(f"spark-pipeline task `{t.name}` has no principal")
            operators[t.name] = SparkPipelinePodOperator(
                task_id=t.name,
                pipeline=p.name,
                task_name=t.name,
                owner_group=p.owner_group,
                files=dict(t.graph),
                writes=t.writes,
                refresh=t.refresh,
                full_refresh=t.full_refresh,
                # The ServiceAccount the operator creates for the principal
                # (`crd.rs::principal_client_id`: `sp-<name>`); the worker may `use` it (#1260).
                principal_sa=f"sp-{p.principal.name}",
                wap=p.publish == "wap",
                outlets=[_asset(table) for table in t.writes],
                namespace=namespace,
                dag=dag,
            )
            continue
        if t.kind == "quality":
            operators[t.name] = PythonOperator(
                task_id=t.name,
                python_callable=_run_quality,
                op_kwargs={
                    "checks": [
                        {"name": c.name, "sql": c.sql, "severity": c.severity}
                        for c in t.checks
                    ],
                    "params": p.params,
                    "publish": p.publish,
                    "audit_table": wap_tables[0] if len(wap_tables) == 1 else None,
                    "provenance": provenance,
                },
                executor_config=executor_config,
                dag=dag,
            )
            continue
        operators[t.name] = PythonOperator(
            task_id=t.name,
            python_callable=_run_task,
            op_kwargs={
                "task": {"name": t.name, "kind": t.kind, "sql": t.sql},
                "params": p.params,
                "provenance": provenance,
            },
            executor_config=executor_config,
            dag=dag,
        )
    for t in p.tasks:
        for dep in t.depends_on:
            operators[dep] >> operators[t.name]
    if wap_prepare is not None:
        for t in p.tasks:
            if t.writes:
                wap_prepare >> operators[t.name]
        for t in p.tasks:
            if t.kind == "quality":
                operators[t.name] >> wap_publish
    return dag


namespace = _namespace()
pipelines, principals, connections = _list_resources(namespace)
for resource in pipelines:
    name = resource.get("metadata", {}).get("name", "?")
    try:
        pipeline_plan = plan(resource, namespace, principals, connections)
        globals()[pipeline_plan.dag_id] = _build_dag(pipeline_plan, _provenance(resource))
    except PlanError as problem:
        log.warning("pipeline %s/%s is not loaded: %s", namespace, name, problem)
