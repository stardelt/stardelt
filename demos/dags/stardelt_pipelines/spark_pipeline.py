"""Run a `spark-pipeline` task as a pod of its own (#80 S2c-2).

The operator renders the graph as the ConfigMap `spark-project-<pipeline>-<task>` (#950/#962):
flat `f00`, `f01`, … keys in path order. The loader renders the same key→path map from the
Pipeline's own `files`, mounts the ConfigMap read-only, and a `python3 -c` runner copies the
graph to /tmp, sets the three keys stardelt owns (`catalog` and `database` from the task's
`writes`, `storage` under the group's bucket), mints the principal's token and runs
`spark-pipelines run` as a thin Spark Connect client — the shape #957 measured (6.6 s, two
flows, no S3 key).

The pod holds no storage credential and creates no Spark of its own: the graph runs on the
shared Connect server, in the principal's session. The token lives only in the pod's env and
in its copy of the spec under /tmp, never in the pod spec, the log or the ConfigMap.
"""
from __future__ import annotations

import json
import os
from collections.abc import Mapping

from kubernetes.client import models as k8s

try:  # Airflow is in the loader image and on the cluster, not in the unit-test environment.
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
except ImportError:  # pragma: no cover - the operator tests importorskip the provider
    KubernetesPodOperator = object  # type: ignore[assignment,misc]

IMAGE_ENV = "PIPELINE_SPARK_IMAGE"
REMOTE_ENV = "PIPELINE_SPARK_REMOTE"
PULL_SECRET_ENV = "PIPELINE_SPARK_PULL_SECRET"
BUCKET_ENV = "PIPELINE_SPARK_BUCKET"
GRAPH_DIR = "/opt/graph"
SPARK_HOME = "/opt/spark"
# The image's own interpreter, the one `platform/images/spark/Dockerfile` installs PyYAML into
# and names as PYSPARK_PYTHON. The Ubuntu `python3` on PATH is 3.10 without yaml, so the runner
# dies on `import yaml` there (measured 2026-11-21, #969).
PYTHON = "/opt/python313/bin/python3"

# The pod's runner: copy the graph under its paths, set the three keys stardelt owns, put the
# principal's token into the catalog block, then run the CLI. `SPARK_CONF_DIR` is an empty
# directory: the image ships no /opt/spark/conf, so there is nothing to strip (#957 Q1).
RUNNER = r'''
import json, os, ssl, subprocess, sys, urllib.parse, urllib.request
from pathlib import Path
import yaml

graph = Path("/opt/graph")
work = Path("/tmp/spark-project")
for key, path in json.loads(os.environ["SPARK_PROJECT_PATHS"]):
    target = work / "graph" / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes((graph / key).read_bytes())

spec = work / "graph" / "spark-pipeline.yml"
document = yaml.safe_load(spec.read_text())
document["catalog"] = os.environ["SPARK_CATALOG"]
document["database"] = os.environ["SPARK_DATABASE"]
document["storage"] = os.environ["SPARK_STORAGE"]
configuration = document.get("configuration") or {}
configuration["spark.remote"] = os.environ["SPARK_REMOTE"]
for name in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
    os.environ.pop(name, None)
body = urllib.parse.urlencode(
    {
        "grant_type": "client_credentials",
        "client_id": os.environ["PIPELINE_CLIENT_ID"],
        "client_secret": os.environ["PIPELINE_CLIENT_SECRET"],
    }
).encode()
request = urllib.request.Request(
    os.environ["PIPELINE_TOKEN_URL"],
    data=body,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
)
with urllib.request.urlopen(request, timeout=30, context=ssl.create_default_context()) as answer:
    token = json.load(answer)["access_token"]
catalog = document["catalog"]
configuration[f"spark.sql.catalog.{catalog}.token"] = token
configuration[f"spark.sql.catalog.{catalog}.header.X-Iceberg-Access-Delegation"] = "vended-credentials"
document["configuration"] = configuration
spec.write_text(yaml.safe_dump(document, sort_keys=False))

conf = work / "conf"
conf.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, SPARK_CONF_DIR=str(conf), HOME="/tmp", SPARK_HOME="/opt/spark")
refresh = os.environ.get("SPARK_REFRESH") or ""
args = ["--full-refresh-all"] if refresh == "all" else (
    ["--full-refresh", refresh] if refresh else []
)
raise SystemExit(
    subprocess.call(
        ["/opt/spark/bin/spark-pipelines", "run", "--spec", str(spec), *args],
        cwd=str(work / "graph"),
        env=env,
    )
)
'''


def config_map_name(pipeline: str, task: str) -> str:
    """The operator's graph ConfigMap (`spark_project.rs::config_map_name`)."""
    return f"spark-project-{pipeline}-{task}"


def key_paths(files: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
    """`(configmap key, relative path)` pairs in the operator's order: `f00` is the first path."""
    return tuple((f"f{index:02}", path) for index, path in enumerate(sorted(files)))


def checkpoint_uri(bucket: str, owner_group: str, pipeline: str, task: str) -> str:
    """Where the graph's streaming checkpoints live: the group's bucket, under `pipelines/`."""
    return f"s3a://{bucket}/pipelines/{owner_group}/{pipeline}/{task}/checkpoints"


def pod_env(
    *,
    paths: tuple[tuple[str, str], ...],
    remote: str,
    catalog: str,
    database: str,
    storage: str,
    refresh: str | None,
    full_refresh: tuple[str, ...],
    principal_secret: str,
) -> list[k8s.V1EnvVar]:
    """The pod's environment: the runner's contract and the principal's Secret by reference."""
    refresh_value = "all" if refresh == "full" else ",".join(full_refresh)
    return [
        k8s.V1EnvVar(
            name="SPARK_PROJECT_PATHS",
            value=json.dumps([[key, path] for key, path in paths]),
        ),
        k8s.V1EnvVar(name="SPARK_REMOTE", value=remote),
        k8s.V1EnvVar(name="SPARK_CATALOG", value=catalog),
        k8s.V1EnvVar(name="SPARK_DATABASE", value=database),
        k8s.V1EnvVar(name="SPARK_STORAGE", value=storage),
        k8s.V1EnvVar(name="SPARK_REFRESH", value=refresh_value),
        k8s.V1EnvVar(name="PIPELINE_TOKEN_URL", value=_required("PIPELINE_TOKEN_URL")),
        _secret_env("PIPELINE_CLIENT_ID", principal_secret, "client-id"),
        _secret_env("PIPELINE_CLIENT_SECRET", principal_secret, "client-secret"),
        k8s.V1EnvVar(name="HOME", value="/tmp"),
        k8s.V1EnvVar(name="SPARK_HOME", value=SPARK_HOME),
        k8s.V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
    ]


def _secret_env(name: str, secret: str, key: str) -> k8s.V1EnvVar:
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name=secret, key=key)
        ),
    )


def _required(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is not set")
    return value


class SparkPipelinePodOperator(KubernetesPodOperator):
    """One `spark-pipeline` task: the graph in a pod of the pinned Spark image (#957's shape).

    The pod spec is built in `execute()` because the image, the Connect URL, the bucket and the
    pull secret come from the loader's environment, so a DAG still parses on an install without
    them and the task fails with a sentence instead of a missing image.
    """

    def __init__(
        self,
        *,
        pipeline: str,
        task_name: str,
        owner_group: str,
        files: Mapping[str, str],
        writes: tuple[str, ...],
        refresh: str | None,
        full_refresh: tuple[str, ...],
        principal_secret: str,
        namespace: str,
        **kwargs,
    ) -> None:
        self.pipeline = pipeline
        self.task_name = task_name
        self.owner_group = owner_group
        self.files = dict(files)
        self.catalog, self.database = writes[0].rsplit(".", 1)[0].split(".", 1)
        self.refresh = refresh
        self.full_refresh = tuple(full_refresh)
        self.principal_secret = principal_secret
        super().__init__(
            namespace=namespace,
            image=None,  # set in execute from PIPELINE_SPARK_IMAGE
            cmds=[PYTHON, "-c", RUNNER],
            labels={
                "app.kubernetes.io/name": "pipeline-spark-pipeline",
                "stardelt.io/pipeline": pipeline,
                # The owner group's label: its SparkCluster admits it (platform#1425).
                f"stardelt.io/group.{owner_group}": "true",
            },
            security_context=k8s.V1PodSecurityContext(
                run_as_non_root=True,
                run_as_user=185,
                run_as_group=185,
                fs_group=185,
                seccomp_profile=k8s.V1SeccompProfile(type="RuntimeDefault"),
            ),
            container_security_context=k8s.V1SecurityContext(
                allow_privilege_escalation=False,
                capabilities=k8s.V1Capabilities(drop=["ALL"]),
            ),
            automount_service_account_token=False,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "250m", "memory": "1Gi"},
                limits={"cpu": "2", "memory": "3Gi"},
            ),
            termination_grace_period=30,
            get_logs=True,
            on_finish_action="delete_pod",
            log_pod_spec_on_failure=False,
            startup_timeout_seconds=300,
            reattach_on_restart=False,
            in_cluster=True,
            random_name_suffix=True,
            do_xcom_push=False,
            **kwargs,
        )

    def execute(self, context):
        paths = key_paths(self.files)
        self.image = _required(IMAGE_ENV)
        self.image_pull_secrets = [
            k8s.V1LocalObjectReference(name=os.environ.get(PULL_SECRET_ENV) or "ghcr-creds")
        ]
        self.env_vars = pod_env(
            paths=paths,
            remote=os.environ.get(REMOTE_ENV)
            or "sc://spark-connect.stardelt.svc.cluster.local:15002",
            catalog=self.catalog,
            database=self.database,
            storage=checkpoint_uri(
                os.environ.get(BUCKET_ENV) or "lakehouse",
                self.owner_group,
                self.pipeline,
                self.task_name,
            ),
            refresh=self.refresh,
            full_refresh=self.full_refresh,
            principal_secret=self.principal_secret,
        )
        self.volumes = [
            k8s.V1Volume(
                name="graph",
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=config_map_name(self.pipeline, self.task_name)
                ),
            ),
            k8s.V1Volume(name="tmp", empty_dir=k8s.V1EmptyDirVolumeSource()),
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(name="graph", mount_path=GRAPH_DIR, read_only=True),
            k8s.V1VolumeMount(name="tmp", mount_path="/tmp"),
        ]
        return super().execute(context)
