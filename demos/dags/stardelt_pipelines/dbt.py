"""Run a `dbt` task as a pod of its own (`demos/images/dbt`, the tool image).

A `dbt` task is a `KubernetesPodOperator`, exactly like a `notebook` task: the tool image has no
Airflow, so the worker only orchestrates it and the pod runs one process,
`/usr/local/bin/stardelt-dbt`. That entrypoint mints a fresh engine token from the pipeline's
principal on every invocation and `exec`s dbt with the arguments this module builds — a token
lives five minutes, so it is never taken from the image or from the worker.

The pod holds no storage credential: dbt reads the query engine and nothing else. The principal's
`client-id` and `client-secret` arrive through `secretKeyRef` (nothing secret passes through
Python), and the engine's CA Secret is mounted read-only at `/etc/stardelt/trino-ca`.

The sample project ships in the image at `/opt/dbt/projects/<name>`; a task names one with
`config.project`. Customer projects from Git or a pinned archive are not part of this slice.

The pure parts (`project_dir`, `dbt_arguments`, `pod_env`) import no Airflow, so the tests that
cover them run without the loader image.
"""
from __future__ import annotations

import os
import re

from kubernetes.client import models as k8s

try:  # Airflow is in the loader image and on the cluster, not in the unit-test environment.
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
except ImportError:  # pragma: no cover - the operator tests importorskip the provider
    KubernetesPodOperator = object  # type: ignore[assignment,misc]

IMAGE_ENV = "PIPELINE_DBT_IMAGE"
TRINO_CA_SECRET_ENV = "PIPELINE_DBT_TRINO_CA_SECRET"
DEFAULT_IMAGE = "ghcr.io/stardelt/dbt:demo"
PROJECTS_DIR = "/opt/dbt/projects"
TRINO_CA_MOUNT = "/etc/stardelt/trino-ca"
TRINO_CA_FILE = f"{TRINO_CA_MOUNT}/ca.crt"
DEFAULT_TRINO_CA_SECRET = "trino-ca"
MAX_BATCHES = 8
MAX_THREADS = 16
# A project is one directory name under PROJECTS_DIR: lower-case letters, digits and dashes.
_PROJECT = re.compile(r"^[a-z][a-z0-9-]{0,38}$")


def project_dir(project: str) -> str:
    """`/opt/dbt/projects/<project>`, or a `ValueError` when the name is not one directory.

    The name becomes a path, so it is checked here rather than normalized: no slash, no `..`,
    no absolute path can name a directory outside `PROJECTS_DIR`.
    """
    if not isinstance(project, str) or not _PROJECT.match(project):
        raise ValueError(
            f"the dbt project `{project}` is not a project name (1 to 39 lower-case letters, "
            "digits and dashes, starting with a letter)"
        )
    return f"{PROJECTS_DIR}/{project}"


def dbt_arguments(
    *, project: str, select: str, threads: str, vars_text: str | None
) -> list[str]:
    """The arguments of one `dbt build`: a project, one selector, its threads, its vars.

    A list of arguments, never a command string: nothing here goes through a shell.
    """
    arguments = [
        "build",
        "--project-dir",
        project_dir(project),
        "--profiles-dir",
        project_dir(project),
        "--select",
        select,
        "--threads",
        threads,
    ]
    if vars_text is not None:
        arguments += ["--vars", vars_text]
    return arguments


def pod_env(*, principal_secret: str) -> list[k8s.V1EnvVar]:
    """The environment of the dbt pod: the wrapper's contract, and no literal credential.

    `stardelt-dbt` requires the engine URL, the token endpoint and the principal's client
    credentials; the CA file is where the operator mounts the CA Secret. The two endpoints come
    from the loader's own environment, the credentials from the principal's Secret through
    `secretKeyRef`.
    """
    return [
        k8s.V1EnvVar(name="PIPELINE_TRINO_URL", value=_required("PIPELINE_TRINO_URL")),
        k8s.V1EnvVar(name="PIPELINE_TOKEN_URL", value=_required("PIPELINE_TOKEN_URL")),
        k8s.V1EnvVar(name="PIPELINE_TRINO_CA_FILE", value=TRINO_CA_FILE),
        _secret_env("PIPELINE_CLIENT_ID", principal_secret, "client-id"),
        _secret_env("PIPELINE_CLIENT_SECRET", principal_secret, "client-secret"),
    ]


def _secret_env(name: str, secret: str, key: str) -> k8s.V1EnvVar:
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name=secret, key=key)
        ),
    )


def _required(name: str) -> str:
    """A non-empty variable from the loader's environment, or a one-line error naming it."""
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is not set")
    return value


def _ca_secret() -> str:
    """The Secret the pod mounts for the CA: the loader's setting, or the platform's default."""
    return (os.environ.get(TRINO_CA_SECRET_ENV) or "").strip() or DEFAULT_TRINO_CA_SECRET


class DbtPodOperator(KubernetesPodOperator):
    """One `dbt` task, run in a pod of the pinned dbt image.

    The image and the CA Secret come from the loader's environment (`PIPELINE_DBT_*`), read at
    execution time, so a DAG still parses on an install that has no dbt image and the task fails
    with a sentence instead of a missing image.
    """

    def __init__(
        self,
        *,
        pipeline: str,
        owner_group: str,
        project: str,
        select: str,
        threads: str,
        vars_text: str | None,
        principal_secret: str,
        namespace: str,
        **kwargs,
    ) -> None:
        self.pipeline = pipeline
        self.owner_group = owner_group
        self.project = project
        self.select = select
        self.threads = threads
        self.vars_text = vars_text
        self.principal_secret = principal_secret
        super().__init__(
            namespace=namespace,
            image=None,  # set in execute from PIPELINE_DBT_IMAGE
            cmds=["/usr/local/bin/stardelt-dbt"],
            arguments=dbt_arguments(
                project=project, select=select, threads=threads, vars_text=vars_text
            ),
            labels={
                "app.kubernetes.io/name": "pipeline-dbt",
                "stardelt.io/pipeline": pipeline,
                # The owner group's label: its SparkCluster admits it (stardelt-platform#1425).
                f"stardelt.io/group.{owner_group}": "true",
            },
            security_context=k8s.V1PodSecurityContext(
                run_as_non_root=True,
                run_as_user=1000,
                run_as_group=100,
                fs_group=100,
                seccomp_profile=k8s.V1SeccompProfile(type="RuntimeDefault"),
            ),
            container_security_context=k8s.V1SecurityContext(
                allow_privilege_escalation=False,
                capabilities=k8s.V1Capabilities(drop=["ALL"]),
            ),
            automount_service_account_token=False,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "250m", "memory": "1Gi"},
                limits={"cpu": "1", "memory": "2Gi"},
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

    def build_pod_request_obj(self, context=None, **kwargs):
        """The operator's pod, with service links off (platform#315: they leak every Service)."""
        pod = super().build_pod_request_obj(context, **kwargs)
        pod.spec.enable_service_links = False
        return pod

    def execute(self, context):
        """Give the pod its image, its CA Secret and the principal's environment, then run it."""
        self.image = (os.environ.get(IMAGE_ENV) or "").strip() or DEFAULT_IMAGE
        ca_secret = _ca_secret()
        self.volumes = [
            k8s.V1Volume(
                name="trino-ca",
                secret=k8s.V1SecretVolumeSource(
                    secret_name=ca_secret,
                    items=[k8s.V1KeyToPath(key="ca.crt", path="ca.crt")],
                ),
            )
        ]
        self.volume_mounts = [
            k8s.V1VolumeMount(name="trino-ca", mount_path=TRINO_CA_MOUNT, read_only=True)
        ]
        self.env_vars = list(self.env_vars) + pod_env(principal_secret=self.principal_secret)
        return super().execute(context)
