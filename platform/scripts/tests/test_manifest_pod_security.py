"""Every pod template under manifests/ declares the fields a `restricted` namespace checks."""

import unittest
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
MANIFESTS = REPO / "manifests"

POD_KINDS = {"Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job"}
CRONJOB = "CronJob"
MAX_NAME = "Pod"


def pod_spec(document):
    """The pod-carrying spec of one document, or None when it carries no pod."""
    if not isinstance(document, dict):
        return None
    kind = document.get("kind")
    if kind in POD_KINDS:
        return document["spec"]["template"]["spec"]
    if kind == CRONJOB:
        return document["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    if kind == MAX_NAME:
        return document["spec"]
    return None


def pod_specs():
    for path in sorted(MANIFESTS.glob("*.yaml")):
        for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            spec = pod_spec(document)
            if spec is not None:
                yield f"{path.name}:{document['kind']}/{document['metadata']['name']}", spec
            if not isinstance(document, dict) or document.get("kind") != "ConfigMap":
                continue
            # #1382: a pod template can ride as a string in ConfigMap.data — the shared
            # server's executor template does (spark.yaml, executor-pod-template.yaml).
            # Unparsed, a regression in it passes this test unseen.
            for key, value in (document.get("data") or {}).items():
                if not isinstance(value, str):
                    continue
                try:
                    inner = list(yaml.safe_load_all(value))
                except yaml.YAMLError:
                    continue
                for nested in inner:
                    nested_spec = pod_spec(nested)
                    if nested_spec is not None:
                        where = f"{path.name}:ConfigMap/{document['metadata']['name']}:{key}"
                        yield where, nested_spec


class ManifestPodSecurity(unittest.TestCase):
    def test_every_template_declares_the_restricted_fields(self):
        specs = list(pod_specs())
        # #1382: a glob that stops matching or a parse that yields nothing would make every
        # assertion below run on an empty list. 16 top-level templates + the shared server's
        # executor pod template in ConfigMap spark-connect-config.
        self.assertEqual(len(specs), 17, "the scan lost a pod template")
        problems = []
        for where, spec in specs:
            pod = spec.get("securityContext") or {}
            containers = (spec.get("containers") or []) + (spec.get("initContainers") or [])
            if pod.get("runAsNonRoot") is not True and not all(
                (c.get("securityContext") or {}).get("runAsNonRoot") is True for c in containers
            ):
                problems.append(f"{where}: runAsNonRoot")
            seccomp = pod.get("seccompProfile") or {}
            if seccomp.get("type") != "RuntimeDefault" and not all(
                ((c.get("securityContext") or {}).get("seccompProfile") or {}).get("type")
                == "RuntimeDefault"
                for c in containers
            ):
                problems.append(f"{where}: seccompProfile")
            for container in containers:
                context = container.get("securityContext") or {}
                if context.get("allowPrivilegeEscalation") is not False:
                    problems.append(f"{where}: {container.get('name')} allowPrivilegeEscalation")
                drop = (context.get("capabilities") or {}).get("drop") or []
                if "ALL" not in drop:
                    problems.append(f"{where}: {container.get('name')} capabilities.drop")
        self.assertEqual(problems, [])
