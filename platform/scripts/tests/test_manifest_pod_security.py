"""Every pod template under manifests/ declares the fields a `restricted` namespace checks."""

import unittest
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
MANIFESTS = REPO / "manifests"

POD_KINDS = {"Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job"}
CRONJOB = "CronJob"
MAX_NAME = "Pod"


def pod_specs():
    for path in sorted(MANIFESTS.glob("*.yaml")):
        for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if not isinstance(document, dict):
                continue
            kind = document.get("kind")
            if kind in POD_KINDS:
                spec = document["spec"]["template"]["spec"]
            elif kind == CRONJOB:
                spec = document["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            elif kind == MAX_NAME:
                spec = document["spec"]
            else:
                continue
            yield f"{path.name}:{kind}/{document['metadata']['name']}", spec


class ManifestPodSecurity(unittest.TestCase):
    def test_every_template_declares_the_restricted_fields(self):
        problems = []
        for where, spec in pod_specs():
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
