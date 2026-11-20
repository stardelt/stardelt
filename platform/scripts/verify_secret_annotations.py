"""No Secret carries a credential in kubectl.kubernetes.io/last-applied-configuration (#544).

`kubectl apply` (client-side) stores the whole object in that annotation, secret bytes
included; `kubectl create` and `--server-side` apply do not. Read-only: it lists and exits.
Exit 0 when every Secret is clean, 1 when one carries a credential (and prints the one-line
fix), 2 when kubectl cannot answer.
"""
import json
import os
import re
import subprocess
import sys

KEY = "kubectl.kubernetes.io/last-applied-configuration"
CREDENTIAL = re.compile(r"client-secret|password", re.I)


def main(namespace):
    done = subprocess.run(
        ["kubectl", "-n", namespace, "get", "secrets", "-o", "json"],
        capture_output=True, text=True,
    )
    if done.returncode != 0:
        print(f"verify-secret-annotations: kubectl cannot read the Secrets: {done.stderr.strip()}")
        return 2
    items = json.loads(done.stdout).get("items") or []
    bad = []
    for item in items:
        metadata = item.get("metadata") or {}
        annotation = (metadata.get("annotations") or {}).get(KEY) or ""
        if CREDENTIAL.search(annotation):
            bad.append(metadata.get("name", "?"))
    for name in bad:
        print(
            f"✗ {name} carries a credential in {KEY} — "
            f"kubectl -n {namespace} annotate secret {name} {KEY}-"
        )
    if bad:
        print(f"verify-secret-annotations: FAIL ({len(bad)} of {len(items)} Secret(s))")
        return 1
    print(f"verify-secret-annotations: PASS ({len(items)} Secret(s), no credential in the annotation)")
    return 0


if __name__ == "__main__":
    sys.exit(main(os.environ.get("NAMESPACE", "stardelt")))
