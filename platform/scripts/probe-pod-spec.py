#!/usr/bin/env python3
"""The Pod document for a throwaway check probe (platform#1656).

Every `make verify-*` check starts short-lived pods in the target namespace to ask
the cluster a question. The namespace enforces `baseline` and warns `restricted`
(`manifests/namespace.yaml`); under `restricted` a pod without a non-root UID,
`RuntimeDefault` seccomp, `allowPrivilegeEscalation: false` and `capabilities.drop:
["ALL"]` is refused, and every one of those checks stops working. The probe images
must be pinned by digest as well: a check that runs different code tomorrow is not a
check.

    python3 scripts/probe-pod-spec.py --name metrics-scrape-probe \
      --image 'curlimages/curl:8.11.1@sha256:<digest>' \
      --command 'sleep 600' --label app.kubernetes.io/name=prometheus

prints a complete `Pod` object for `kubectl run --overrides`. A complete object
replaces the Pod `kubectl run` would generate, so the name, the labels and the
container all come from here.

The command runs as `sh -c 'exec …'`. The `exec` is load bearing: several checks ask
inside the probe with `timeout 3 nc …`, and when a denied port drops the packet that
timeout signals the container's process group. A shell that is PID 1 takes the signal,
the container ends, and every later check of that pod fails — measured on the dev
cluster on 2026-11-17 (platform#1656): the pod died at the second of four checks and
the S3 control read "does not answer". With the command as PID 1 (the pre-#1656
`--command -- sleep 300` shape) the pod survives the same signal. `--command` is
therefore one command, not a script; a script belongs in `--merge`.

    python3 scripts/probe-pod-spec.py --merge < pod-overrides.json

reads a Pod (or a bare Pod spec, as `kubectl run --overrides` accepts) on stdin and
prints it with the same floor added. A key the caller already sets wins — a probe
whose image has its own UID keeps it — and the caller's `command`, `env`,
`volumeMounts` and `volumes` are left untouched.

Both modes exit 2 with `probe-pod-spec: <image> is a floating tag` when a container
image has no `@sha256:` digest.

Standard library only.
"""

import argparse
import json
import sys

UID = 65534
GID = 65534

POD_SECURITY_CONTEXT = {
    "runAsNonRoot": True,
    "runAsUser": UID,
    "runAsGroup": GID,
    "seccompProfile": {"type": "RuntimeDefault"},
}

CONTAINER_SECURITY_CONTEXT = {
    "allowPrivilegeEscalation": False,
    "capabilities": {"drop": ["ALL"]},
}


def fail(message):
    sys.stderr.write("probe-pod-spec: %s\n" % message)
    sys.exit(2)


def pinned(containers):
    """Exit 2 when a container image is a floating tag."""
    for container in containers:
        image = container.get("image") or ""
        if "@sha256:" not in image:
            fail("%s is a floating tag" % (image or "<no image>"))


def labels(pairs):
    found = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            fail("--label takes K=V, got %r" % pair)
        found[key] = value
    return found


def generate(args):
    if not (args.name and args.image and args.command):
        fail("--name, --image and --command are required without --merge")
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": args.name, "labels": labels(args.label)},
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "securityContext": dict(POD_SECURITY_CONTEXT),
            "containers": [{
                "name": args.name,
                "image": args.image,
                "command": ["sh", "-c", "exec " + args.command],
                "securityContext": json.loads(json.dumps(CONTAINER_SECURITY_CONTEXT)),
            }],
        },
    }


def merge(document):
    spec = document.get("spec")
    if not isinstance(spec, dict):
        fail("the document on stdin has no spec")
    containers = spec.get("containers") or []
    if not containers:
        fail("the document on stdin has no spec.containers")
    pinned(containers)

    security = dict(POD_SECURITY_CONTEXT)
    security.update(spec.get("securityContext") or {})
    spec["securityContext"] = security
    spec["automountServiceAccountToken"] = False

    for container in containers:
        context = dict(CONTAINER_SECURITY_CONTEXT)
        context.update(container.get("securityContext") or {})
        # The restricted profile names ALL explicitly; a caller's extra drops stay.
        drop = list((container.get("securityContext") or {}).get("capabilities", {}).get("drop") or [])
        context["capabilities"] = {"drop": drop if "ALL" in drop else drop + ["ALL"]}
        context["allowPrivilegeEscalation"] = False
        container["securityContext"] = context
    return document


def main(argv):
    parser = argparse.ArgumentParser(description="probe Pod documents (platform#1656)")
    parser.add_argument("--merge", action="store_true",
                        help="read a Pod on stdin and add the restricted security floor")
    parser.add_argument("--name", help="the pod's name (and its container's)")
    parser.add_argument("--image", help="the probe image, pinned by digest")
    parser.add_argument("--command", help="the shell command the container runs")
    parser.add_argument("--label", action="append", metavar="K=V",
                        help="a pod label; repeat for more (NetworkPolicies select on them)")
    args = parser.parse_args(argv)

    if args.merge:
        try:
            document = json.load(sys.stdin)
        except ValueError as error:
            fail("stdin is not JSON: %s" % error)
        result = merge(document)
    else:
        result = generate(args)
        pinned(result["spec"]["containers"])

    json.dump(result, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main(sys.argv[1:])
