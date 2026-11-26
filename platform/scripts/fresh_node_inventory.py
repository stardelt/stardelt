#!/usr/bin/env python3
"""Write the fresh-node run's inventory from the cluster it just built (#1803).

The Sovereignty page promises "no required outbound calls from stardelt's own
components" and a per-release egress list; nobody could check either claim. This
script turns what one fresh-node run observed into two files next to its report:

* ``egress.txt`` — every name a pod resolved through the cluster's CoreDNS, with the
  pods that asked. CoreDNS logging is switched on by the run's ``enable_dns_log``.
* ``images.txt`` — every image a pod ran, with the digest the node actually pulled.

Standard library only, like the rest of ``scripts/``: the run executes it with the
node's ``python3``.

    fresh_node_inventory.py --coredns-log coredns.log --pods-json pods.json \\
        --own-domain dev.example.test --out-dir <run dir>
"""

import argparse
import json
import re
import sys
from pathlib import Path

# One CoreDNS query line:
# [INFO] 10.42.0.12:41234 - 4567 "AAAA IN acme-v02.api.letsencrypt.org. udp 59 false 1232" NOERROR qr,rd,ra 135 0.020s
QUERY = re.compile(r'\[INFO\] (\S+):\d+ - \d+ "\S+ IN (\S+) [^"]*" (\S+) ')
CLUSTER_SUFFIXES = ("cluster.local", "in-addr.arpa", "ip6.arpa")
EGRESS_HEADER = "# name\tkind\tcallers"
IMAGES_HEADER = "# image\timageID"


def pod_ip_map(pods_json):
    """status.podIP -> <namespace>/<name>, pod-network pods only."""
    by_ip = {}
    for pod in pods_json.get("items", []):
        spec = pod.get("spec") or {}
        if spec.get("hostNetwork"):
            continue
        ip = (pod.get("status") or {}).get("podIP")
        if not ip:
            continue
        metadata = pod.get("metadata") or {}
        namespace = metadata.get("namespace") or "default"
        name = metadata.get("name") or ""
        by_ip[ip] = f"{namespace}/{name}"
    return by_ip


def is_cluster_name(name):
    return any(name.endswith(suffix) for suffix in CLUSTER_SUFFIXES)


def read_egress(log_path, by_ip):
    """name -> set of callers, for answered lookups of names outside the cluster."""
    callers = {}
    with open(log_path, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = QUERY.search(line)
            if not match:
                continue
            client, name, code = match.groups()
            if code != "NOERROR":
                continue
            name = name.lower().rstrip(".")
            if not name or is_cluster_name(name):
                continue
            callers.setdefault(name, set()).add(by_ip.get(client, f"node:{client}"))
    return callers


def read_images(pods_json):
    """Every (image, imageID) a pod ran, init containers included."""
    entries = set()
    for pod in pods_json.get("items", []):
        status = pod.get("status") or {}
        for key in ("containerStatuses", "initContainerStatuses"):
            for container in status.get(key) or []:
                image = container.get("image")
                image_id = container.get("imageID")
                if image:
                    entries.add((image, image_id or ""))
    return sorted(entries)


def is_own(name, own_domain):
    if not own_domain:
        return False
    own_domain = own_domain.lower().strip().rstrip(".")
    return name == own_domain or name.endswith("." + own_domain)


def write_inventory(callers, images, own_domain, out_dir):
    out_dir = Path(out_dir)
    egress = out_dir / "egress.txt"
    with open(egress, "w", encoding="utf-8") as handle:
        handle.write(EGRESS_HEADER + "\n")
        external = own = 0
        for name in sorted(callers):
            kind = "own" if is_own(name, own_domain) else "external"
            if kind == "own":
                own += 1
            else:
                external += 1
            handle.write(f"{name}\t{kind}\t{','.join(sorted(callers[name]))}\n")

    images_file = out_dir / "images.txt"
    with open(images_file, "w", encoding="utf-8") as handle:
        handle.write(IMAGES_HEADER + "\n")
        for image, image_id in images:
            handle.write(f"{image}\t{image_id}\n")

    summary = f"egress: {external} external name(s), {own} own (egress.txt)"
    image_line = f"images: {len(images)} (images.txt)"
    (out_dir / "inventory-summary.txt").write_text(f"{summary}\n{image_line}\n", encoding="utf-8")
    print(summary)
    print(image_line)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Write egress.txt and images.txt for a fresh-node run")
    parser.add_argument("--coredns-log", required=True, help="the CoreDNS pod log from the run")
    parser.add_argument("--pods-json", required=True, help="`kubectl get pods -A -o json` output")
    parser.add_argument("--own-domain", default="", help="the run's own domain (may be empty)")
    parser.add_argument("--out-dir", required=True, help="where egress.txt and images.txt go")
    args = parser.parse_args(argv)

    try:
        with open(args.pods_json, encoding="utf-8") as handle:
            pods_json = json.load(handle)
    except (OSError, ValueError) as error:
        print(f"fresh_node_inventory: cannot read {args.pods_json}: {error}", file=sys.stderr)
        return 1

    try:
        callers = read_egress(args.coredns_log, pod_ip_map(pods_json))
    except OSError as error:
        print(f"fresh_node_inventory: cannot read {args.coredns_log}: {error}", file=sys.stderr)
        return 1

    try:
        Path(args.out_dir).mkdir(parents=True, exist_ok=True)
        write_inventory(callers, read_images(pods_json), args.own_domain, args.out_dir)
    except OSError as error:
        print(f"fresh_node_inventory: cannot write {args.out_dir}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
