"""Spark compute from code (internal#161 slice 6): list, start and stop a SparkCluster.

The operator owns the cluster; Nova exposes it. Nothing here starts a cluster implicitly —
the maintainer's decision: a person presses Start (or `stardelt.compute.start`).
"""

from __future__ import annotations

import time

from stardelt.api import Client


class ClusterStopped(RuntimeError):
    """The cluster exists and the caller may use it, but it is Stopped (or Failed)."""


def _client(client=None):
    return client or Client()


def list(client=None):
    """Every cluster this person may use, as Nova answers them (`GET /api/compute`)."""
    return _client(client).call("compute_list")["clusters"]


def get(name, client=None):
    """One cluster by name, or `LookupError` when it does not exist or the caller may not use it."""
    for cluster in list(client):
        if cluster["name"] == name:
            return cluster
    raise LookupError(f"no Spark cluster named {name!r} that you may use")


def default(client=None):
    """The one Spark cluster this person's group owns; `LookupError` naming what they have instead.

    The library's answer to "which cluster is mine". Only a cluster the caller may act on counts
    (its owning group, or an administrator), never a Spark server the platform's manifests install
    (`kind: platform`) and never the `shared` one. Ambiguity is an error, not a guess: a person in
    two groups that own clusters gets a sentence naming them, so they choose.
    """
    clusters = [
        cluster
        for cluster in list(client)
        if cluster.get("kind", "cluster") == "cluster"
        and not cluster.get("shared")
        and cluster.get("canAct")
    ]
    if len(clusters) == 1:
        return clusters[0]
    names = ", ".join(sorted(cluster["name"] for cluster in clusters))
    if not clusters:
        raise LookupError(
            "you have no Spark cluster of your own: no group you are in owns one — "
            "use the shared Spark server (or %sql), ask an administrator for a cluster, "
            'or create your group\'s with stardelt.compute.ensure("<your group>")'
        )
    raise LookupError(
        f"you may act on {len(clusters)} Spark clusters ({names}): name one with "
        "STARDELT_CLUSTER, or pass its name to stardelt.compute.get/start"
    )


def ensure(group, client=None):
    """The group's Spark cluster: reuse it, or create it now (``POST /api/compute/ensure``).

    The caller must be a member of ``group``; Nova answers the cluster the group already owns,
    whatever its phase, and creates one otherwise (`size: small`, `runtime: spark-4.1-py3.13`,
    stops after 60 minutes without a job). A group whose name is not a valid cluster name
    (1 to 40 lower-case letters, digits and dashes) cannot create one this way: an
    administrator names that cluster with ``PUT /api/admin/compute/{name}``.
    """
    return _client(client).call("compute_ensure", body={"group": group})


def start(name, wait=True, timeout=300, interval=2, client=None):
    """Start `name`; with `wait`, poll until the phase is Running.

    Answers the last view Nova returned. Raises `TimeoutError` when the cluster is still
    Starting after `timeout` seconds (the cluster is not stopped by this call), and
    `ClusterStopped` if it answers Failed.
    """
    client = _client(client)
    view = client.call("compute_start", path={"name": name})
    if not wait:
        return view
    deadline = time.monotonic() + timeout
    while view.get("phase") != "Running":
        if view.get("phase") == "Failed":
            raise ClusterStopped(f"{name} failed to start: {view.get('message') or 'no reason given'}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{name} is still {view.get('phase')} after {timeout}s")
        time.sleep(interval)
        view = get(name, client)
    return view


def stop(name, client=None):
    """Stop `name`; answers the updated view. Idempotent."""
    return _client(client).call("compute_stop", path={"name": name})
