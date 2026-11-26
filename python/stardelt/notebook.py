"""Every kernel gets `spark` and `display`, the native names (maintainer 2026-11-17: no middleware).
Load with `%load_ext stardelt.notebook`; the platform's notebook image loads it for every kernel."""
import os
import sys

import requests

# Resolved once per kernel: a cluster name does not change under a notebook, and every
# attribute access on `spark` would otherwise cost one Nova call.
_CLUSTER_RESOLVED = False
_CLUSTER_NAME = None


def _nova_configured():
    """Whether a Nova URL is set at all — the only case in which asking it can answer anything."""
    return bool(os.environ.get("STARDELT_NOVA_API_URL") or os.environ.get("STARDELT_NOVA_URL"))


def _cluster_name():
    """`STARDELT_CLUSTER`, else the one Spark cluster the person's group owns (#493 point 4).

    With neither there is no cluster: the session refuses and says how to get one, and never
    starts one (a person presses Start). A person in several groups is told which clusters
    there are (once) and names one with `STARDELT_CLUSTER` rather than a guess. A kernel with
    no Nova URL, or one whose Nova does not answer, is "no cluster of mine" rather than an
    error (#1398).
    """
    global _CLUSTER_RESOLVED, _CLUSTER_NAME
    if _CLUSTER_RESOLVED:
        return _CLUSTER_NAME
    _CLUSTER_RESOLVED = True
    name = os.environ.get("STARDELT_CLUSTER") or None
    if name:
        _CLUSTER_NAME = name
        return name
    if not _nova_configured():
        _CLUSTER_NAME = None
        return None
    from stardelt import compute

    try:
        _CLUSTER_NAME = compute.default()["name"]
    except LookupError as err:
        text = str(err)
        if text.startswith("you may act on"):
            print(f"stardelt.spark: {text}", file=sys.stderr)
        print(
            "stardelt.spark: create your group's cluster with "
            "stardelt.compute.ensure('<your group>') (or in Nova → Compute)",
            file=sys.stderr,
        )
        _CLUSTER_NAME = None
    except requests.RequestException:
        _CLUSTER_NAME = None
    return _CLUSTER_NAME


class _LazySpark:
    """`spark`, opened on first use, rebuilt when the sign-in token is renewed."""

    def _get(self):
        import stardelt

        return stardelt._session(cluster=_cluster_name())

    def __getattr__(self, name):
        return getattr(self._get(), name)

    def __repr__(self):
        return "<spark: Spark Connect session as you, opened on first use>"


def display(obj, limit=1000):
    """Show a Spark DataFrame as a table (the first `limit` rows); anything else as IPython would."""
    from IPython.display import display as _display

    if hasattr(obj, "toPandas") and hasattr(obj, "limit"):
        return _display(obj.limit(limit).toPandas())
    return _display(obj)


def load_ipython_extension(ip):
    ip.push({"spark": _LazySpark(), "display": display})
