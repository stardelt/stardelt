"""Every kernel gets `spark` and `display`, the native names (maintainer 2026-11-17: no middleware).
Load with `%load_ext stardelt.notebook`; the platform's notebook image loads it for every kernel."""
import os
import sys

# Resolved once per kernel: a cluster name does not change under a notebook, and every
# attribute access on `spark` would otherwise cost one Nova call.
_CLUSTER_RESOLVED = False
_CLUSTER_NAME = None


def _cluster_name():
    """`STARDELT_CLUSTER`, else the one Spark cluster the person's group owns (#493 point 4).

    With neither, the session falls back to the shared server — the behaviour of every kernel
    until 0.0.5. A person in several groups is told which clusters there are (once) and gets the
    shared server rather than a guess; `None` is never a silent choice.
    """
    global _CLUSTER_RESOLVED, _CLUSTER_NAME
    if _CLUSTER_RESOLVED:
        return _CLUSTER_NAME
    _CLUSTER_RESOLVED = True
    name = os.environ.get("STARDELT_CLUSTER") or None
    if name:
        _CLUSTER_NAME = name
        return name
    from stardelt import compute

    try:
        _CLUSTER_NAME = compute.default()["name"]
    except LookupError as err:
        text = str(err)
        if text.startswith("you may act on"):
            print(f"stardelt.spark: {text}", file=sys.stderr)
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
