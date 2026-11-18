"""Every kernel gets `spark` and `display`, the native names (maintainer 2026-11-17: no middleware).
Load with `%load_ext stardelt.notebook`; the platform's notebook image loads it for every kernel."""
import os


class _LazySpark:
    """`spark`, opened on first use, rebuilt when the sign-in token is renewed."""

    def _get(self):
        import stardelt

        return stardelt._session(cluster=os.environ.get("STARDELT_CLUSTER") or None)

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
