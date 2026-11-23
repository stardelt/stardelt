"""The notebook extension: `spark` and `display` are pushed into every kernel.

No kernel, no Spark server and no cluster: the IPython shell is a stub with a
``push`` method, and ``stardelt._session`` is replaced by a counter.
"""

import pandas as pd
import pytest
import requests

import stardelt
import stardelt.notebook as notebook
from test_stardelt import fake_spark  # the pyspark stub the spark tests share


class FakeShell:
    """The part of an IPython shell the extension uses: ``push``."""

    def __init__(self):
        self.pushed = []

    def push(self, namespace):
        self.pushed.append(namespace)


def test_load_pushes_spark_and_display():
    shell = FakeShell()
    notebook.load_ipython_extension(shell)
    assert len(shell.pushed) == 1
    assert set(shell.pushed[0]) == {"spark", "display"}
    assert isinstance(shell.pushed[0]["spark"], notebook._LazySpark)
    assert shell.pushed[0]["display"] is notebook.display


def test_lazy_spark_opens_one_session_on_first_use(monkeypatch):
    opened = []

    class StubSession:
        range = "the stub's range"

    def fake_session(cluster=None):
        opened.append(cluster)
        return StubSession()

    monkeypatch.setattr(stardelt, "_session", fake_session)
    # No group owns a cluster here: the kernel uses the shared server, as before #1390.
    monkeypatch.setattr(notebook, "_CLUSTER_RESOLVED", False)
    monkeypatch.setattr(notebook, "_CLUSTER_NAME", None)
    monkeypatch.delenv("STARDELT_CLUSTER", raising=False)
    # A URL is set, so the lookup really happens; "no cluster of mine" is LookupError.
    monkeypatch.setenv("STARDELT_NOVA_URL", "https://nova.example")
    monkeypatch.setattr(
        "stardelt.compute.default",
        lambda client=None: (_ for _ in ()).throw(
            LookupError("you have no Spark cluster of your own")
        ),
    )

    lazy = notebook._LazySpark()
    assert opened == []  # creating it opens nothing
    assert lazy.range == "the stub's range"
    assert len(opened) == 1


def _stub_session(monkeypatch, opened):
    class StubSession:
        range = "the stub's range"

    def fake_session(cluster=None):
        opened.append(cluster)
        return StubSession()

    monkeypatch.setattr(stardelt, "_session", fake_session)
    monkeypatch.setattr(notebook, "_CLUSTER_RESOLVED", False)
    monkeypatch.setattr(notebook, "_CLUSTER_NAME", None)
    monkeypatch.delenv("STARDELT_CLUSTER", raising=False)


def test_lazy_spark_does_not_ask_nova_without_a_url(monkeypatch, capsys):
    """A kernel with no Nova URL opens the shared server without a round trip (#1398)."""
    opened = []
    _stub_session(monkeypatch, opened)
    monkeypatch.delenv("STARDELT_NOVA_URL", raising=False)
    monkeypatch.delenv("STARDELT_NOVA_API_URL", raising=False)

    def no_lookup(client=None):
        raise AssertionError("compute.default() was called with no Nova URL set")

    monkeypatch.setattr("stardelt.compute.default", no_lookup)

    assert notebook._LazySpark().range == "the stub's range"
    assert opened == [None]
    assert capsys.readouterr().err == ""


def test_lazy_spark_falls_back_when_nova_does_not_answer(monkeypatch, capsys):
    """Nova unreachable is "no cluster of mine", not a broken `spark` (#1398)."""
    opened = []
    _stub_session(monkeypatch, opened)
    monkeypatch.setenv("STARDELT_NOVA_URL", "https://nova.example")
    monkeypatch.setattr(
        "stardelt.compute.default",
        lambda client=None: (_ for _ in ()).throw(
            requests.ConnectionError("nova.example refused the connection")
        ),
    )

    assert notebook._LazySpark().range == "the stub's range"
    assert opened == [None]
    assert capsys.readouterr().err == ""


def test_lazy_spark_names_itself():
    assert repr(notebook._LazySpark()) == (
        "<spark: Spark Connect session as you, opened on first use>"
    )


def test_display_limits_a_spark_dataframe(monkeypatch):
    shown = []

    import IPython.display as ipython_display

    monkeypatch.setattr(ipython_display, "display", lambda obj: shown.append(obj) or obj)

    class Frame:
        def __init__(self):
            self.limits = []

        def limit(self, rows):
            self.limits.append(rows)
            return self

        def toPandas(self):
            return "the pandas frame"

    frame = Frame()
    notebook.display(frame)
    assert frame.limits == [1000]
    assert shown == ["the pandas frame"]


def test_display_passes_anything_else_through(monkeypatch):
    shown = []

    import IPython.display as ipython_display

    monkeypatch.setattr(ipython_display, "display", lambda obj: shown.append(obj) or obj)

    notebook.display("plain text")
    assert shown == ["plain text"]


def test_write_df_is_deprecated(monkeypatch):
    class StubCursor:
        def execute(self, statement, params=None):
            pass

        def close(self):
            pass

    class StubConnection:
        def cursor(self):
            return StubCursor()

        def close(self):
            pass

    monkeypatch.setenv("TRINO_URL", "https://trino.example:8443")
    monkeypatch.setattr(stardelt, "_connect", lambda **kwargs: StubConnection())

    frame = pd.DataFrame({"id": [1]})
    with pytest.warns(FutureWarning, match="stardelt.write_df"):
        assert stardelt.write_df(frame, "demo.t") == 1


def test_spark_is_deprecated(fake_spark, monkeypatch):
    monkeypatch.setattr(stardelt, "sql", lambda _query: pd.DataFrame({"Catalog": ["warehouse"]}))
    monkeypatch.setattr(stardelt, "_token", lambda: "canary-token-317")

    with pytest.warns(FutureWarning, match="stardelt.spark"):
        session = stardelt.spark()
    assert session.configs["spark.sql.repl.eagerEval.enabled"] == "true"


def test_lazy_spark_opens_the_group_cluster_when_the_environment_names_none(monkeypatch):
    import stardelt.compute as compute

    opened = []
    monkeypatch.setattr(notebook, "_CLUSTER_RESOLVED", False, raising=False)
    monkeypatch.setattr(notebook, "_CLUSTER_NAME", None, raising=False)
    monkeypatch.delenv("STARDELT_CLUSTER", raising=False)
    # A URL is set, so the lookup happens: this test is about the answer, not about #1398.
    monkeypatch.setenv("STARDELT_NOVA_URL", "https://nova.example")
    monkeypatch.setattr(compute, "default", lambda client=None: {"name": "team"})
    monkeypatch.setattr(stardelt, "_session", lambda cluster=None: opened.append(cluster))

    notebook._LazySpark()._get()

    assert opened == ["team"]


def test_lazy_spark_keeps_the_shared_server_and_prints_the_notice(monkeypatch, capsys):
    import stardelt.compute as compute

    opened = []

    def several(client=None):
        raise LookupError("you may act on 2 Spark clusters (a-team, b-team): name one with "
                          "STARDELT_CLUSTER, or pass its name to stardelt.compute.get/start")

    monkeypatch.setattr(notebook, "_CLUSTER_RESOLVED", False, raising=False)
    monkeypatch.setattr(notebook, "_CLUSTER_NAME", None, raising=False)
    monkeypatch.delenv("STARDELT_CLUSTER", raising=False)
    # A URL is set, so the lookup happens: this test is about the answer, not about #1398.
    monkeypatch.setenv("STARDELT_NOVA_URL", "https://nova.example")
    monkeypatch.setattr(compute, "default", several)
    monkeypatch.setattr(stardelt, "_session", lambda cluster=None: opened.append(cluster))

    notebook._LazySpark()._get()

    assert opened == [None]
    assert capsys.readouterr().err.startswith("stardelt.spark: you may act on 2 Spark clusters")


def test_lazy_spark_is_silent_when_no_group_owns_a_cluster(monkeypatch, capsys):
    import stardelt.compute as compute

    def none_of_my_own(client=None):
        raise LookupError("you have no Spark cluster of your own: no group you are in owns one — "
                          "use the shared Spark server (or %sql), or ask an administrator for a cluster")

    monkeypatch.setattr(notebook, "_CLUSTER_RESOLVED", False, raising=False)
    monkeypatch.setattr(notebook, "_CLUSTER_NAME", None, raising=False)
    monkeypatch.delenv("STARDELT_CLUSTER", raising=False)
    # A URL is set, so the lookup happens: this test is about the answer, not about #1398.
    monkeypatch.setenv("STARDELT_NOVA_URL", "https://nova.example")
    monkeypatch.setattr(compute, "default", none_of_my_own)
    monkeypatch.setattr(stardelt, "_session", lambda cluster=None: None)

    notebook._LazySpark()._get()

    assert capsys.readouterr().err == ""
