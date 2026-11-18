"""The notebook extension: `spark` and `display` are pushed into every kernel.

No kernel, no Spark server and no cluster: the IPython shell is a stub with a
``push`` method, and ``stardelt._session`` is replaced by a counter.
"""

import pandas as pd
import pytest

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

    lazy = notebook._LazySpark()
    assert opened == []  # creating it opens nothing
    assert lazy.range == "the stub's range"
    assert len(opened) == 1


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
