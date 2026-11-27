import os

import pytest

import stardelt
import stardelt.mlflow_tracking as tracking
from stardelt.connections import ConnectionUnavailable


class FakeConnection:
    def __init__(self, kind="mlflow", settings=None, keys=("token",), token="tok-123"):
        self.type = kind
        self.settings = {"trackingUrl": "https://mlflow.example.com"} if settings is None else settings
        self.keys = list(keys)
        self._token = token

    def value(self, key):
        assert key == "token"
        return self._token


def use(monkeypatch, conn):
    monkeypatch.setattr(tracking, "connection", lambda name, scope=None, group=None: conn)


def clear(monkeypatch):
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    monkeypatch.delenv("MLFLOW_TRACKING_TOKEN", raising=False)


def test_sets_the_uri_and_returns_it(monkeypatch):
    clear(monkeypatch)
    use(monkeypatch, FakeConnection())
    assert tracking.mlflow() == "https://mlflow.example.com"
    assert os.environ["MLFLOW_TRACKING_URI"] == "https://mlflow.example.com"


def test_sets_the_token_when_the_connection_has_one(monkeypatch):
    clear(monkeypatch)
    use(monkeypatch, FakeConnection())
    tracking.mlflow()
    assert os.environ["MLFLOW_TRACKING_TOKEN"] == "tok-123"


def test_clears_a_stale_token_when_the_connection_has_none(monkeypatch):
    clear(monkeypatch)
    monkeypatch.setenv("MLFLOW_TRACKING_TOKEN", "stale-token")
    use(monkeypatch, FakeConnection(keys=()))
    tracking.mlflow()
    assert "MLFLOW_TRACKING_TOKEN" not in os.environ


def test_refuses_another_type(monkeypatch):
    clear(monkeypatch)
    use(monkeypatch, FakeConnection(kind="s3"))
    with pytest.raises(ConnectionUnavailable) as excinfo:
        tracking.mlflow()
    assert "not an MLflow tracking server" in str(excinfo.value)


def test_refuses_a_connection_without_tracking_url(monkeypatch):
    clear(monkeypatch)
    use(monkeypatch, FakeConnection(settings={}))
    with pytest.raises(ConnectionUnavailable) as excinfo:
        tracking.mlflow()
    assert "has no trackingUrl setting" in str(excinfo.value)


def test_the_package_exports_it():
    assert callable(stardelt.mlflow)
