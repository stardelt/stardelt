"""Tests for stardelt.samples: the sample loader is one contract call (#1816)."""

import pytest

from stardelt import samples
from stardelt.api import Client
from tests.test_api import FakeResponse, FakeSession

ANSWER = {"created": True, "catalog": "playground", "namespace": "samples", "table": "trips", "rows": 10000}


def client(*responses):
    session = FakeSession(*responses)
    return Client(base_url="https://nova.example", token="t", session=session), session


def test_load_calls_the_public_operation_with_the_catalog():
    nova, session = client(FakeResponse(201, ANSWER))
    assert samples.load("trips", catalog="playground", client=nova) == ANSWER
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("POST", "https://nova.example/api/samples/trips")
    assert kwargs["params"] == {"catalog": "playground"}
    assert "json" not in kwargs, "the operation takes no body"


def test_load_without_a_catalog_sends_no_query():
    nova, session = client(FakeResponse(200, {**ANSWER, "created": False}))
    samples.load("trips", client=nova)
    assert session.calls[0][1] == "https://nova.example/api/samples/trips"
    assert "params" not in session.calls[0][2]


def test_an_unknown_sample_is_refused_before_any_request():
    nova, session = client()
    with pytest.raises(ValueError, match="no such sample: cities"):
        samples.load("cities", client=nova)
    assert session.calls == []
