"""The agent in the terminal (stardelt.agent, `stardelt agent`). No network: a fake Nova and a real mcp.Server."""

import json

import pytest

from stardelt import agent, mcp
from tests.test_api import FakeResponse

CATALOG_SEARCH = {"results": [{"namespace": "shop", "table": "orders", "catalog": "silver"}]}


class FakeNova:
    """A Nova that answers canned chat-completions bodies and one catalog operation."""

    def __init__(self, *answers, endpoints=None, operation=None):
        self.answers = list(answers)
        self.bodies = []
        self.requests = []
        self.endpoints = list(endpoints or [])
        self.operation = CATALOG_SEARCH if operation is None else operation

    def _request(self, method, path, **kwargs):
        self.requests.append((method, path))
        self.bodies.append(kwargs.get("json"))
        return FakeResponse(payload=self.answers.pop(0))

    def call(self, operation_id, path=None, query=None, body=None):
        if operation_id == "model_endpoints_list":
            return self.endpoints
        return self.operation


def answer(content=None, tool_calls=None):
    message = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls
    finish = "tool_calls" if tool_calls else "stop"
    return {"choices": [{"index": 0, "message": message, "finish_reason": finish}]}


def tool_call(id_, name, arguments):
    return {"id": id_, "type": "function", "function": {"name": name, "arguments": json.dumps(arguments)}}


def test_openai_tools_converts_the_mcp_list():
    listed = mcp.Server(FakeNova()).handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})["result"]["tools"]
    converted = agent.openai_tools(listed)
    assert [t["function"]["name"] for t in converted] == [t["name"] for t in listed]
    assert all(t["type"] == "function" for t in converted)
    search = next(t for t in converted if t["function"]["name"] == "catalog_search")
    listed_search = next(t for t in listed if t["name"] == "catalog_search")
    assert search["function"]["parameters"] == listed_search["inputSchema"]
    assert search["function"]["description"] == listed_search["description"]


def test_pick_endpoint_takes_the_first_reachable_agents_endpoint():
    rows = [
        {"name": "ask-only", "purposes": ["ask"], "reachable": True},
        {"name": "unreachable", "purposes": ["ask", "agents"], "reachable": False},
        {"name": "not-probed", "purposes": ["ask", "agents"], "reachable": None},
        {"name": "first", "purposes": ["ask", "agents"], "reachable": True},
        {"name": "second", "purposes": ["agents"], "reachable": True},
    ]
    assert agent.pick_endpoint(rows) == "first"
    assert agent.pick_endpoint(rows, "second") == "second"


def test_pick_endpoint_without_one_raises_the_sentence():
    rows = [
        {"name": "ask-only", "purposes": ["ask"], "reachable": True},
        {"name": "dark", "purposes": ["ask", "agents"], "reachable": False},
    ]
    with pytest.raises(ValueError) as raised:
        agent.pick_endpoint(rows)
    assert str(raised.value) == agent.NO_ENDPOINT
    assert str(raised.value) == (
        "no model endpoint is offered to agents; an administrator adds the agents purpose to one"
    )
    with pytest.raises(ValueError):
        agent.pick_endpoint([])


def test_ask_runs_a_tool_and_returns_the_answer():
    nova = FakeNova(
        answer(tool_calls=[tool_call("call_1", "catalog_search", {"q": "orders"})]),
        answer(content="silver.shop.orders holds the orders."),
    )
    server = mcp.Server(nova)
    messages = [{"role": "user", "content": "where are the orders?"}]
    said = []

    text = agent.ask(nova, server, "local", messages, say=said.append)

    assert text == "silver.shop.orders holds the orders."
    assert nova.requests == [
        ("POST", "/api/models/local/v1/chat/completions"),
        ("POST", "/api/models/local/v1/chat/completions"),
    ]
    first = nova.bodies[0]
    assert first["messages"][0] == {"role": "system", "content": agent.SYSTEM_PROMPT}
    offered = server.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})["result"]["tools"]
    assert [t["function"]["name"] for t in first["tools"]] == [t["name"] for t in offered]
    tool_messages = [m for m in nova.bodies[1]["messages"] if m["role"] == "tool"]
    assert tool_messages == [
        {"role": "tool", "tool_call_id": "call_1", "content": json.dumps(CATALOG_SEARCH, ensure_ascii=False)}
    ]
    assert said == ['· catalog_search {"q": "orders"}']
    assert messages[-1] == {"role": "assistant", "content": "silver.shop.orders holds the orders."}


def test_ask_stops_at_the_tool_cap_and_asks_for_words():
    nova = FakeNova(
        answer(
            tool_calls=[
                tool_call("call_1", "catalog_search", {"q": "orders"}),
                tool_call("call_2", "catalog_search", {"q": "customers"}),
            ]
        ),
        answer(content="I could not finish looking; here is what I know."),
    )
    server = mcp.Server(nova)
    messages = [{"role": "user", "content": "everything about shop"}]
    said = []

    text = agent.ask(nova, server, "local", messages, max_tools=1, say=said.append)

    assert text == "I could not finish looking; here is what I know."
    assert len(nova.bodies) == 2
    last = nova.bodies[-1]
    assert "tools" not in last
    assert last["messages"][-1] == {"role": "user", "content": agent.ANSWER_NOW}
    assert said == []


def test_the_thread_is_kept_between_questions():
    nova = FakeNova(
        answer(content="silver.shop.orders is the orders table."),
        answer(content="it has 42 rows."),
    )
    server = mcp.Server(nova)
    messages = []

    messages.append({"role": "user", "content": "where are the orders?"})
    agent.ask(nova, server, "local", messages)
    messages.append({"role": "user", "content": "how many rows?"})
    agent.ask(nova, server, "local", messages)

    assert nova.bodies[1]["messages"][1:] == [
        {"role": "user", "content": "where are the orders?"},
        {"role": "assistant", "content": "silver.shop.orders is the orders table."},
        {"role": "user", "content": "how many rows?"},
    ]
