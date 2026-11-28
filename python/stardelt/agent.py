"""``stardelt agent``: an agent in the terminal (agent harness slice 3, stardelt/stardelt#2381).

The agent runs on the person's own device and holds no key of its own:

- its tools are the package's own read-only MCP tools, in-process (``stardelt.mcp.Server``), so a call
  runs as the signed-in person through the same public API;
- its model is a ``ModelEndpoint`` of the platform, reached through Nova's OpenAI-compatible proxy
  ``/api/models/{endpoint}/v1/chat/completions`` with the person's token, so the endpoint's key stays in
  the cluster;
- its identity is the sign-in ``stardelt login`` already stored.

The loop is bounded: a question costs at most ``max_steps`` model calls and ``max_tools`` tool calls, then
the model is asked once more without tools to answer in words.
"""

import json

SYSTEM_PROMPT = (
    "You answer questions about the data the person may read, using the tools. Use the catalog and "
    "documentation tools to find tables, sql_read to look at data (read-only), and answer in words, "
    "naming the tables and numbers you used. Never invent a table, a column or a number."
)
NO_ENDPOINT = "no model endpoint is offered to agents; an administrator adds the agents purpose to one"
SHOWN_ARGUMENTS = 120
MAX_STEPS = 8
MAX_TOOLS = 6
ANSWER_NOW = "Answer now with what you have, in words."


def openai_tools(listed):
    """MCP `tools/list` entries -> OpenAI function tools (D2)."""
    return [
        {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool.get("description") or "",
                "parameters": tool.get("inputSchema") or {"type": "object", "properties": {}},
            },
        }
        for tool in listed
    ]


def pick_endpoint(rows, wanted=None):
    """The endpoint name to use (D1), or raise ValueError with the D1 sentence."""
    if wanted:
        return wanted
    for row in rows or []:
        if "agents" in (row.get("purposes") or []) and row.get("reachable") is True:
            return row.get("name")
    raise ValueError(NO_ENDPOINT)


def ask(nova, server, endpoint, messages, *, max_steps=8, max_tools=6, say=lambda line: None):
    """Run the loop for the last user message in `messages`; return the answer text.

    `messages` is extended in place, so a REPL keeps the thread.
    """
    listed = (server.handle({"jsonrpc": "2.0", "id": 0, "method": "tools/list"}) or {}).get("result") or {}
    tools = openai_tools(listed.get("tools") or [])
    path = f"/api/models/{endpoint}/v1/chat/completions"
    used_tools = 0
    for _ in range(max(0, max_steps)):
        body = {"messages": [{"role": "system", "content": SYSTEM_PROMPT}] + messages}
        if tools:
            body["tools"] = tools
        message = _message_of(nova._request("POST", path, json=body).json())
        calls = message.get("tool_calls") or []
        if not calls:
            text = message.get("content") or ""
            messages.append({"role": "assistant", "content": text})
            return text
        if used_tools + len(calls) > max_tools:
            break
        messages.append(message)
        for index, call in enumerate(calls):
            name = (call.get("function") or {}).get("name") or ""
            arguments = _arguments_of(call)
            say(f"· {name} {_cut(json.dumps(arguments, ensure_ascii=False), SHOWN_ARGUMENTS)}")
            reply = server.handle(
                {
                    "jsonrpc": "2.0",
                    "id": index + 1,
                    "method": "tools/call",
                    "params": {"name": name, "arguments": arguments},
                }
            )
            result = (reply or {}).get("result") or {}
            text = "".join(
                part.get("text") or "" for part in result.get("content") or [] if isinstance(part, dict)
            )
            messages.append({"role": "tool", "tool_call_id": call.get("id"), "content": text or "(no content)"})
            used_tools += 1
    messages.append({"role": "user", "content": ANSWER_NOW})
    body = {"messages": [{"role": "system", "content": SYSTEM_PROMPT}] + messages}
    message = _message_of(nova._request("POST", path, json=body).json())
    text = message.get("content") or ""
    messages.append({"role": "assistant", "content": text})
    return text


def _message_of(answer):
    """The first choice's message of an OpenAI chat-completions answer."""
    choices = answer.get("choices") if isinstance(answer, dict) else None
    if not choices:
        return {"content": ""}
    return choices[0].get("message") or {"content": ""}


def _arguments_of(call):
    """A tool call's arguments as an object: the proxy passes the provider's JSON text through."""
    function = call.get("function") or {}
    arguments = function.get("arguments")
    if isinstance(arguments, dict):
        return arguments
    if not arguments:
        return {}
    try:
        parsed = json.loads(arguments)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _cut(text, limit):
    return text if len(text) <= limit else text[: limit - 1] + "…"
