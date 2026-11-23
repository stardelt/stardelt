#!/usr/bin/env python3
"""The smallest OpenAI-compatible endpoint that Nova's ModelEndpoint probe and Ask accept.

The four calls Nova's probe makes (nova/backend/src/model_probe.rs) and the one Ask makes
(nova/backend/src/ask.rs). A check fixture, never a shipped component: verify-ask.sh starts it
in a pod, points a ModelEndpoint at it, and deletes both.

    GET  /v1/models              -> a context window (the probe's best effort)
    POST /v1/chat/completions    -> a tool call when the body has `tools`, the {"sql",
                                    "sentence"} object when it has `response_format`,
                                    a plain answer otherwise
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SQL = "SELECT 1 AS one"


def answer(body):
    if body.get("tools"):
        return {"choices": [{"message": {"role": "assistant", "content": None, "tool_calls": [
            {"id": "call_1", "type": "function", "function": {
                "name": "sql_explain", "arguments": json.dumps({"sql": SQL})}}]}}]}
    if body.get("response_format"):
        return {"choices": [{"message": {"role": "assistant", "content": json.dumps(
            {"sql": SQL, "sentence": "One constant column."})}}]}
    return {"choices": [{"message": {"role": "assistant", "content": "A lake is water."}}]}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, payload, status=200):
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path.rstrip("/") == "/v1/models":
            self._send({"object": "list", "data": [
                {"id": "stub", "object": "model", "meta": {"n_ctx": 8192}}]})
            return
        self._send({"error": "not found"}, 404)

    def do_POST(self):
        if self.path.rstrip("/") != "/v1/chat/completions":
            self._send({"error": "not found"}, 404)
            return
        length = int(self.headers.get("content-length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw)
        except ValueError:
            self._send({"error": "bad json"}, 400)
            return
        self._send(answer(body))

    def log_message(self, fmt, *args):
        sys.stderr.write("%s\n" % (fmt % args))


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
