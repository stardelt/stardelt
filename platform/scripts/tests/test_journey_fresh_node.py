"""`trino_sql` in `journey-fresh-node.sh` treats a finished DDL/DML answer as success (#1874).

The body raised `no rows and no nextUri` for every answer that carried neither `data` nor
`nextUri` — the normal final answer of `create schema`, `create table`, `delete` and `insert`.
`phase_write` therefore reported `✗ write "create schema journey.e2e did not answer"` although
the write had succeeded, and J3 could never pass.

The body hardcodes `https://127.0.0.1:18443/v1/statement`, so the stub below listens there
with a generated self-signed certificate. Nothing else is needed: no Trino, no cluster.
`unittest` (not `pytest` functions) because CI discovers this folder with
`python3 -m unittest discover -s scripts/tests`; the file also runs under `pytest`.
"""

import json
import re
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "journey-fresh-node.sh"
PORT = 18443
STUB = {}


def trino_body():
    """The `trino_sql` Python heredoc — the only one that speaks `/v1/statement`."""
    bodies = re.findall(r"<<'PY'\n(.*?)\nPY\n", SCRIPT.read_text(), re.S)
    matching = [body for body in bodies if "v1/statement" in body]
    if len(matching) != 1:
        raise AssertionError("expected one /v1/statement heredoc, found %d" % len(matching))
    return matching[0]


def answer_for(sql):
    if sql.startswith("select"):
        return {"data": [["3"]]}
    if sql.startswith(("create", "insert", "delete")):
        return {"stats": {"state": "FINISHED"}}
    return {}


class StubHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        payload = json.dumps(answer_for(self.rfile.read(length).decode())).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


def setUpModule():
    if shutil.which("openssl") is None:
        raise unittest.SkipTest("openssl is not available to make the stub's certificate")
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.2).close()
    except OSError:
        pass
    else:
        raise unittest.SkipTest("127.0.0.1:%d is already bound; the body hardcodes the port" % PORT)

    work = tempfile.TemporaryDirectory(prefix="journey-fresh-node-")
    STUB["work"] = work
    folder = Path(work.name)
    key, cert = folder / "key.pem", folder / "cert.pem"
    made = subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-keyout", str(key), "-out", str(cert), "-subj", "/CN=127.0.0.1"],
        capture_output=True,
    )
    if made.returncode != 0:
        raise AssertionError("openssl req failed: %s" % made.stderr.decode())
    (folder / "body.py").write_text(trino_body())
    (folder / "token").write_text("stub-token")

    server = ThreadingHTTPServer(("127.0.0.1", PORT), StubHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    STUB["server"] = server
    STUB["thread"] = thread


def tearDownModule():
    if "server" in STUB:
        STUB["server"].shutdown()
        STUB["server"].server_close()
        STUB["thread"].join(timeout=10)
    if "work" in STUB:
        STUB["work"].cleanup()


class TrinoSql(unittest.TestCase):
    def run_body(self, sql):
        """Run the extracted heredoc body against the stub; no cluster, no Trino."""
        folder = Path(STUB["work"].name)
        return subprocess.run(
            [sys.executable, str(folder / "body.py"), str(folder / "token"), sql],
            capture_output=True, text=True, timeout=60,
        )

    def test_select_prints_the_first_column_of_the_first_row(self):
        done = self.run_body("select current_user")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(done.stdout, "3\n")

    def test_a_finished_ddl_answer_is_success(self):
        done = self.run_body("create schema journey.e2e")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(done.stdout, "")

    def test_an_answer_without_rows_and_without_finished_is_still_an_error(self):
        done = self.run_body("wait for something")
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("no rows and no nextUri", done.stderr)


if __name__ == "__main__":
    unittest.main()
