"""The Airflow checks refuse a busy local port (platform#1726).

`kubectl port-forward` prints `address already in use` and dies while anything else holds
the port. Both scripts used to discard that message, wait for the port to answer and then
sign in to whatever held it: on 2026-11-17 a stale Nova forward on 18081 answered
`405 Method Not Allowed` and `verify-airflow-runs` called a healthy Airflow broken.

Neither test needs a cluster: the guard runs before the first `kubectl` call.
"""

import os
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

MISSING = [tool for tool in ("bash", "kubectl", "curl", "python3")
           if subprocess.run(["sh", "-c", "command -v " + tool],
                             capture_output=True).returncode != 0]


def start_listener(port):
    """`python3 -m http.server <port> --bind 127.0.0.1`, ready to answer."""
    server = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(port), "--bind", "127.0.0.1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    for _ in range(100):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return server
        except OSError:
            time.sleep(0.05)
    server.terminate()
    server.wait(timeout=10)
    raise AssertionError("the test's http.server never listened on %d" % port)


class AirflowPortGuard(unittest.TestCase):
    def setUp(self):
        if MISSING:
            self.skipTest("a printed line: missing tool(s) " + " ".join(MISSING))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]

    def assert_refused(self, script):
        server = start_listener(self.port)
        try:
            done = subprocess.run(
                ["bash", script],
                cwd=REPO,
                env={**os.environ, "NS": "stardelt", "PORT": str(self.port)},
                capture_output=True, text=True, timeout=120,
            )
        finally:
            server.terminate()
            server.wait(timeout=10)

        output = done.stdout + done.stderr
        self.assertEqual(done.returncode, 3, output)
        self.assertIn("already in use", output)
        self.assertIn("port %d is already in use" % self.port, output)

    def test_verify_airflow_runs_refuses_a_busy_port(self):
        self.assert_refused("scripts/verify-airflow-runs.sh")

    def test_verify_airflow_actions_refuses_a_busy_port(self):
        self.assert_refused("scripts/verify-airflow-actions.sh")


if __name__ == "__main__":
    unittest.main()
