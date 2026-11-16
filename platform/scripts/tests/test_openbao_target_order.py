"""`make openbao` waits for the pod to be Ready before it port-forwards (platform#1613).

The upgrade replaces the pod; `openbao-bootstrap.sh` polls the API itself, but
`openbao-connections.sh` port-forwards into the pod and died with `connection refused` on a fresh
upgrade. The recipe must therefore run a Ready wait between the two, and this test pins the order
by reading `make -n openbao` (no cluster, no helm).
"""

import os
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


class OpenbaoTargetOrderTest(unittest.TestCase):
    def setUp(self):
        done = subprocess.run(
            ["make", "-n", "openbao"],
            cwd=REPO,
            capture_output=True,
            text=True,
            env={**os.environ, "NAMESPACE": "stardelt"},
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.lines = done.stdout.splitlines()

    def index(self, needle, after=0):
        for number, line in enumerate(self.lines):
            if number >= after and needle in line:
                return number
        self.fail(f"`make -n openbao` does not print {needle!r}")

    def test_the_connections_step_port_forwards_only_into_a_ready_pod(self):
        bootstrap = self.index("scripts/openbao-bootstrap.sh")
        ready = self.index("wait --for=condition=Ready", after=bootstrap)
        self.assertIn("pod/openbao-0", self.lines[ready])
        connections = self.index("scripts/openbao-connections.sh", after=bootstrap)
        self.assertLess(bootstrap, ready)
        self.assertLess(ready, connections)


if __name__ == "__main__":
    unittest.main()
