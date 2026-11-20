"""Unit tests for scripts/verify-catalog-workload-auth.sh (stardelt#492, #562, #570).

The gap the tests close: `make verify-catalog-workload-auth` is a release gate whose whole
subject is invisible until a workload tries to sign in — two chart values in Secret
`lakekeeper-config-envs`, ClusterRoleBinding `stardelt:lakekeeper-token-review`, and the
audience split (the workload audience authenticates, the OIDC audience `lakekeeper` and the
API server's own audience stay 401). A gate that read the wrong status as PASS would turn a
missing `auth.k8s.audience` into "everything authenticates".

Standard library only, like the other tests here: `python3 -m unittest discover -s
scripts/tests -v` runs them in CI. `kubectl` and `curl` are shell shims first on `PATH` that
answer the Secret, the ClusterRoleBinding, the port-forward, the token and `whoami` from
environment variables, so no cluster is touched and no token exists.
"""
import os
import shutil
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "verify-catalog-workload-auth.sh"
BASH = shutil.which("bash") or "/bin/bash"

KUBECTL = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: the Secret carries FAKE_ENABLE/FAKE_AUDIENCE, the binding answers
# system:auth-delegator, the forward outlives the script, and a token is named after its
# audience so the curl shim can answer without seeing a real credential.
case " $* " in
  *" get secret lakekeeper-config-envs "*)
    if [ "${FAKE_NO_SECRET:-}" = 1 ]; then
      echo 'Error from server (NotFound): secrets "lakekeeper-config-envs" not found' >&2
      exit 1
    fi
    python3 - <<'PY'
import base64, json, os
data = {}
for key, var in (
    ("LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION", "FAKE_ENABLE"),
    ("LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE", "FAKE_AUDIENCE"),
):
    value = os.environ.get(var)
    if value is not None:
        data[key] = base64.b64encode(value.encode()).decode()
print(json.dumps({"data": data}))
PY
    ;;
  *" get clusterrolebinding "*)
    if [ "${FAKE_NO_CRB:-}" = 1 ]; then
      echo 'Error from server (NotFound): clusterrolebindings.rbac.authorization.k8s.io "stardelt:lakekeeper-token-review" not found' >&2
      exit 1
    fi
    printf '%s' "${FAKE_CRB_ROLE:-system:auth-delegator}"
    ;;
  *" port-forward "*)
    exec sleep 300
    ;;
  *" create token "*)
    audience=""
    for arg in "$@"; do
      case "$arg" in
        --audience=*) audience="${arg#--audience=}" ;;
      esac
    done
    case "$audience" in
      stardelt-catalog-workload) printf 'fake-workload-token' ;;
      lakekeeper) printf 'fake-oidc-token' ;;
      '') printf 'fake-apiserver-token' ;;
      *) printf 'fake-other-token' ;;
    esac
    ;;
  *) : ;;
esac
exit 0
"""

CURL = """#!/usr/bin/env bash
set -euo pipefail
# Fake curl: the `whoami` probe needs the token, which travels in a header file (`-H @file`).
out=/dev/null
header=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    -H) header="${2#@}"; shift 2 ;;
    -m|--max-time|-w|-D) shift 2 ;;
    *) shift ;;
  esac
done
code=401
body='{}'
if [ -n "$header" ] && [ -s "$header" ]; then
  token="$(sed -n 's/^Authorization: Bearer //p' "$header")"
  case "$token" in
    fake-workload-token)
      code="${FAKE_WORKLOAD_CODE:-404}"
      body="${FAKE_WORKLOAD_BODY:-{\\"error\\":{\\"code\\":\\"UserNotFound\\"}}}"
      ;;
    fake-oidc-token) code="${FAKE_OIDC_CODE:-401}" ;;
    fake-apiserver-token) code="${FAKE_APISERVER_CODE:-401}" ;;
  esac
fi
if [ "$out" != /dev/null ]; then
  printf '%s' "$body" > "$out"
fi
printf '%s' "$code"
"""


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class VerifyCatalogWorkloadAuth(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        for name, text in (("kubectl", KUBECTL), ("curl", CURL)):
            shim = self.dir / name
            shim.write_text(text)
            shim.chmod(0o755)
        self.port = free_port()

    def run_script(self, **extra):
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NS="stardelt",
            PORT=str(self.port),
            TMPDIR=str(self.dir),
        )
        env.update(extra)
        return subprocess.run(
            [BASH, str(SCRIPT)], env=env, capture_output=True, text=True
        )

    def test_all_green_passes(self):
        result = self.run_script(
            FAKE_ENABLE="true", FAKE_AUDIENCE="stardelt-catalog-workload"
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("verify-catalog-workload-auth: PASS", result.stdout)
        self.assertIn(
            "✓ the catalog enables Kubernetes authentication (audience stardelt-catalog-workload)",
            result.stdout,
        )
        self.assertIn("✓ ClusterRoleBinding stardelt:lakekeeper-token-review grants system:auth-delegator",
                      result.stdout)
        self.assertIn("✓ a workload-audience ServiceAccount token authenticates", result.stdout)
        self.assertIn("✓ the OIDC audience lakekeeper on a ServiceAccount token is refused (401)",
                      result.stdout)
        self.assertIn("✓ the API-server audience is refused (401)", result.stdout)
        self.assertNotIn("FAIL", result.stdout)

    def test_kubernetes_authentication_off_is_a_fail(self):
        """The invisible half of #562: `enabled: false` must fail, and the row says the value."""
        result = self.run_script(
            FAKE_ENABLE="false", FAKE_AUDIENCE="stardelt-catalog-workload"
        )
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION=false", result.stdout)
        self.assertNotIn("PASS", result.stdout)

    def test_a_missing_clusterrolebinding_is_a_fail(self):
        result = self.run_script(
            FAKE_ENABLE="true",
            FAKE_AUDIENCE="stardelt-catalog-workload",
            FAKE_NO_CRB="1",
        )
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("ClusterRoleBinding stardelt:lakekeeper-token-review is missing", result.stdout)

    def test_a_workload_token_answered_401_is_not_a_pass(self):
        """401 on the workload audience means the workload login is off, never a pass."""
        result = self.run_script(
            FAKE_ENABLE="true",
            FAKE_AUDIENCE="stardelt-catalog-workload",
            FAKE_WORKLOAD_CODE="401",
        )
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("401 means the workload login is off", result.stdout)
        self.assertNotIn("PASS", result.stdout)

    def test_no_kubectl_is_unverified(self):
        empty = self.dir / "empty"
        empty.mkdir()
        env = dict(os.environ, PATH=str(empty), NS="stardelt", PORT=str(self.port),
                   TMPDIR=str(self.dir), FAKE_ENABLE="true",
                   FAKE_AUDIENCE="stardelt-catalog-workload")
        result = subprocess.run(
            [BASH, str(SCRIPT)], env=env, capture_output=True, text=True
        )
        self.assertEqual(3, result.returncode, result.stdout + result.stderr)
        self.assertIn("UNVERIFIED (kubectl not found)", result.stdout)


if __name__ == "__main__":
    unittest.main()
