"""verify-contract.sh compares the served Nova contract with nova/openapi/nova.json at the pin (#951).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is a shell shim first on PATH that prints one image, and `DOCUMENT`
replaces the fetch, so no cluster and no network are touched.

Why: the document at /api/openapi.json is what the CLI, the MCP server and the UI are generated
against, and nothing compared it with the committed one. verify-pins proves the digest is the pin;
an image built from another commit, or a pin rolled without a restart, answers a contract nobody
committed.
"""

import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "verify-contract.sh"
REPO = Path(
    subprocess.run(
        ["git", "-C", str(PLATFORM), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
)
PIN_FILE = "platform/manifests/nova-deployment.yaml"
CONTRACT_FILE = "nova/openapi/nova.json"

SHIM = """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "${FAKE_IMAGE:?}"
"""


def git_show(spec: str) -> str:
    return subprocess.run(
        ["git", "-C", str(REPO), "show", spec],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def pinned_image() -> str:
    manifest = git_show(f"HEAD:{PIN_FILE}")
    match = re.search(r"^\s*image:\s*(ghcr\.io/stardelt/nova:\S+@sha256:[0-9a-f]{64})\s*$", manifest, re.M)
    if not match:
        raise unittest.SkipTest(f"no pinned nova image in HEAD:{PIN_FILE}")
    return match.group(1)


def pinned_contract() -> str:
    digest = pinned_image().split("@", 1)[1]
    commit = subprocess.run(
        ["git", "-C", str(REPO), "log", "-1", "--format=%H", f"-S{digest}", "--", PIN_FILE],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if not commit:
        raise unittest.SkipTest(f"no commit pins {digest}")
    return git_show(f"{commit}:{CONTRACT_FILE}")


class VerifyContract(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)

    def run_script(self, *args, document=None, image=None):
        env = dict(os.environ, PATH=f"{self.dir}:{os.environ['PATH']}")
        env["FAKE_IMAGE"] = image if image is not None else pinned_image()
        env.pop("DOCUMENT", None)
        if document is not None:
            env["DOCUMENT"] = str(document)
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            cwd=PLATFORM,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

    def write(self, name: str, text: str) -> Path:
        path = self.dir / name
        path.write_text(text)
        return path

    def test_normalize_prints_one_sorted_line_per_operation_and_schema(self):
        document = {
            "paths": {
                "/b": {"get": {"operationId": "b_get"}, "parameters": []},
                "/a": {
                    "post": {"operationId": "a_post"},
                    "delete": {"operationId": "a_delete"},
                },
            },
            "components": {"schemas": {"Zeta": {}, "Alpha": {}}},
        }
        fixture = self.write("doc.json", json.dumps(document))
        done = self.run_script("--normalize", str(fixture))
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertEqual(
            done.stdout.splitlines(),
            ["DELETE /a a_delete", "GET /b b_get", "POST /a a_post", "schema Alpha", "schema Zeta"],
        )

    def test_a_document_that_matches_the_pinned_commit_passes(self):
        document = self.write("live.json", pinned_contract())
        done = self.run_script(document=document)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        digest = pinned_image().split("@", 1)[1]
        self.assertIn(f"✓ nova image {digest} pinned by ", done.stdout)
        self.assertRegex(
            done.stdout,
            r"✓ \d+ operations and \d+ schemas match nova/openapi/nova\.json at [0-9a-f]{7}\n"
            r"verify-contract: PASS\n$",
        )

    def test_one_renamed_operation_id_fails_with_two_changed_lines(self):
        live = json.loads(pinned_contract())
        for path, item in live["paths"].items():
            for method, operation in item.items():
                if isinstance(operation, dict) and operation.get("operationId"):
                    operation["operationId"] = "spike_renamed_operation"
                    break
            else:
                continue
            break
        document = self.write("stale.json", json.dumps(live))
        done = self.run_script(document=document)
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn(
            "verify-contract: FAIL (the served contract differs from "
            "nova/openapi/nova.json at ",
            done.stderr,
        )
        self.assertIn(": 2 operations or schemas)", done.stderr)
        self.assertIn("+ ", done.stderr)
        self.assertIn("- ", done.stderr)
        self.assertIn("spike_renamed_operation", done.stderr)

    def test_a_deployment_without_a_digest_fails(self):
        document = self.write("live.json", pinned_contract())
        done = self.run_script(document=document, image="ghcr.io/stardelt/nova:dev")
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn(
            "verify-contract: FAIL (the Nova deployment carries no digest: "
            "ghcr.io/stardelt/nova:dev)",
            done.stderr,
        )

    def test_a_digest_no_commit_pins_fails(self):
        document = self.write("live.json", pinned_contract())
        unknown = "ghcr.io/stardelt/nova@sha256:" + "0" * 64
        done = self.run_script(document=document, image=unknown)
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn(
            f"verify-contract: FAIL (no commit pins sha256:{'0' * 64} in {PIN_FILE})",
            done.stderr,
        )

    def test_a_served_document_that_is_not_json_fails(self):
        document = self.write("live.json", "<html>not the contract</html>")
        done = self.run_script(document=document)
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("verify-contract: FAIL (the served document is not JSON)", done.stderr)


if __name__ == "__main__":
    unittest.main()
