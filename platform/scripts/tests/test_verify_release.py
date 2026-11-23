"""The verify-release recipe table and the Makefile stay in step (stardelt/stardelt#540)."""

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "verify-release.sh"
MAKEFILE = PLATFORM / "Makefile"

ENTRY = re.compile(r"^\s*\[([a-z0-9-]+)\]=(?:'([^']*)'|\"([^\"]*)\")$")
ASSIGN = re.compile(r"^(?:[A-Z_]+=\$\([A-Z_]+\)\s+)+")
TARGET = re.compile(r"^([A-Za-z0-9_.-]+):")


def normalize(command: str) -> str:
    command = command.lstrip("@").strip()
    command = ASSIGN.sub("", command)
    return " ".join(command.split())


def table() -> dict:
    out = {}
    for line in SCRIPT.read_text().splitlines():
        match = ENTRY.match(line)
        if match:
            out[match.group(1)] = match.group(2) if match.group(2) is not None else match.group(3)
    return out


def default_targets() -> list:
    match = re.search(r'^default_targets="([^"]*)"', SCRIPT.read_text(), re.M | re.S)
    assert match, "verify-release.sh has no default_targets"
    return match.group(1).split()


def recipes() -> dict:
    """target -> its Makefile recipe's command, continuations joined and `@`/assignments stripped."""
    out, target, lines = {}, None, []
    for line in MAKEFILE.read_text().splitlines():
        head = TARGET.match(line)
        if head and not line.startswith("\t"):
            if target and lines:
                out[target] = normalize(" ".join(lines))
            target, lines = head.group(1), []
        elif target is not None and line.startswith("\t"):
            lines.append(line[1:].rstrip().removesuffix("\\"))
        elif lines:
            out[target] = normalize(" ".join(lines))
            target, lines = None, []
    if target and lines:
        out[target] = normalize(" ".join(lines))
    return out


class VerifyReleaseTest(unittest.TestCase):
    def test_the_commit_is_read_before_the_checks_run(self):
        """The summary names the tree the checks ran on, not HEAD at the end (stardelt/stardelt#1385)."""
        text = SCRIPT.read_text()
        self.assertIn('COMMIT="$(git rev-parse --short HEAD)"', text)
        summary = [line for line in text.splitlines() if line.startswith("echo \"commit ")]
        self.assertEqual(len(summary), 1, "verify-release.sh has no single summary line")
        self.assertIn("commit $COMMIT", summary[0])
        self.assertNotIn("$(git rev-parse", summary[0])
        self.assertEqual(text.count("the checkout moved to"), 1)

    def test_every_default_target_has_a_recipe_in_the_table(self):
        self.assertEqual(list(table()), default_targets())

    def test_the_table_matches_the_makefile(self):
        make = recipes()
        for target, command in table().items():
            self.assertIn(target, make, f"{target} is not a Makefile target")
            self.assertEqual(command, make[target], f"{target}: the table and the Makefile differ")

    def test_the_three_outcomes_print_three_shapes_and_set_the_exit_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name, code in (("ok", 0), ("no", 1), ("un", 3)):
                Path(tmp, f"{name}.sh").write_text(f"echo {name}\nexit {code}\n")
            bindir = Path(tmp, "bin")
            bindir.mkdir()
            for name in ("make", "kubectl"):
                tool = bindir / name
                tool.write_text("#!/bin/sh\necho ok\nexit 0\n")
                tool.chmod(0o755)
            environment = dict(
                os.environ,
                PATH=f"{bindir}:{os.environ['PATH']}",
                VERIFY_RELEASE_TARGETS="ok no un",
                VERIFY_RELEASE_RECIPES="\n".join(
                    f"{name}=bash {tmp}/{name}.sh" for name in ("ok", "no", "un")
                ),
            )
            run = subprocess.run(
                ["bash", str(SCRIPT)], capture_output=True, text=True, env=environment
            )
        self.assertEqual(run.returncode, 1, run.stdout + run.stderr)   # a ✗ outranks a ?
        self.assertIn("✓ ok", run.stdout)
        self.assertIn("✗ no", run.stdout)
        self.assertIn("? un", run.stdout)
        self.assertIn("1/3 passed · 1 unverified", run.stdout)

    def test_an_unreachable_api_server_is_unverified_not_a_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            bindir = Path(tmp, "bin")
            bindir.mkdir()
            (bindir / "make").write_text("#!/bin/sh\nexit 0\n")
            (bindir / "kubectl").write_text("#!/bin/sh\nexit 1\n")   # /readyz fails
            for tool in bindir.iterdir():
                tool.chmod(0o755)
            run = subprocess.run(
                ["bash", str(SCRIPT)],
                capture_output=True, text=True,
                env=dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}",
                         VERIFY_RELEASE_TARGETS="verify-pins"),
            )
        self.assertEqual(run.returncode, 3, run.stdout + run.stderr)
        self.assertIn("UNVERIFIED (the API server does not answer /readyz", run.stdout)

    def test_secret_reaches_only_verify_images(self):
        """SECRET means the pull credential only for verify-images; the other checks keep their own."""
        with tempfile.TemporaryDirectory() as tmp:
            bindir = Path(tmp, "bin")
            bindir.mkdir()
            for name in ("make", "kubectl"):
                tool = bindir / name
                tool.write_text("#!/bin/sh\nexit 0\n")
                tool.chmod(0o755)
            env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}",
                       VERIFY_RELEASE_TARGETS="verify-images verify-airflow-runs",
                       VERIFY_RELEASE_RECIPES=f"verify-images=echo \"$SECRET\" > {tmp}/images\n"
                                              f"verify-airflow-runs=echo \"$SECRET\" > {tmp}/airflow")
            env.pop("SECRET", None)
            env.pop("GHCR_SECRET", None)
            run = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
            images = Path(tmp, "images").read_text().strip()
            airflow = Path(tmp, "airflow").read_text().strip()
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        self.assertEqual(images, "ghcr-creds")
        self.assertEqual(airflow, "", "verify-airflow-runs must see its own default, not ghcr-creds")


if __name__ == "__main__":
    unittest.main()
