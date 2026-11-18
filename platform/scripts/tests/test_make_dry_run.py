"""`make -n install` is a dry run: no recipe line mixes `$(MAKE)` with a write (platform#1719).

GNU make executes a whole **logical** recipe line under `-n` when that line contains
`$(MAKE)`, and a logical line is everything joined by backslash continuations. So a
`step-*` block that called a sub-make and then `helm upgrade` ran the upgrade for real:
measured on the lab 2026-11-17, `make -n install` bumped `helm status trino` REVISION
112 -> 113 and `step-pins` rewrote `helm-values/` and rolled Deployments.

The fix splits every such block so that a line carrying `$(MAKE)` carries nothing else,
and repeats the guard on each split line. `test_no_recipe_block_mixes_make_with_a_write`
is the guard that survives refactors; `test_make_n_install_runs_no_write_command`
reproduces the reported bug end to end with `helm`/`kubectl` shims on `PATH`.
"""

import os
import pathlib
import re
import stat
import subprocess
import tempfile
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
MAKEFILE = REPO_ROOT / "Makefile"

# A write is anything that changes the cluster or the working tree. Kept as literal
# substrings: the assertion message names the offending Makefile line.
WRITES = (
    "helm ",
    "kubectl apply",
    "kubectl create",
    "kubectl delete",
    "kubectl patch",
    "kubectl rollout",
    "kubectl label",
    "kubectl annotate",
    "kubectl scale",
    "kubectl replace",
    "kubectl exec",
    "bash scripts/",
)

# The kubectl verbs that only read; anything else in a shim log is a write.
READ_VERBS = ("get", "version", "config", "api-resources", "explain", "cluster-info")

_SUBMAKE = re.compile(r"\$\(MAKE\)\s+--no-print-directory\s+\S+")

# kubectl options whose value is the next argument, so the verb is not mistaken for it.
_OPTIONS_WITH_VALUE = (
    "-n",
    "--namespace",
    "-o",
    "--output",
    "--context",
    "--kubeconfig",
    "--cluster",
    "--user",
    "--server",
)


def logical_lines(text):
    """Yield (first line number, joined text) for every recipe line, continuations joined."""
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        start = index + 1
        joined = lines[index]
        while joined.endswith("\\") and index + 1 < len(lines):
            joined = joined[:-1] + " " + lines[index + 1]
            index += 1
        yield start, joined
        index += 1


def kubectl_verb(arguments):
    """The subcommand of a `kubectl ...` argument string, skipping flags and their values."""
    tokens = arguments.split()
    for position, token in enumerate(tokens):
        if token in _OPTIONS_WITH_VALUE:
            continue
        if token.startswith("-"):
            continue
        previous = tokens[position - 1] if position else ""
        if previous in _OPTIONS_WITH_VALUE:
            continue
        return token
    return ""


class DryRunTest(unittest.TestCase):
    def test_no_recipe_block_mixes_make_with_a_write(self):
        text = MAKEFILE.read_text(encoding="utf-8")
        offenders = []
        for number, line in logical_lines(text):
            if "$(MAKE)" not in line:
                continue
            remainder = _SUBMAKE.sub("", line)
            for write in WRITES:
                if write in remainder:
                    offenders.append((number, write, line.strip()))
        self.assertEqual(
            offenders,
            [],
            "Makefile recipe lines run for real under `make -n` (a logical line with "
            "$(MAKE) executes as a whole). Split the line so it carries either $(MAKE) "
            "or a write, never both:\n"
            + "\n".join(
                f"  Makefile:{number}: {write!r} beside $(MAKE)\n    {line}"
                for number, write, line in offenders
            ),
        )

    def test_make_n_install_runs_no_write_command(self):
        with tempfile.TemporaryDirectory() as scratch:
            bin_dir = pathlib.Path(scratch) / "bin"
            bin_dir.mkdir()
            log = pathlib.Path(scratch) / "commands.log"
            log.touch()
            for tool in ("helm", "kubectl"):
                shim = bin_dir / tool
                shim.write_text(
                    "#!/bin/sh\n"
                    f'printf "%s %s\\n" "$(basename "$0")" "$*" >> {log}\n'
                    "exit 0\n",
                    encoding="utf-8",
                )
                shim.chmod(shim.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

            environment = dict(os.environ)
            environment["PATH"] = f"{bin_dir}:{environment.get('PATH', '')}"
            environment["NAMESPACE"] = "stardelt"
            environment.setdefault("BASE_DOMAIN", "alpha.dev.stardelt.io")

            result = subprocess.run(
                ["make", "-n", "install"],
                cwd=REPO_ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=600,
            )
            self.assertEqual(
                result.returncode,
                0,
                f"`make -n install` exited {result.returncode}\nstdout tail:\n"
                + "\n".join(result.stdout.splitlines()[-20:])
                + "\nstderr tail:\n"
                + "\n".join(result.stderr.splitlines()[-20:]),
            )

            lines = [line for line in log.read_text(encoding="utf-8").splitlines() if line]
            writes = []
            for line in lines:
                tool, _, arguments = line.partition(" ")
                if tool == "helm":
                    writes.append(line)
                elif tool == "kubectl" and kubectl_verb(arguments) not in READ_VERBS:
                    writes.append(line)
            self.assertEqual(
                writes,
                [],
                "`make -n install` executed write commands through helm/kubectl shims:\n"
                + "\n".join(f"  {line}" for line in writes),
            )


if __name__ == "__main__":
    unittest.main()
