"""Every waiting `helm upgrade` can roll back a failed one (#1122).

A release left `pending-upgrade` refuses every later upgrade, which stalled the dev cluster
three times; `--atomic` makes helm roll back instead. The recipes are read from the Makefile so
a new install step cannot silently skip it.
"""

import pathlib
import re
import unittest

MAKEFILE = pathlib.Path(__file__).resolve().parents[2] / "Makefile"

# The umbrella chart is the one upgrade that does not wait (no `--wait`, no `$(HELM_FLAGS)`):
# a release that never waits does not pass through the pending state this fixes, and
# `--atomic` would imply `--wait` and make it block on every workload. The exemption is
# bounded by `test_chart_install_still_does_not_wait`: adding `--wait` there fails the suite.
#
# `step-lakekeeper` too (#1384): a namespace that enforces `restricted` refuses the chart's
# unpatched `opa` sidecar, so `--wait` times out and `--atomic` rolls back before
# `scripts/lakekeeper-opa-security.sh` patches it; the step waits itself with `rollout status`
# after the patch. Bounded by `test_lakekeeper_step_still_does_not_wait`.
NON_WAITING = {"chart-install", "step-lakekeeper"}


def helm_upgrades():
    """Every logical recipe line that runs `helm upgrade`, with the target it belongs to."""
    lines = MAKEFILE.read_text(encoding="utf-8").splitlines()
    found, target = [], None
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.match(r"^[a-z][a-z0-9-]*:", line) and not line.startswith("\t"):
            target = line.split(":", 1)[0]
        if "helm upgrade" in line and not line.lstrip().startswith("#") and "@#" not in line:
            block = [line]
            while lines[i].rstrip().endswith("\\") and i + 1 < len(lines):
                i += 1
                block.append(lines[i])
            found.append((target, "\n".join(block)))
        i += 1
    return found


def helm_flags_line():
    """The `HELM_FLAGS :=` line every waiting upgrade passes to helm."""
    return next(
        line
        for line in MAKEFILE.read_text(encoding="utf-8").splitlines()
        if line.startswith("HELM_FLAGS :=")
    )


def carries_atomic(block):
    """A recipe carries `--atomic` itself, or through a `HELM_FLAGS` that carries it."""
    return "--atomic" in block or (
        "$(HELM_FLAGS)" in block and "--atomic" in helm_flags_line()
    )


class HelmAtomicTest(unittest.TestCase):
    def test_every_helm_upgrade_is_atomic(self):
        offenders = [
            target
            for target, block in helm_upgrades()
            if target not in NON_WAITING and not carries_atomic(block)
        ]
        self.assertEqual(offenders, [], "these targets upgrade without --atomic")

    def test_helm_flags_carry_atomic_when_they_wait(self):
        line = helm_flags_line()
        self.assertIn("--wait", line)
        self.assertIn("--atomic", line)

    def test_chart_install_still_does_not_wait(self):
        blocks = [block for target, block in helm_upgrades() if target == "chart-install"]
        self.assertTrue(blocks, "chart-install no longer runs `helm upgrade`")
        for block in blocks:
            self.assertNotIn("--wait", block, "chart-install waits now: it needs --atomic too")
            self.assertNotIn("$(HELM_FLAGS)", block, "chart-install waits now: it needs --atomic too")
            self.assertNotIn("--atomic", block, "chart-install does not wait, so it takes no --atomic")

    def test_lakekeeper_step_still_does_not_wait(self):
        # #1384: the exemption above is only sound while the release does not wait. A
        # `step-lakekeeper` that waits again needs `--atomic` again, and its opa patch has to
        # run before the release does.
        blocks = [block for target, block in helm_upgrades() if target == "step-lakekeeper"]
        self.assertTrue(blocks, "step-lakekeeper no longer runs `helm upgrade`")
        for block in blocks:
            self.assertNotIn("--wait", block, "step-lakekeeper waits now: it needs --atomic too")
            self.assertNotIn("$(HELM_FLAGS)", block, "step-lakekeeper waits now: it needs --atomic too")
            self.assertNotIn("--atomic", block, "step-lakekeeper does not wait, so it takes no --atomic")

    def test_a_recipe_that_switches_wait_off_switches_atomic_off_too(self):
        # `--atomic` implies `--wait` whatever the flag order: a recipe that turns the wait off
        # for a workload it knows cannot become Ready (openbao's first install) must turn the
        # rollback off as well, or helm waits, times out and uninstalls what it just created.
        offenders = [
            target
            for target, block in helm_upgrades()
            if "--wait=false" in block and carries_atomic(block) and "--atomic=false" not in block
        ]
        self.assertEqual(offenders, [], "these targets switch --wait off but keep --atomic")


# --- the shell scripts, not only the Makefile ----------------------------------------------
# A `helm upgrade` past the Makefile is still an upgrade: `sync-operator-chart.sh` (called by
# pin-image.sh) and `catalog-authz-bootstrap.sh` both wait on the operator's and the catalog's
# release. The same pending-* trap applies, so the same rule does.
SCRIPTS = pathlib.Path(__file__).resolve().parents[1]


def script_upgrades():
    """Every `helm upgrade` command in `scripts/*.sh`, with the file it belongs to."""
    found = []
    for path in sorted(SCRIPTS.glob("*.sh")):
        lines = path.read_text(encoding="utf-8").splitlines()
        i = 0
        while i < len(lines):
            line = lines[i]
            if re.match(r"^\s*helm upgrade\b", line):
                block = [line]
                while lines[i].rstrip().endswith("\\") and i + 1 < len(lines):
                    i += 1
                    block.append(lines[i])
                found.append((path.name, "\n".join(block)))
            i += 1
    return found


def script_helm_flags(path_name):
    """The `HELM_FLAGS=` default a script passes to its upgrades, or an empty string."""
    match = re.search(r"^HELM_FLAGS=.*$", (SCRIPTS / path_name).read_text(encoding="utf-8"),
                      re.MULTILINE)
    return match.group(0) if match else ""


class ScriptUpgradeTest(unittest.TestCase):
    def test_the_walker_finds_the_scripts_that_upgrade(self):
        names = {name for name, _ in script_upgrades()}
        for expected in ("sync-operator-chart.sh", "catalog-authz-bootstrap.sh"):
            self.assertIn(expected, names, "the script walker lost a `helm upgrade`")

    def test_every_script_upgrade_waiting_on_a_release_is_atomic(self):
        offenders = []
        for name, block in script_upgrades():
            if "--wait=false" in block:
                continue
            if "--atomic" in block:
                continue
            if re.search(r"\$\{?HELM_FLAGS\}?", block) and "--atomic" in script_helm_flags(name):
                continue
            offenders.append(name)
        self.assertEqual(offenders, [], "these scripts upgrade without --atomic")


if __name__ == "__main__":
    unittest.main()
