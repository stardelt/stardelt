"""`scripts/pin-rollup.sh` rolls every image `scripts/check_pin_freshness.py` checks (#1728).

The checker is the definition of "a pin on `main`"; the rollup is the thing that moves it.
When the two disagree the drift is only fixable by hand, which is what happened to the
notebook image (`ghcr.io/stardelt/jupyter-singleuser:dev`, three files): it was checked but
never rolled, so `make check-pins` stayed red until a person noticed.

The app names are derived from the checker's `APPS`, not listed here, so the next image
added to the checker fails this test until the rollup covers it too.

Standard library only, no network and no cluster; discovered by
`python3 -m unittest discover -s scripts/tests`.
"""

import re
import unittest
from pathlib import Path

from scripts.check_pin_freshness import APPS

REPO = Path(__file__).resolve().parents[2]
ROLLUP = REPO / "scripts" / "pin-rollup.sh"
PIN_IMAGE = REPO / "scripts" / "pin-image.sh"


def app_names() -> list[str]:
    """The distinct apps the freshness checker resolves, in the order it names them."""
    return list(dict.fromkeys(app for app, *_ in APPS))


def changes_block(text: str) -> str:
    """The lines between `changes=()` and the empty-changes early exit."""
    start = text.index("changes=()")
    end = text.index('if [ "${#changes[@]}" -eq 0 ]', start)
    return text[start:end]


class PinRollupCoversCheckerTest(unittest.TestCase):
    def test_each_checked_app_is_resolved_and_added_to_changes(self):
        text = ROLLUP.read_text()
        block = changes_block(text)
        self.assertIn("APP=jupyter WRITE_MANIFEST_ONLY=1", text,
                      "the notebook image is pinned three times; its writer lives in pin-image.sh")
        for app in app_names():
            # `pin <app>` resolves the tag's digest; without it the app is never compared.
            self.assertIn(f"(pin {app})", text,
                          f"scripts/pin-rollup.sh does not resolve the {app} pin,"
                          f" but scripts/check_pin_freshness.py checks it")
            # The `changes` entry is what the PR body and the output line report; the
            # operator is shortened to `operator` there.
            label = app.removeprefix("stardelt-")
            self.assertIn(f'changes+=("{label} ', block,
                          f"scripts/pin-rollup.sh never reports a {label} pin change"
                          f" in its `changes` block")


def git_add_block(text: str) -> str:
    """The `git add …` command that stages the rollup's working tree, up to the commit."""
    start = text.index("git add ")
    end = text.index("git commit", start)
    return text[start:end]


def pin_image_relative_files() -> list[str]:
    """Every repository-relative file `pin-image.sh` assigns to a named path inside its writer.

    The spark branch names two of them (`catalogue`, `fixture`) and writes them through
    `open(…)`; a writer added the same way is covered without touching this test.
    """
    text = PIN_IMAGE.read_text()
    return re.findall(r'^\s+(?:catalogue|fixture) = "([^"]+)"\s*$', text, re.M)


class PinRollupStagesEveryFilePinImageWritesTest(unittest.TestCase):
    """A file pin-image.sh writes but the rollup does not stage is silently dropped (#450).

    Measured 2026-11-19: `scripts/pin-image.sh` writes the Spark executor fixture for every
    spark pin, the rollup's `git add` list named only the manifest and the runtime catalogue,
    and the open rollup PR was red on `Lint` — `test_verify_spark_pod_guard` compares the
    fixture with `manifests/spark.yaml` and refuses when they drift (#209).
    """

    def test_every_named_writer_file_is_staged(self):
        staged = git_add_block(ROLLUP.read_text())
        files = pin_image_relative_files()
        self.assertIn("scripts/fixtures/spark-executor-pod.yaml", files,
                      "the fixture is the file this guard was written for; rename it here too")
        for path in files:
            self.assertIn(path, staged,
                          f"pin-image.sh writes {path} but scripts/pin-rollup.sh does not stage it")

    def test_the_executor_fixture_and_the_runtime_catalogue_are_staged(self):
        staged = git_add_block(ROLLUP.read_text())
        self.assertIn("scripts/fixtures/spark-executor-pod.yaml", staged)
        self.assertIn("chart/files/spark-runtimes.json", staged)


class RollupMergesAsASquashTest(unittest.TestCase):
    """stardelt/stardelt#1789, option B: main keeps linear history, so the roll squashes."""

    def test_the_roll_merges_with_squash_never_a_merge_commit(self):
        text = ROLLUP.read_text()
        self.assertIn('gh pr merge "$pr_number" --auto --squash', text)
        self.assertIn('gh pr merge "$pr_number" --squash --delete-branch', text)
        self.assertNotIn("--merge", text, "main's ruleset refuses merge commits")

    def test_the_operator_is_not_rebuilt_on_the_branch(self):
        text = ROLLUP.read_text()
        self.assertNotIn("operator-image.yml", text,
                         "the operator built on main from the squash commit is pinned by the next roll")
        self.assertNotIn("embedded_changed", text)

    def test_a_branch_from_an_old_checkout_is_refused_before_any_push(self):
        """#1644: a run whose checkout main has moved past must not pin images older than main."""
        text = ROLLUP.read_text()
        guard = text.index("git merge-base --is-ancestor origin/main HEAD")
        self.assertLess(guard, text.index('git push --force-with-lease origin "$branch"'),
                        "the guard runs before the branch is pushed")

    def test_the_workflow_grants_actions_write_and_ignores_non_main_builds(self):
        wf = REPO.parent / ".github" / "workflows" / "platform-pin-rollup.yml"
        text = wf.read_text()
        self.assertIn("actions: write", text)
        self.assertIn("GITHUB_TOKEN: ${{ github.token }}", text)
        # The guard must sit on the job's `if:` line, not in a comment.
        if_lines = [line for line in text.splitlines() if re.match(r"^\s+if: ", line)]
        self.assertEqual(len(if_lines), 1, f"expected one job-level if:, found {if_lines}")
        self.assertIn("github.event.workflow_run.head_branch == 'main'", if_lines[0])

    def test_a_waiting_run_is_never_cancelled_by_the_next_one(self):
        wf = REPO.parent / ".github" / "workflows" / "platform-pin-rollup.yml"
        text = wf.read_text()
        self.assertIn("group: platform-pin-rollup", text)
        self.assertIn("cancel-in-progress: false", text)
        self.assertNotIn("cancel-in-progress: true", text)


if __name__ == "__main__":
    unittest.main()
