"""scripts/verify-spark-no-platform-key.sh prints names, never the storage key's value (#427).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests`
runs them in CI, and `python3 -m pytest scripts/tests/test_verify_spark_no_platform_key.py -q`
runs the same tests. No cluster is touched — the script is read as text, and `bash -n` parses it.

The check exists to prove the platform storage key is absent from the shared Spark Connect
server's driver, executors, user code and executor admission (#75). Its own risk is the one it
measures: a probe that prints a value instead of a name would copy the key into a log, a CI
transcript and this ticket. Each assertion below is that guard or one of the four facts it reads.
"""

import re
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "verify-spark-no-platform-key.sh"

# The four judged lines: the path the key would travel, and the string the script prints for it.
FACT_LINES = ["driver-env=", "executor-conf=", "udf-env=", "pod-guard="]

# The three objects the four paths are read from.
OBJECTS = ["stardelt-s3-creds", "spark-connect-config", "spark-connect-executor-pods"]


class NoPlatformKeyVerifier(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = SCRIPT.read_text()
        # Comments explain the value case; only code can leak one.
        cls.code = "\n".join(line for line in cls.text.splitlines()
                             if not line.lstrip().startswith("#"))

    def test_the_script_exists_parses_and_is_executable(self):
        self.assertTrue(SCRIPT.is_file(), "scripts/verify-spark-no-platform-key.sh is missing")
        done = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertTrue(SCRIPT.stat().st_mode & 0o111, "the script is not executable")

    def test_it_never_reads_an_env_entrys_value(self):
        # `driver-env` comes from `secretKeyRef.name`; `.value` (but not `.valueFrom`) is the
        # literal next to it, and asking for it would put the key on stdout.
        self.assertNotRegex(self.code, r"\.value\b")
        self.assertIn("secretKeyRef.name? ==", self.code)
        self.assertIn("| .name]", self.code, "the driver fact must select names")

    def test_no_output_line_prints_a_value_from_the_secret(self):
        printing = [line.strip() for line in self.code.splitlines()
                    if re.match(r"(echo|printf)\b", line.strip())]
        self.assertTrue(printing, "the script prints nothing")
        for line in printing:
            self.assertNotRegex(
                line, r"\.value\b|\.key\b|valueFrom|secretKeyRef\.key",
                f"an output line reads the Secret: {line}",
            )
        # No line pipes a Secret value into the output: jq only ever emits `.name`/`.data` keys.
        self.assertNotIn("secretKeyRef.key", self.code)

    def test_it_prints_the_four_judged_lines(self):
        # A line that *starts* with echo/printf and prints the fact first, not an extraction that
        # merely mentions it later on the same line.
        missing = [fact for fact in FACT_LINES
                   if not re.search(rf"^\s*(echo|printf)[^\n]*[\"']{re.escape(fact)}",
                                    self.code, re.MULTILINE)]
        self.assertEqual(missing, [], "these fact lines are not printed by the script")

    def test_it_reads_the_three_objects(self):
        missing = [name for name in OBJECTS if name not in self.code]
        self.assertEqual(missing, [], "the script does not read these objects")

    def test_a_pass_needs_all_four_paths_clean(self):
        for condition in ('[ "$driver_env" = none ]',
                          '[ "$executor_conf" = none ]',
                          '[ "$udf_env" = none ]',
                          '[ "$pod_guard" = "admits no Secret" ]'):
            self.assertIn(condition, self.code, f"PASS does not require {condition}")
        self.assertIn("exit 0", self.code)
        self.assertIn("exit 1", self.code)
        self.assertIn("exit 3", self.code, "UNVERIFIED must stay distinct from FAIL")

    def test_a_missing_policy_is_a_failure_not_a_skip(self):
        self.assertIn("pod_guard=missing", self.code)


if __name__ == "__main__":
    unittest.main()
