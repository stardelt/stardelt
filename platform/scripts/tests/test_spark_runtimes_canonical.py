"""The runtime catalogue is compared as JSON, not as serialized strings (platform#1591).

`chart/files/spark-runtimes.json` is the one catalogue; the ConfigMap
`stardelt-spark-runtimes` carries every entry as a JSON *string*. The strings are
written by different tools with different separators (`pin-image.sh` renders
`json.dumps` with default separators, the Makefile writes `jq ... tojson` compact),
so comparing the strings reported a false FAIL on a correct catalogue. The check
must compare what the field means.

The test reproduces both writer shapes and runs the same `jq` pipeline that
`scripts/verify-compute.sh` runs: the fixed one (`fromjson` on the live side, the
chart file untouched) must agree on both, and the old string comparison must still
differ — the whitespace-only difference alone is not a change. A changed digest
must still be caught.

Standard library plus the `jq` binary; the module skips itself where `jq` is absent.
"""

import json
import pathlib
import shutil
import subprocess
import tempfile
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
CATALOGUE = REPO_ROOT / "chart" / "files" / "spark-runtimes.json"

JQ = shutil.which("jq")


def jq(args, stdin_text=""):
    """Run `jq` with `args` and return its stdout, failing loudly on an error."""
    result = subprocess.run([JQ, *args], input=stdin_text, capture_output=True,
                            text=True, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise AssertionError("jq %s failed: %s" % (" ".join(args), result.stderr))
    return result.stdout


def write_temp(directory, name, document):
    path = pathlib.Path(directory) / name
    path.write_text(json.dumps(document), encoding="utf-8")
    return str(path)


def compact_values(catalogue):
    """The `data` map with jq's compact `tojson` strings — the Makefile's shape."""
    document = jq(["-c", '{"data": with_entries(.value = (.value | tojson))}'],
                  stdin_text=json.dumps(catalogue))
    return json.loads(document)["data"]


def default_values(catalogue):
    """The `data` map with `json.dumps` default separators — `pin-image.sh`'s shape."""
    return {name: json.dumps(value) for name, value in catalogue.items()}


def fixed_comparison(document_path):
    """`verify-compute.sh`'s fixed pipeline: `fromjson` on the live side."""
    live = jq(["-S", ".data | with_entries(.value |= fromjson)"],
              stdin_text=pathlib.Path(document_path).read_text(encoding="utf-8"))
    wanted = jq(["-S", "."], stdin_text=CATALOGUE.read_text(encoding="utf-8"))
    return live == wanted


def old_comparison(document_path):
    """The pipeline before #1591: both sides serialized, so whitespace matters."""
    live = jq(["-S", ".data"],
              stdin_text=pathlib.Path(document_path).read_text(encoding="utf-8"))
    wanted = jq(["-S", "with_entries(.value |= tojson)"],
                stdin_text=CATALOGUE.read_text(encoding="utf-8"))
    return live == wanted


@unittest.skipUnless(JQ, "jq is not on PATH")
class CanonicalCatalogueTest(unittest.TestCase):
    def setUp(self):
        with open(CATALOGUE, encoding="utf-8") as handle:
            self.catalogue = json.load(handle)
        self.assertIn("spark-4.1-py3.11", self.catalogue)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.pin_image = write_temp(self.directory.name, "pin-image.json",
                                    {"data": default_values(self.catalogue)})
        self.makefile = write_temp(self.directory.name, "makefile.json",
                                   {"data": compact_values(self.catalogue)})

    def test_the_two_writer_shapes_are_whitespace_different(self):
        data = json.loads(pathlib.Path(self.pin_image).read_text(encoding="utf-8"))["data"]
        self.assertNotEqual(data["spark-4.1-py3.11"],
                            compact_values(self.catalogue)["spark-4.1-py3.11"])
        self.assertEqual(json.loads(data["spark-4.1-py3.11"]),
                         json.loads(compact_values(self.catalogue)["spark-4.1-py3.11"]))

    def test_the_pin_image_shape_matches_the_chart(self):
        self.assertTrue(fixed_comparison(self.pin_image))

    def test_the_makefile_shape_matches_the_chart(self):
        self.assertTrue(fixed_comparison(self.makefile))

    def test_the_old_string_comparison_differed_regression_pin(self):
        self.assertFalse(old_comparison(self.pin_image))
        self.assertTrue(old_comparison(self.makefile))

    def test_a_changed_digest_still_differs(self):
        changed = json.loads(json.dumps(self.catalogue))
        entry = changed["spark-4.1-py3.11"]
        entry["image"] = entry["image"].rsplit(":", 1)[0] + ":" + "0" * 64
        stale = write_temp(self.directory.name, "stale.json",
                           {"data": default_values(changed)})
        self.assertFalse(fixed_comparison(stale))

    def test_a_changed_spark_conf_still_differs(self):
        changed = json.loads(json.dumps(self.catalogue))
        changed["spark-4.1-py3.11"]["sparkConf"]["spark.sql.shuffle.partitions"] = "1"
        stale = write_temp(self.directory.name, "stale-conf.json",
                           {"data": compact_values(changed)})
        self.assertFalse(fixed_comparison(stale))


if __name__ == "__main__":
    unittest.main()
