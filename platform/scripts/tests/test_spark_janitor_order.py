"""The Spark ConfigMap janitor's script ConfigMap is filled, never applied (platform#1584).

`manifests/spark-confmap-janitor.yaml` declared the script ConfigMap with `data: {}`, and
`make install-spark` filled that ConfigMap *before* applying the manifest — so every
install emptied it again and the CronJob's pods failed with
`bash: /opt/scripts/spark-confmap-janitor.sh: No such file or directory` (measured on the
lab 2026-11-13T21:1xZ).

Reordering the install was not enough: `make spark-confmap-janitor-config` fills the object
with `kubectl apply`, which records `data` in the last-applied configuration, and a later
`kubectl apply -f manifests/spark-confmap-janitor.yaml` of a document without `data` computes
"remove data" and empties the live object. That is how the #1577 rollout left every scheduled
run exiting 127 (2026-11-16T14:00Z onward).

The manifest therefore carries no ConfigMap document at all; the script is only ever written
by `make spark-confmap-janitor-config` (or the operator). The class test below keeps a future
manifest from reintroducing the same removal for any ConfigMap the Makefile fills.
"""

import pathlib
import re
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "manifests" / "spark-confmap-janitor.yaml"
MAKEFILE = REPO_ROOT / "Makefile"
SCRIPTS = REPO_ROOT / "scripts"


def documents(path):
    with open(path, encoding="utf-8") as handle:
        return [d for d in yaml.safe_load_all(handle) if d]


def filled_configmap_names():
    """Every ConfigMap name the Makefile creates with `create configmap <name>`."""
    text = MAKEFILE.read_text(encoding="utf-8")
    names = set()
    for raw in re.findall(r"create configmap (\S+)", text):
        name = raw.strip("\"'")
        if "$" in name:
            # A shell variable (`create configmap "$$cm"`): not a literal name to pin.
            continue
        names.add(name)
    return names


def applied_manifest_files():
    """Every `manifests/<file>` a `kubectl apply -f` in the Makefile or scripts names.

    Only files an apply names are scanned: `manifests/openbao-group-policies-script.yaml`
    declares a filled ConfigMap but is never applied (it is input to a `create ... --from-file`),
    so applying it is not a risk. A file that no apply names cannot empty anything.
    """
    pattern = re.compile(r"apply\s+(?:-\S+\s+)*-f\s+manifests/([A-Za-z0-9._-]+)")
    files = set()
    sources = [MAKEFILE, *sorted(SCRIPTS.glob("*.sh"))]
    for source in sources:
        text = source.read_text(encoding="utf-8", errors="replace")
        for match in pattern.finditer(text):
            candidate = REPO_ROOT / "manifests" / match.group(1)
            if candidate.is_file():
                files.add(candidate)
    return files


class JanitorInstall(unittest.TestCase):
    def test_the_workload_manifest_has_no_configmap(self):
        kinds = [d.get("kind") for d in documents(MANIFEST)]
        self.assertNotIn(
            "ConfigMap",
            kinds,
            "the workload manifest must not carry the script ConfigMap: an apply of a document "
            "without `data` removes the data a `create configmap` apply recorded",
        )
        self.assertIn("CronJob", kinds, "the janitor CronJob left the workload manifest")

    def test_install_fills_the_script_after_applying_the_manifest(self):
        text = MAKEFILE.read_text(encoding="utf-8")
        block = re.search(r"^install-spark:.*?(?=^\S|\Z)", text, re.S | re.M)
        self.assertIsNotNone(block, "the install-spark target is missing")
        body = block.group(0)
        apply_at = body.find("apply -f manifests/spark-confmap-janitor.yaml")
        fill_at = body.find("spark-confmap-janitor-config")
        self.assertGreaterEqual(apply_at, 0, "install-spark does not apply the janitor manifest")
        self.assertGreaterEqual(fill_at, 0, "install-spark does not fill the janitor script")
        self.assertLess(
            apply_at,
            fill_at,
            "the manifest apply must come before the script is filled, or it empties it",
        )

    def test_no_applied_manifest_carries_a_filled_configmap(self):
        filled = filled_configmap_names()
        self.assertIn(
            "spark-confmap-janitor-script",
            filled,
            "the Makefile no longer fills the janitor script with `create configmap`",
        )
        applied = applied_manifest_files()
        self.assertTrue(applied, "no `kubectl apply -f manifests/...` was found to scan")
        self.assertIn(MANIFEST, applied, "the janitor workload manifest is not applied by any target")

        offenders = []
        for path in sorted(applied):
            for document in documents(path):
                if document.get("kind") != "ConfigMap":
                    continue
                name = (document.get("metadata") or {}).get("name")
                if name in filled:
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{name}")
        self.assertEqual(
            [],
            offenders,
            "an applied manifest declares a ConfigMap the Makefile fills; applying it removes "
            "the filled data unless the document carries the same data",
        )


if __name__ == "__main__":
    unittest.main()
