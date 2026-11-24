"""The Spark limits the guidance quotes are the ones manifests/spark.yaml sets (#108)."""
import pathlib
import re
import unittest

import yaml

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
MANIFEST = PLATFORM / "manifests" / "spark.yaml"
README = PLATFORM.parent / "python" / "stardelt" / "README.md"
GUIDE = PLATFORM.parent.parent / "stardelt-docs" / "docs" / "guides" / "spark.md"


def manifest_numbers():
    """The sizing values the ConfigMap and the Deployment set, as ints."""
    docs = list(yaml.safe_load_all(MANIFEST.read_text()))
    conf = next(
        d for d in docs
        if d and d.get("kind") == "ConfigMap" and "spark-defaults.conf" in (d.get("data") or {})
    )
    values = {}
    for line in conf["data"]["spark-defaults.conf"].splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition(" ")
        values[key] = value.strip()
    deployment = next(
        d for d in docs if d and d.get("kind") == "Deployment" and d["metadata"]["name"] == "spark-connect"
    )
    limit = deployment["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["memory"]
    return {
        "driver_heap_gib": int(values["spark.driver.memory"].rstrip("g")),
        "driver_pod_gib": int(limit.rstrip("Gi")),
        "max_result_mib": int(values["spark.driver.maxResultSize"].rstrip("m")),
        "executor_gib": int(values["spark.executor.memory"].rstrip("g")),
        "executor_cores": int(values["spark.executor.cores"]),
        "max_executors": int(values["spark.dynamicAllocation.maxExecutors"]),
    }


class SparkGuidanceNumbers(unittest.TestCase):
    def test_the_helper_readme_quotes_the_manifest(self):
        text = README.read_text()
        n = manifest_numbers()
        match = re.search(r"(\d+) GiB heap in a\s+(\d+) GiB pod", text)
        self.assertIsNotNone(match, "the README no longer carries the sentence '1 GiB heap in a 3 GiB pod'")
        heap, pod = match.groups()
        self.assertEqual((int(heap), int(pod)), (n["driver_heap_gib"], n["driver_pod_gib"]))
        self.assertIn(f"maxResultSize` {n['max_result_mib']} MiB", text)
        match = re.search(r"at most (\d+) x (\d+) core x (\d+) GiB", text)
        self.assertIsNotNone(
            match, "the README no longer carries the sentence 'at most 2 x 1 core x 1 GiB today'"
        )
        execs = match.groups()
        self.assertEqual(
            tuple(int(x) for x in execs),
            (n["max_executors"], n["executor_cores"], n["executor_gib"]),
        )

    def test_the_guide_quotes_the_manifest(self):
        if not GUIDE.exists():
            self.skipTest(f"no docs checkout at {GUIDE}")
        text = GUIDE.read_text()
        n = manifest_numbers()
        match = re.search(r"at most two executors of one core and (\d+) GiB each", text)
        self.assertIsNotNone(
            match, "the guide no longer carries the sentence 'at most two executors of one core and 1 GiB each'"
        )
        self.assertEqual(int(match.group(1)), n["executor_gib"])
        match = re.search(r"stop at (\d+) MB", text)
        self.assertIsNotNone(
            match, "the guide no longer carries the sentence 'collect() and toPandas() stop at 512 MB'"
        )
        self.assertEqual(int(match.group(1)), n["max_result_mib"])


if __name__ == "__main__":
    unittest.main()
