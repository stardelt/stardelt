"""The Spark limits the guidance quotes are the ones manifests/spark.yaml sets (#108)."""
import pathlib
import re
import unittest

import yaml

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
MANIFEST = PLATFORM / "manifests" / "spark.yaml"
README = PLATFORM.parent / "python" / "stardelt" / "README.md"
GUIDE = PLATFORM.parent.parent / "stardelt-docs" / "docs" / "guides" / "spark.md"
PERFORMANCE = PLATFORM.parent.parent / "stardelt-docs" / "docs" / "guides" / "performance.md"
HELPER = PLATFORM.parent / "python" / "stardelt" / "__init__.py"
CONNECT_GUIDE = PLATFORM.parent.parent / "stardelt-docs" / "docs" / "guides" / "connect-your-tools.md"
OPERATOR = PLATFORM.parent / "operator" / "crates" / "stardelt-operator" / "src" / "spark_cluster.rs"


def spark_tips():
    """`SPARK_TIPS` from the helper, parsed — the seven strings `spark_tips()` prints."""
    import ast
    tree = ast.parse(HELPER.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "SPARK_TIPS":
            return [ast.literal_eval(element) for element in node.value.elts]
    raise AssertionError("python/stardelt/__init__.py no longer defines SPARK_TIPS")


def readme_table_rows():
    """The data rows of the README's "Keeping the shared driver healthy" table."""
    text = README.read_text()
    block = text.split("## Keeping the shared driver healthy", 1)[1].split("\n## ", 1)[0]
    rows = [line for line in block.splitlines() if line.startswith("| ")]
    if not rows:
        raise AssertionError("the README no longer carries the 'Keeping the shared driver healthy' table")
    data = [row for row in rows[1:] if not set(row) <= set("|-: ")]
    return data


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


def size_presets():
    """`preset()`'s three classes: name -> (driver cores, driver GiB, executor cores, executor GiB, max)."""
    rows = {}
    pattern = re.compile(
        r'SparkClusterSize::(Small|Medium|Large) => \((\d+), "(\d+)Gi", (\d+), "(\d+)Gi", (\d+)\)'
    )
    for match in pattern.finditer(OPERATOR.read_text()):
        rows[match.group(1).lower()] = tuple(int(value) for value in match.groups()[1:])
    if set(rows) != {"small", "medium", "large"}:
        raise AssertionError("spark_cluster.rs::preset no longer has the three size classes")
    return rows


def guide_presets():
    """The Compute guide's size table, in the same shape."""
    rows = {}
    pattern = re.compile(
        r"^[ \t]*\|[ \t]*\*\*(small|medium|large)\*\*[ \t]*\|[ \t]*(\d+) cores?, (\d+) GiB[ \t]*\|"
        r"[ \t]*(\d+) cores?, (\d+) GiB[ \t]*\|[ \t]*(\d+)[ \t]*\|",
        re.M,
    )
    for match in pattern.finditer(CONNECT_GUIDE.read_text()):
        rows[match.group(1)] = tuple(int(value) for value in match.groups()[1:])
    if set(rows) != {"small", "medium", "large"}:
        raise AssertionError("connect-your-tools.md no longer carries the three size rows")
    return rows


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

    def test_the_performance_page_quotes_the_manifest(self):
        if not PERFORMANCE.exists():
            return
        text = PERFORMANCE.read_text()
        numbers = manifest_numbers()
        for phrase in (
            f"{numbers['driver_heap_gib']} GiB",
            f"{numbers['driver_pod_gib']} GiB",
            f"{numbers['max_result_mib']} MiB",
            f"{numbers['executor_gib']} GiB",
            f"{numbers['executor_cores']} core",
            f"{numbers['max_executors']} executors",
        ):
            self.assertIn(phrase, text, f"the Performance page does not quote {phrase!r} from the manifest")
        self.assertIn("stardelt skill", text, "the Performance page must name how an assistant gets the rules")

    def test_the_helper_tips_quote_the_manifest(self):
        tips = spark_tips()
        text = "\n".join(tips)
        n = manifest_numbers()
        self.assertIn(f"{n['driver_heap_gib']} GiB driver heap", text)
        self.assertIn(f"above {n['max_result_mib']} MiB", text)
        self.assertIn(f"{n['executor_gib']} GiB each", text)
        self.assertEqual(len(tips), len(readme_table_rows()))

    def test_the_compute_guide_states_the_operator_presets(self):
        if not CONNECT_GUIDE.exists():
            self.skipTest(f"no docs checkout at {CONNECT_GUIDE}")
        self.assertEqual(guide_presets(), size_presets())


if __name__ == "__main__":
    unittest.main()
