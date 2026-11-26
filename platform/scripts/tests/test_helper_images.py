"""Tests that the helper images are referenced by digest, not by floating tag (platform#1563).

`scripts/check_pin_freshness.py` reads live Deployments; these two images run in a CronJob
and in one-shot Jobs, so nothing watched them. `manifests/pg-dump-backup.yaml` already
pinned `amazon/aws-cli:2.17.0`; the rest of the repository did not, which let two pods of
the same manifest run different bytes (platform#1563). The two helper tests are the start;
`test_every_applied_image_is_pinned` covers every `image:` line in `manifests/` and `dev/`.
"""

import json
import re
import unittest
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]

IMAGE_LINE = re.compile(r"^\s*(?:-\s*)?image:\s*(\S+)(?:\s+#.*)?$")
IMAGE_NAME_LINE = re.compile(r"^\s*imageName:\s*(\S+)\s*$")

AWS_CLI = "amazon/aws-cli:2.17.0"
AWS_CLI_DIGEST = "@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73"
PYTHON = "python:3.13.16-slim-bookworm"
PYTHON_DIGEST = "@sha256:73aa5faee130c60da030e105ec971ccf57473fbd22ce73bd6fc9ba99a40b838d"

# The references platform#1563 pinned. `manifests/pg-dump-backup.yaml` pinned the aws-cli
# image earlier and is deliberately not part of the count below.
AWS_CLI_PINS = {
    "manifests/object-store-backup.yaml": 1,
    "scripts/create-backup-bucket.sh": 1,
    "scripts/create-notebooks-bucket.sh": 1,
    "scripts/verify-backups.sh": 1,
    "scripts/verify-backups-all.sh": 1,
}
PYTHON_PINS = {
    "manifests/access-sync.yaml": 2,
    "manifests/demo-reset.yaml": 1,
}


def scanned_files():
    """Every manifest and script a pod image can be declared in."""
    paths = sorted((REPO / "manifests").glob("*.yaml")) + sorted(
        (REPO / "scripts").glob("*.sh")
    )
    return [path for path in paths if path.is_file()]


def code_lines(path):
    """Yield (line number, line) for the lines that are not comments."""
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.lstrip().startswith("#"):
            yield number, line


def applied_manifest_files():
    """Every YAML a `make` step or a documented lab step applies a pod image from."""
    paths = sorted((REPO / "manifests").glob("*.yaml")) + sorted((REPO / "dev").rglob("*.yaml"))
    return [path for path in paths if path.is_file()]


def helm_values_files():
    """Every Helm values file the install paths upgrade a chart with (`make step-*`)."""
    return [path for path in sorted((REPO / "helm-values").rglob("*.yaml")) if path.is_file()]


# manifests/spark-demo-sandbox.yaml carries no digest: `make install-spark-demo` renders
# `__SPARK_IMAGE__` from the runtime catalogue chart/files/spark-runtimes.json at install time,
# so the public demo's sandbox follows the pin without a second copy of it (stardelt/stardelt#246).
# The placeholder is judged where it is resolved — the pin it renders must itself carry a digest —
# so nothing here floats by tag.
SPARK_IMAGE_PLACEHOLDER = "__SPARK_IMAGE__"


def spark_pin():
    """The runtime catalogue's image, the value `make install-spark-demo` renders."""
    catalogue = json.loads((REPO / "chart" / "files" / "spark-runtimes.json").read_text())
    return next(iter(catalogue.values()))["image"]


def resolved_image(reference):
    """The reference a pod runs: a placeholder is rendered from the manifest it names."""
    if reference == SPARK_IMAGE_PLACEHOLDER:
        return spark_pin() or reference
    return reference


def image_values(value, path=""):
    """Yield (path, tag, digest) for every image reference in one loaded values document.

    Two shapes: an `image: repo:tag` string (a container override), and a block that names
    `tag:` under a key called `image`/`images` (the chart's own image keys), with or without
    a `repository:`. Anything else that happens to carry a `tag` key is not an image.
    """
    if isinstance(value, dict):
        if (
            isinstance(value.get("tag"), str)
            and ("repository" in value or "image" in path.split(".") or "images" in path.split("."))
        ):
            yield path, value["tag"], value.get("digest")
        for key, child in value.items():
            yield from image_values(child, f"{path}.{key}" if path else str(key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from image_values(child, f"{path}[{index}]")
    elif isinstance(value, str) and path.rsplit(".", 1)[-1].split("[")[0] == "image":
        yield path, value, None


class TestHelperImages(unittest.TestCase):
    def assert_every_reference_is_pinned(self, image, digest, expected):
        """No bare reference anywhere, and the enumerated pins are all still there."""
        counts = {path: 0 for path in expected}
        for path in scanned_files():
            relative = str(path.relative_to(REPO))
            for number, line in code_lines(path):
                if image not in line:
                    continue
                self.assertIn(
                    digest,
                    line,
                    f"{relative}:{number}: the bare image reference",
                )
                if relative in counts:
                    counts[relative] += 1
        for relative, wanted in expected.items():
            self.assertEqual(
                counts[relative],
                wanted,
                f"{relative}: expected {wanted} pinned reference(s), found {counts[relative]}",
            )
        self.assertEqual(
            sum(counts.values()),
            sum(expected.values()),
            f"{image}: the pinned references were deleted",
        )

    def test_the_aws_cli_image_has_one_pinned_reference(self):
        self.assert_every_reference_is_pinned(AWS_CLI, AWS_CLI_DIGEST, AWS_CLI_PINS)

    def test_the_python_image_has_one_pinned_reference(self):
        self.assert_every_reference_is_pinned(PYTHON, PYTHON_DIGEST, PYTHON_PINS)

    def test_every_applied_image_is_pinned(self):
        """The whole class, not two names: every `image:`/`imageName:` a pod runs carries a digest."""
        total = 0
        image_names = 0
        for path in applied_manifest_files():
            relative = str(path.relative_to(REPO))
            for number, line in code_lines(path):
                match = IMAGE_LINE.match(line)
                if match:
                    total += 1
                    self.assertIn(
                        "@sha256:",
                        resolved_image(match.group(1)),
                        f"{relative}:{number}: the bare image reference",
                    )
                name_match = IMAGE_NAME_LINE.match(line)
                if name_match:
                    image_names += 1
                    self.assertIn(
                        "@sha256:",
                        name_match.group(1),
                        f"{relative}:{number}: the bare imageName reference",
                    )
        self.assertEqual(
            total,
            23,
            "an image line was added or removed — pin it and update this count",
        )
        self.assertEqual(
            image_names,
            5,
            "an imageName line was added or removed — pin it and update this count",
        )

    def test_every_helm_values_image_is_pinned(self):
        """The charts' values files are an install path too: nothing there floats by tag."""
        lines = 0
        for path in helm_values_files():
            relative = str(path.relative_to(REPO))
            for number, line in code_lines(path):
                match = IMAGE_LINE.match(line)
                if not match:
                    continue
                lines += 1
                self.assertIn(
                    "@sha256:",
                    match.group(1),
                    f"{relative}:{number}: the bare image reference",
                )
        self.assertEqual(
            lines,
            6,
            "an image line was added or removed — pin it and update this count",
        )

        values = 0
        for path in helm_values_files():
            relative = str(path.relative_to(REPO))
            document = yaml.safe_load(path.read_text()) or {}
            for where, tag, digest in image_values(document):
                values += 1
                pinned = "@sha256:" in tag or str(digest or "").startswith("sha256:")
                self.assertTrue(
                    pinned,
                    f"{relative}:{where}: the bare image reference {tag}",
                )
        self.assertEqual(
            values,
            37,
            "an image value was added or removed — pin it and update this count",
        )


if __name__ == "__main__":
    unittest.main()
