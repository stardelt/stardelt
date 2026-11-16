"""Tests that the helper images are referenced by digest, not by floating tag (platform#1563).

`scripts/check_pin_freshness.py` reads live Deployments; these two images run in a CronJob
and in one-shot Jobs, so nothing watched them. `manifests/pg-dump-backup.yaml` already
pinned `amazon/aws-cli:2.17.0`; the rest of the repository did not, which let two pods of
the same manifest run different bytes (platform#1563). The two helper tests are the start;
`test_every_applied_image_is_pinned` covers every `image:` line in `manifests/` and `dev/`.
"""

import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

IMAGE_LINE = re.compile(r"^\s*(?:-\s*)?image:\s*(\S+)\s*$")

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
        """The whole class, not two names: every `image:` a pod runs carries a digest."""
        total = 0
        for path in applied_manifest_files():
            relative = str(path.relative_to(REPO))
            for number, line in code_lines(path):
                match = IMAGE_LINE.match(line)
                if not match:
                    continue
                total += 1
                self.assertIn(
                    "@sha256:",
                    match.group(1),
                    f"{relative}:{number}: the bare image reference",
                )
        self.assertEqual(
            total,
            18,
            "an image line was added or removed — pin it and update this count",
        )


if __name__ == "__main__":
    unittest.main()
