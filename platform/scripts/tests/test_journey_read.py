"""The reader of the fresh-node journey (#1806 X1), with boto3, botocore and pyiceberg injected."""
import contextlib
import datetime
import io
import sys
import tempfile
import types
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "journey_read.py"

OLDER = datetime.datetime(2026, 11, 26, 10, 0, 0)
NEWER = datetime.datetime(2026, 11, 26, 11, 0, 0)
OLD_KEY = "warehouse/journey/e2e/exit_test/metadata/00001-old.metadata.json"
NEW_KEY = "warehouse/journey/e2e/exit_test/metadata/00002-new.metadata.json"


def make_modules(pages, metadata_calls, num_rows):
    """Install fake boto3, botocore.config and pyiceberg.table into sys.modules."""

    class FakeS3:
        def list_objects_v2(self, **kwargs):
            assert "Bucket" in kwargs
            token = kwargs.get("ContinuationToken")
            index = 0 if token is None else int(token)
            return pages[index]

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda service, **kwargs: FakeS3()

    fake_botocore = types.ModuleType("botocore")
    fake_config_module = types.ModuleType("botocore.config")

    class Config:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    fake_config_module.Config = Config
    fake_botocore.config = fake_config_module

    fake_pyiceberg = types.ModuleType("pyiceberg")
    fake_table_module = types.ModuleType("pyiceberg.table")

    class FakeScan:
        def to_arrow(self):
            return types.SimpleNamespace(num_rows=num_rows)

    class FakeTable:
        def scan(self):
            return FakeScan()

    class StaticTable:
        @staticmethod
        def from_metadata(location, properties=None):
            metadata_calls.append({"location": location, "properties": properties})
            return FakeTable()

    fake_table_module.StaticTable = StaticTable
    fake_pyiceberg.table = fake_table_module

    return {
        "boto3": fake_boto3,
        "botocore": fake_botocore,
        "botocore.config": fake_config_module,
        "pyiceberg": fake_pyiceberg,
        "pyiceberg.table": fake_table_module,
    }


TWO_PAGES = [
    {
        "Contents": [{"Key": OLD_KEY, "LastModified": OLDER}],
        "IsTruncated": True,
        "NextContinuationToken": "1",
    },
    {
        "Contents": [{"Key": NEW_KEY, "LastModified": NEWER}],
        "IsTruncated": False,
    },
]


class JourneyReadTest(unittest.TestCase):
    def run_main(self, pages, num_rows, calls):
        modules = make_modules(pages, calls, num_rows)
        saved = {name: sys.modules.get(name) for name in modules}
        sys.modules.update(modules)
        sys.path.insert(0, str(SCRIPT.parent))
        try:
            import journey_read

            with tempfile.TemporaryDirectory() as work:
                access = Path(work) / "access"
                secret = Path(work) / "secret"
                access.write_text("AK\n", encoding="utf-8")
                secret.write_text("SK\n", encoding="utf-8")
                answer = io.StringIO()
                with contextlib.redirect_stdout(answer):
                    code = journey_read.main(
                        [
                            "--endpoint",
                            "http://127.0.0.1:18333",
                            "--bucket",
                            "lakehouse",
                            "--needle",
                            "/e2e/exit_test/metadata/",
                            "--access-key-file",
                            str(access),
                            "--secret-key-file",
                            str(secret),
                            "--rows",
                            "3",
                        ]
                    )
                return code, answer.getvalue()
        finally:
            sys.path.remove(str(SCRIPT.parent))
            for name, module in saved.items():
                if module is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = module

    def test_newest_metadata_is_read_path_style(self):
        calls = []
        code, line = self.run_main(TWO_PAGES, 3, calls)
        self.assertEqual(code, 0)
        self.assertIn("3 rows from " + NEW_KEY + " (pyiceberg, no catalog)", line)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["location"], "s3://lakehouse/" + NEW_KEY)
        properties = calls[0]["properties"]
        self.assertEqual(properties["s3.path-style-access"], "true")
        self.assertEqual(properties["s3.endpoint"], "http://127.0.0.1:18333")

    def test_row_count_mismatch_is_a_failure(self):
        calls = []
        code, line = self.run_main(TWO_PAGES, 2, calls)
        self.assertEqual(code, 1)
        self.assertIn("has 2 rows, not 3", line)
        self.assertEqual(len(calls), 1)

    def test_tied_timestamps_pick_the_newest_version(self):
        # SeaweedFS reports LastModified at second granularity: a create and an append land in the
        # same second, and the older metadata must not win by listing order.
        tied = [
            {
                "Contents": [
                    {"Key": OLD_KEY, "LastModified": NEWER},
                    {"Key": NEW_KEY, "LastModified": NEWER},
                ],
                "IsTruncated": False,
            }
        ]
        calls = []
        code, line = self.run_main(tied, 3, calls)
        self.assertEqual(code, 0)
        self.assertEqual(calls[0]["location"], "s3://lakehouse/" + NEW_KEY)


if __name__ == "__main__":
    unittest.main()
