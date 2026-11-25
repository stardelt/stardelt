"""The `trino-warehouses` seed is one document in three copies (#1615).

`resource-groups.json` is created before the first access sync runs — by
`platform/Makefile` (`trino-warehouse-configmaps`) on a `make` install and by the
operator (`access_sync.rs::warehouses_config_map`) on a `kubectl apply` install. An
empty file is not valid JSON, so a fresh coordinator crash-loops on it
(`No content to map due to end-of-input`) until the sync succeeds. Both paths now seed
the same document: `render_resource_groups([])` — the admin group and the
zero-concurrency `no-warehouse`.

Three copies exist and must not drift (D44, the twins rule):

* `platform/manifests/trino-warehouses-seed.json` — what the Makefile mounts;
* `operator/crates/stardelt-operator/src/access_sync.rs` — `SEED_RESOURCE_GROUPS`;
* `scripts/access-sync-job.py` — `render_resource_groups([])`, the source of both.

Standard library only, like its neighbours: `python3 -m unittest discover -s
scripts/tests -v` runs it in CI.
"""
import importlib.util
import json
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
JOB = PLATFORM / "scripts" / "access-sync-job.py"
SEED = PLATFORM / "manifests" / "trino-warehouses-seed.json"
OPERATOR = PLATFORM.parent / "operator" / "crates" / "stardelt-operator" / "src" / "access_sync.rs"

_spec = importlib.util.spec_from_file_location("access_sync_job", JOB)
access_sync = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(access_sync)

CONSTANT = "SEED_RESOURCE_GROUPS"


def rendered():
    """The document the job renders with no warehouse — the seed by definition."""
    return access_sync.render_resource_groups([])


def rust_constant():
    """The `r#"…"#` value of `access_sync.rs::SEED_RESOURCE_GROUPS`."""
    source = OPERATOR.read_text()
    marker = "pub const %s: &str = r#\"" % CONSTANT
    start = source.find(marker)
    if start < 0:
        raise AssertionError("%s: no `%s` raw string" % (OPERATOR, CONSTANT))
    start += len(marker)
    end = source.find('"#;', start)
    if end < 0:
        raise AssertionError("%s: `%s` raw string is not terminated" % (OPERATOR, CONSTANT))
    return source[start:end]


class SeedIsTheRenderedDocument(unittest.TestCase):
    def test_the_manifest_file_equals_render_resource_groups_of_no_warehouse(self):
        self.assertEqual(SEED.read_text(), rendered())

    def test_the_rust_constant_equals_render_resource_groups_of_no_warehouse(self):
        self.assertEqual(rust_constant(), rendered())

    def test_the_seed_parses_as_json_with_root_groups_and_selectors(self):
        document = json.loads(SEED.read_text())
        self.assertTrue(document["rootGroups"])
        self.assertIn("selectors", document)
        names = [group["name"] for group in document["rootGroups"]]
        self.assertEqual(names, ["global", "admin", "no-warehouse"])
        no_warehouse = document["rootGroups"][names.index("no-warehouse")]
        self.assertEqual(no_warehouse["hardConcurrencyLimit"], 0)
        self.assertEqual(no_warehouse["maxQueued"], 0)


if __name__ == "__main__":
    unittest.main()
