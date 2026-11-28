"""`stardelt bundle dashboards`: a bundle's `dashboards:` folders imported for one target (#115 b)."""

import io
import zipfile

import yaml

from stardelt import api, bundle, cli

MANIFEST = """
bundle: {name: sales-etl, ownerGroup: sales}
variables:
  catalog: {default: sales_dev}
  unused: {default: x}
include: [resources/*.yaml]
files: [sql/*.sql, notebooks/*.ipynb]
requires: {connections: [crm-db]}
targets:
  dev:  {mode: development, variables: {catalog: sales_dev}}
  prod: {mode: production, variables: {catalog: sales}, runAs: pipeline-sales}
"""
LOAD = "name: load\nownerGroup: sales\nsqlFile: sql/load.sql\nschedule: {cron: '0 3 * * *', timezone: Europe/Berlin}\n"
NIGHTLY = "name: nightly\nsql: select * from ${var.catalog}.demo.trips\n"


class Nova:
    """The upload half of a Nova: it records every import and can refuse by folder slug.

    ``refuse`` holds folder names (``shop``); the upload path is the same for every folder, so
    the fake reads the slug from the archive's ``dashboards/<slug>.yaml``.
    """

    def __init__(self, answer=None, refuse=None):
        self.answer = answer or {"databases": ["Trino Gold"], "result": {"ok": True}}
        self.refuse = refuse or set()
        self.uploads = []

    def upload(self, path, data, content_type="application/zip"):
        self.uploads.append((path, len(data), content_type))
        names = zipfile.ZipFile(io.BytesIO(data)).namelist()
        if any(f"dashboards/{name}.yaml" in names for name in self.refuse):
            raise api.StardeltApiError(502, "Superset answered 500")
        return self.answer


def make(tmp_path, folders=("dashboards/shop",)):
    """A bundle that passes `bundle.validate`, with each folder written as an export."""
    block = yaml.safe_dump(list(folders), sort_keys=False)
    (tmp_path / "bundle.yaml").write_text(MANIFEST + "\ndashboards:\n" + "".join(
        "  " + line for line in block.splitlines(keepends=True)
    ), encoding="utf-8")
    (tmp_path / "resources").mkdir(exist_ok=True)
    for name, text in {"load.yaml": LOAD, "nightly.yaml": NIGHTLY}.items():
        (tmp_path / "resources" / name).write_text(text)
    for name, text in {"sql/load.sql": "select 1", "notebooks/explore.ipynb": "{}"}.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    for folder in folders:
        target = tmp_path / folder
        (target / "dashboards").mkdir(parents=True, exist_ok=True)
        (target / "metadata.yaml").write_text("version: 1.0.0\n")
        slug = folder.rsplit("/", 1)[-1]
        (target / "dashboards" / f"{slug}.yaml").write_text(f"dashboard_title: {slug}\n")
    return tmp_path


def test_a_folder_without_a_dashboards_child_is_a_problem(tmp_path):
    directory = make(tmp_path, folders=())
    (directory / "dashboards" / "shop").mkdir(parents=True)
    (directory / "dashboards" / "shop" / "metadata.yaml").write_text("version: 1.0.0\n")
    (directory / "bundle.yaml").write_text(MANIFEST + "\ndashboards:\n  - dashboards/shop\n", encoding="utf-8")
    nova = Nova()

    problems, rows = bundle.import_dashboards(nova, directory, "prod")

    assert problems == [("dashboards/shop", "has no dashboards/*.yaml; is this a Superset export?")]
    assert rows == []
    assert nova.uploads == []


def test_a_folder_that_holds_databases_is_refused(tmp_path):
    directory = make(tmp_path)
    (directory / "dashboards" / "shop" / "databases").mkdir()
    (directory / "dashboards" / "shop" / "databases" / "warehouse.yaml").write_text("sqlalchemy_uri: x\n")
    nova = Nova()

    problems, rows = bundle.import_dashboards(nova, directory, "prod")

    assert problems == [("dashboards/shop", "holds databases/; a bundle never stores the connection")]
    assert rows == []
    assert nova.uploads == []


def test_a_dashboards_entry_outside_the_bundle_is_a_problem(tmp_path):
    directory = make(tmp_path)
    (directory / "bundle.yaml").write_text(MANIFEST + "\ndashboards:\n  - ../shop\n", encoding="utf-8")
    nova = Nova()

    problems, rows = bundle.import_dashboards(nova, directory, "prod")

    assert problems == [("../shop", "a folder must stay inside the bundle (no absolute path, no ..)")]
    assert rows == []
    assert nova.uploads == []


def test_the_command_uploads_each_folder_once_with_the_bundle_and_target(tmp_path, monkeypatch):
    directory = make(tmp_path, folders=("dashboards/shop", "dashboards/ops"))
    nova = Nova()
    monkeypatch.setattr(api, "Client", lambda **kwargs: nova)
    out, err = io.StringIO(), io.StringIO()

    code = cli.main(
        ["bundle", "dashboards", str(directory), "-t", "prod"],
        environ={"STARDELT_NOVA_URL": "https://nova.example", "STARDELT_TOKEN": "t"},
        stdout=out, stderr=err, session=object(),
    )

    assert code == 0
    assert len(nova.uploads) == 2
    for path, size, content_type in nova.uploads:
        assert path.startswith("/api/dashboards/import?bundle=")
        assert "ownerGroup=sales&target=prod" in path
        assert "overwrite=true" not in path
        assert content_type == "application/zip"
        assert size > 0
    assert "bundle=sales-etl&ownerGroup=sales&target=prod" in nova.uploads[0][0]
    assert out.getvalue() == (
        "dashboards/ops  imported 1 file(s); databases: Trino Gold\n"
        "dashboards/shop  imported 1 file(s); databases: Trino Gold\n"
    )
    assert err.getvalue() == ""

    nova.uploads.clear()
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(
        ["bundle", "dashboards", str(directory), "-t", "prod", "--overwrite"],
        environ={"STARDELT_NOVA_URL": "https://nova.example", "STARDELT_TOKEN": "t"},
        stdout=out, stderr=err, session=object(),
    )
    assert code == 0
    assert len(nova.uploads) == 2
    for path, _size, _content_type in nova.uploads:
        assert "ownerGroup=sales&target=prod&overwrite=true" in path


def test_a_nova_refusal_is_a_problem_and_the_other_folder_still_imports(tmp_path):
    directory = make(tmp_path, folders=("dashboards/shop", "dashboards/ops"))
    nova = Nova(refuse={"shop"})

    problems, rows = bundle.import_dashboards(nova, directory, "prod")

    assert problems == [("dashboards/shop", "Superset answered 500 (HTTP 502)")]
    assert rows == [
        {
            "folder": "dashboards/ops",
            "dashboards": 1,
            "databases": ["Trino Gold"],
            "action": "imported",
        }
    ]
    assert len(nova.uploads) == 2


def test_bundle_validate_accepts_the_key(tmp_path):
    directory = make(tmp_path)

    problems, _warnings, _resolved = bundle.validate(directory)

    assert problems == []
