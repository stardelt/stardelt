"""Tests for stardelt.dashboards: a Superset export becomes bundle files, without databases/."""

import io
import zipfile

import pytest

from stardelt import dashboards


class FakeNova:
    """The operations the exporter and the importer use; it records what it was asked for."""

    def __init__(self, body):
        self.body = body
        self.paths = []
        self.data = None

    def download(self, path):
        self.paths.append(path)
        return self.body

    def upload(self, path, data, content_type="application/zip"):
        self.paths.append((path, len(data), content_type))
        self.data = data
        return self.body


def archive(names):
    """A ZIP in memory whose members are ``names``, each with a little body."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        for name in names:
            zf.writestr(name, f"# {name}\n")
    return buffer.getvalue()


def test_the_export_writes_every_folder_but_databases(tmp_path):
    out = tmp_path / "shop"
    nova = FakeNova(
        archive(
            [
                "metadata.yaml",
                "dashboards/shop.yaml",
                "charts/sales.yaml",
                "datasets/orders.yaml",
                "databases/warehouse.yaml",
            ]
        )
    )

    target, written = dashboards.export(nova, 42, out, "shop", "analytics")

    assert target == out
    assert written == ["charts/sales.yaml", "dashboards/shop.yaml", "datasets/orders.yaml", "metadata.yaml"]
    for name in written:
        assert (out / name).read_text() == f"# {name}\n"
    assert not (out / "databases").exists()


def test_an_unsafe_path_writes_nothing(tmp_path):
    out = tmp_path / "shop"
    nova = FakeNova(archive(["metadata.yaml", "dashboards/../../evil.yaml"]))

    with pytest.raises(dashboards.ExportError, match="unsafe path"):
        dashboards.export(nova, 42, out, "shop", "analytics")

    assert not out.exists() or not any(out.rglob("*"))
    assert not (tmp_path / "evil.yaml").exists()


def test_an_archive_that_is_not_a_zip_is_a_sentence(tmp_path):
    with pytest.raises(dashboards.ExportError) as raised:
        dashboards.export(FakeNova(b"not a zip"), 42, tmp_path / "shop", "shop", "analytics")

    assert str(raised.value) == "Nova did not answer a Superset export (not a ZIP archive)"


def test_an_empty_or_foreign_archive_is_refused(tmp_path):
    with pytest.raises(dashboards.ExportError) as raised:
        dashboards.export(FakeNova(archive(["databases/warehouse.yaml"])), 42, tmp_path / "shop", "shop", "analytics")

    assert str(raised.value) == "the archive has no dashboards/ folder; is 42 a dashboard?"


def test_the_download_path_names_the_nova_operation(tmp_path):
    nova = FakeNova(archive(["dashboards/shop.yaml"]))

    dashboards.export(nova, 42, tmp_path / "shop", "shop", "analytics")

    assert nova.paths == ["/api/dashboards/42/export?bundle=shop&ownerGroup=analytics"]


def test_a_bundle_identity_is_read_from_bundle_yaml(tmp_path):
    (tmp_path / "bundle.yaml").write_text("bundle:\n  name: shop\n  ownerGroup: analytics\n")

    assert dashboards.bundle_identity(tmp_path) == ("shop", "analytics")


def test_exporting_outside_a_bundle_is_a_sentence(tmp_path):
    with pytest.raises(dashboards.ExportError, match="no bundle.yaml here"):
        dashboards.bundle_identity(tmp_path)


def test_import_zips_the_folder_and_posts_the_query(tmp_path):
    folder = tmp_path / "shop"
    (folder / "dashboards").mkdir(parents=True)
    (folder / "dashboards" / "shop.yaml").write_text("# shop\n")
    (folder / "charts").mkdir()
    (folder / "charts" / "sales.yaml").write_text("# sales\n")
    nova = FakeNova({"databases": ["warehouse"], "result": {"ok": True}})

    answer = dashboards.import_folder(
        nova, folder, bundle="shop", owner_group="analytics", target="prod"
    )

    assert answer == {"databases": ["warehouse"], "result": {"ok": True}}
    ((path, size, content_type),) = nova.paths
    assert path == "/api/dashboards/import?bundle=shop&ownerGroup=analytics&target=prod"
    assert content_type == "application/zip"
    assert size == len(nova.data)
    with zipfile.ZipFile(io.BytesIO(nova.data)) as bundle:
        assert bundle.namelist() == ["charts/sales.yaml", "dashboards/shop.yaml"]
        assert bundle.read("dashboards/shop.yaml") == b"# shop\n"


def test_overwrite_and_commit_reach_the_query(tmp_path):
    folder = tmp_path / "shop"
    (folder / "dashboards").mkdir(parents=True)
    (folder / "dashboards" / "shop.yaml").write_text("# shop\n")
    nova = FakeNova({"databases": [], "result": {}})

    dashboards.import_folder(
        nova,
        folder,
        bundle="shop",
        owner_group="analytics",
        target="prod",
        commit="abc123",
        overwrite=True,
    )

    ((path, _, _),) = nova.paths
    assert path == (
        "/api/dashboards/import?bundle=shop&ownerGroup=analytics&target=prod"
        "&commit=abc123&overwrite=true"
    )


def test_a_folder_without_dashboards_is_a_sentence(tmp_path):
    folder = tmp_path / "shop"
    (folder / "charts").mkdir(parents=True)
    (folder / "charts" / "sales.yaml").write_text("# sales\n")

    with pytest.raises(dashboards.UploadError) as raised:
        dashboards.import_folder(
            FakeNova({}), folder, bundle="shop", owner_group="analytics", target="prod"
        )

    assert str(raised.value) == f"{folder} has no dashboards/ folder to import"
