"""Tests for stardelt.dashboards: a Superset export becomes bundle files, without databases/."""

import io
import zipfile

import pytest

from stardelt import dashboards


class FakeNova:
    """The one operation the exporter uses; it records the path it was asked for."""

    def __init__(self, body):
        self.body = body
        self.paths = []

    def download(self, path):
        self.paths.append(path)
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

    target, written = dashboards.export(nova, 42, out)

    assert target == out
    assert written == ["charts/sales.yaml", "dashboards/shop.yaml", "datasets/orders.yaml", "metadata.yaml"]
    for name in written:
        assert (out / name).read_text() == f"# {name}\n"
    assert not (out / "databases").exists()


def test_an_unsafe_path_writes_nothing(tmp_path):
    out = tmp_path / "shop"
    nova = FakeNova(archive(["metadata.yaml", "dashboards/../../evil.yaml"]))

    with pytest.raises(dashboards.ExportError, match="unsafe path"):
        dashboards.export(nova, 42, out)

    assert not out.exists() or not any(out.rglob("*"))
    assert not (tmp_path / "evil.yaml").exists()


def test_an_archive_that_is_not_a_zip_is_a_sentence(tmp_path):
    with pytest.raises(dashboards.ExportError) as raised:
        dashboards.export(FakeNova(b"not a zip"), 42, tmp_path / "shop")

    assert str(raised.value) == "Nova did not answer a Superset export (not a ZIP archive)"


def test_an_empty_or_foreign_archive_is_refused(tmp_path):
    with pytest.raises(dashboards.ExportError) as raised:
        dashboards.export(FakeNova(archive(["databases/warehouse.yaml"])), 42, tmp_path / "shop")

    assert str(raised.value) == "the archive has no dashboards/ folder; is 42 a dashboard?"


def test_the_download_path_names_the_nova_operation(tmp_path):
    nova = FakeNova(archive(["dashboards/shop.yaml"]))

    dashboards.export(nova, 42, tmp_path / "shop")

    assert nova.paths == ["/api/admin/dashboards/42/export"]
