"""A Superset dashboard as the files of a bundle: the unzipped export, without the connection.

A Superset export is a ZIP with ``metadata.yaml``, ``dashboards/``, ``charts/``, ``datasets/`` and
``databases/``. A bundle keeps the unzipped export in Git, and never ``databases/``: that folder
carries the target's connection URI. So this module writes every member of the archive except the
``databases/`` folder, and never ``extractall``: it refuses an unsafe member name before it writes
anything.

The caller's own Nova sign-in is the authority for the download; no token is stored in these
files.
"""

import io
import pathlib
import shutil
import zipfile

NAME = "databases"

CHUNK = 64 * 1024


class ExportError(RuntimeError):
    """The archive is not something a bundle can store. The message is a sentence for the user."""


def _target(out, name):
    """Where ``name`` of the archive goes, or ``ExportError`` when the name is unsafe."""
    member = pathlib.PurePosixPath(name)
    if member.is_absolute() or ".." in member.parts:
        raise ExportError(f"the archive has an unsafe path: {name}")
    target = (out / name).resolve()
    root = out.resolve()
    if target != root and root not in target.parents:
        raise ExportError(f"the archive has an unsafe path: {name}")
    return out / name


def export(nova, dashboard_id, out):
    """Write the Superset dashboard ``dashboard_id`` as the files of a bundle under ``out``.

    Returns ``(out, written)``: the folder and the sorted relative paths written. The
    ``databases/`` folder of the archive is never written.
    """
    number = int(dashboard_id)
    if number < 1:
        raise ValueError("the dashboard id must be a positive integer")
    archive = nova.download(f"/api/admin/dashboards/{number}/export")
    try:
        bundle = zipfile.ZipFile(io.BytesIO(archive))
    except zipfile.BadZipFile:
        raise ExportError("Nova did not answer a Superset export (not a ZIP archive)") from None

    out = pathlib.Path(out)
    members = []
    with bundle:
        for member in bundle.infolist():
            if member.is_dir():
                continue
            if pathlib.PurePosixPath(member.filename).parts[0] == NAME:
                continue
            _target(out, member.filename)
            members.append(member)
        out.mkdir(parents=True, exist_ok=True)
        for member in members:
            target = out / member.filename
            target.parent.mkdir(parents=True, exist_ok=True)
            with bundle.open(member) as source, target.open("wb") as sink:
                shutil.copyfileobj(source, sink, CHUNK)
    written = sorted(str(pathlib.PurePosixPath(member.filename)) for member in members)
    if not written:
        raise ExportError(f"the archive has no dashboards/ folder; is {number} a dashboard?")
    return out, written


def summary(out, written):
    """The one line the command line prints after a successful export."""
    return f"wrote {len(written)} file(s) to {out}; databases/ is not stored"
