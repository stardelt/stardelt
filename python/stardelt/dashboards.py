"""A Superset dashboard as the files of a bundle: the unzipped export, without the connection.

A Superset export is a ZIP with ``metadata.yaml``, ``dashboards/``, ``charts/``, ``datasets/`` and
``databases/``, all wrapped in one ``dashboard_export_<timestamp>/`` folder. A bundle keeps the
unzipped export in Git and never ``databases/``: that folder carries the target's connection URI.
So this module writes every member of the archive without the single wrapper folder and drops a
``databases`` segment anywhere, and never ``extractall``: it refuses an unsafe member name before it
writes anything.

The caller's own Nova sign-in is the authority for the download; no token is stored in these
files.
"""

import hashlib
import io
import pathlib
import shutil
import urllib.parse
import zipfile

import yaml

NAME = "databases"

CHUNK = 64 * 1024

MAX_UPLOAD_BYTES = 16 * 1024 * 1024  # the route's own limit (413 above it)


class ExportError(RuntimeError):
    """The archive is not something a bundle can store. The message is a sentence for the user."""


def _top(members):
    """The folder Superset wraps an export in (``dashboard_export_*/``), or "".

    Only a shared first segment that holds ``metadata.yaml`` is a wrapper: an archive of one
    folder named ``databases/`` must not have it stripped, or its folder would be written back.
    """
    shared = None
    names = set()
    for member in members:
        if member.is_dir():
            continue
        names.add(member.filename)
        parts = pathlib.PurePosixPath(member.filename).parts
        if len(parts) < 2:
            return ""
        if shared is None:
            shared = parts[0]
        elif shared != parts[0]:
            return ""
    if shared and f"{shared}/metadata.yaml" in names:
        return shared
    return ""


def _relative(member, top):
    """``member``'s path without ``top/``, the single folder Superset wraps an export in."""
    name = pathlib.PurePosixPath(member.filename)
    if top and name.parts and name.parts[0] == top:
        return pathlib.PurePosixPath(*name.parts[1:])
    return name


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


def normalized_hash(folder):
    """The sha256 of an export folder as Nova stamps it (#2306): the wrapper, the export minute
    and the certification stamp do not change it.

    The rule matches `nova/backend/src/superset_archive.rs::normalized_hash`: sorted relative
    paths, `databases/` dropped, one shared top folder stripped, `metadata.yaml`'s `timestamp:`
    line dropped, and the two stamp lines of every `dashboards/*.yaml` dropped.
    """
    folder = pathlib.Path(folder)
    files = sorted(path for path in folder.rglob("*") if path.is_file())
    names = [path.relative_to(folder).as_posix() for path in files]
    top = _shared_top(names)
    digest = hashlib.sha256()
    for path, name in zip(files, names):
        relative = name[len(top) + 1:] if top else name
        if "databases" in pathlib.PurePosixPath(relative).parts:
            continue
        data = path.read_bytes()
        if relative == "metadata.yaml":
            data = _without_lines(data, (b"timestamp:",))
        elif relative.startswith("dashboards/") and relative.endswith(".yaml"):
            data = _without_lines(data, (b"certified_by:", b"certification_details:"))
        digest.update(relative.encode() + b"\0" + data + b"\0")
    return digest.hexdigest()


def _shared_top(names):
    """The one top folder Superset wraps an export in, or "" (the Rust `top` rule)."""
    if not names or any("/" not in name for name in names):
        return ""
    shared = {name.split("/", 1)[0] for name in names}
    if len(shared) != 1:
        return ""
    top = shared.pop()
    return top if f"{top}/metadata.yaml" in names else ""


def _without_lines(data, prefixes):
    """`data` without every line that starts with one of `prefixes`, byte for byte."""
    return b"".join(
        line for line in data.splitlines(keepends=True) if not line.startswith(prefixes)
    )


def bundle_identity(directory="."):
    """``(name, owner_group)`` from ``bundle.yaml`` in ``directory``; ``ExportError`` otherwise.

    ``stardelt dashboards export`` runs inside the bundle it writes into, so the route's guard
    reads the same name and group the deploy will use.
    """
    manifest = pathlib.Path(directory) / "bundle.yaml"
    if not manifest.is_file():
        raise ExportError("run this inside a bundle: no bundle.yaml here")
    try:
        document = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    except (yaml.YAMLError, OSError) as err:
        raise ExportError("bundle.yaml cannot be read: %s" % str(err).splitlines()[0]) from None
    head = document.get("bundle") if isinstance(document, dict) else None
    name = head.get("name") if isinstance(head, dict) else None
    owner = head.get("ownerGroup") if isinstance(head, dict) else None
    if not isinstance(name, str) or not name:
        raise ExportError("bundle.yaml names no bundle.name")
    if not isinstance(owner, str) or not owner:
        raise ExportError("bundle.yaml names no bundle.ownerGroup")
    return name, owner


EXPORT_ROOTS = ("dashboards", "charts", "datasets", "databases")


def _remove_stale(out, keep):
    """Delete export files the new archive does not carry; leave every other file. Returns their paths."""
    removed = []
    if out.is_dir():
        for path in sorted(out.rglob("*")):
            relative = path.relative_to(out).as_posix()
            if not path.is_file() or relative in keep:
                continue
            if relative == "metadata.yaml" or relative.split("/", 1)[0] in EXPORT_ROOTS:
                path.unlink()
                removed.append(relative)
        for path in sorted(out.rglob("*"), reverse=True):
            if path.is_dir() and not any(path.iterdir()):
                path.rmdir()
    return removed


def export(nova, dashboard_id, out, bundle, owner_group):
    """Write the Superset dashboard ``dashboard_id`` as the files of a bundle under ``out``.

    Returns ``(out, written, removed)``: the folder, the sorted relative paths written, and the
    stale export files the previous export left that this one removed. The ``databases/`` folder
    of the archive is never written.
    """
    number = int(dashboard_id)
    if number < 1:
        raise ValueError("the dashboard id must be a positive integer")
    query = urllib.parse.urlencode({"bundle": bundle, "ownerGroup": owner_group})
    archive = nova.download("/api/dashboards/%d/export?%s" % (number, query))
    try:
        bundle = zipfile.ZipFile(io.BytesIO(archive))
    except zipfile.BadZipFile:
        raise ExportError("Nova did not answer a Superset export (not a ZIP archive)") from None

    out = pathlib.Path(out)
    if (out / "bundle.yaml").is_file():
        raise ExportError(f"{out} is a bundle root; export into a folder of its own, for example dashboards/<slug>")
    top = _top(bundle.infolist())
    members = []
    with bundle:
        for member in bundle.infolist():
            if member.is_dir():
                continue
            relative = _relative(member, top)
            if NAME in pathlib.PurePosixPath(member.filename).parts or NAME in relative.parts:
                continue
            _target(out, relative.as_posix())
            members.append((member, relative))
        keep = {relative.as_posix() for _member, relative in members}
        written = sorted(keep)
        if not written:
            raise ExportError(f"the archive has no dashboards/ folder; is {number} a dashboard?")
        removed = _remove_stale(out, keep)
        out.mkdir(parents=True, exist_ok=True)
        for member, relative in members:
            target = out / relative.as_posix()
            target.parent.mkdir(parents=True, exist_ok=True)
            with bundle.open(member) as source, target.open("wb") as sink:
                shutil.copyfileobj(source, sink, CHUNK)
    return out, written, removed


def summary(out, written, removed):
    """The one line the command line prints after a successful export."""
    return f"wrote {len(written)} file(s) to {out}; removed {len(removed)} stale export file(s); databases/ is not stored"


class UploadError(RuntimeError):
    """The folder cannot be imported. The message is a sentence for the user."""


def import_folder(nova, folder, *, bundle, owner_group, target, commit="", overwrite=False):
    """Zip ``folder`` and import it as a bundle's dashboards for ``target``.

    Returns Nova's answer: ``{"databases": [stems], "result": Superset's import result}``.
    """
    folder = pathlib.Path(folder)
    for name, value in (("bundle", bundle), ("owner group", owner_group), ("target", target)):
        if not str(value or "").strip():
            raise ValueError(f"the {name} is required")
    if not folder.is_dir():
        raise UploadError(f"{folder} is not a directory")
    files = sorted(path for path in folder.rglob("*") if path.is_file())
    if not any(path.relative_to(folder).parts[0] == "dashboards" for path in files):
        raise UploadError(f"{folder} has no dashboards/ folder to import")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            archive.write(path, path.relative_to(folder).as_posix())
    if buffer.tell() > MAX_UPLOAD_BYTES:
        raise UploadError(f"the archive is larger than 16 MiB ({buffer.tell()} bytes)")
    query = {"bundle": bundle.strip(), "ownerGroup": owner_group.strip(), "target": target.strip()}
    if commit.strip():
        query["commit"] = commit.strip()
    if overwrite:
        query["overwrite"] = "true"
    return nova.upload(
        "/api/dashboards/import?" + urllib.parse.urlencode(query), buffer.getvalue()
    )


def import_summary(folder, target, answer):
    """The one line the command line prints after a successful import."""
    dashboards = len(list(pathlib.Path(folder).glob("dashboards/*.yaml")))
    databases = ", ".join(answer.get("databases") or []) or "none"
    return f"imported {dashboards} dashboard file(s) for {target}; databases: {databases}"
