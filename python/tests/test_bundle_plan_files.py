"""`stardelt bundle plan --format markdown` shows the files and how they compare with the newest applied record (#104).

The ticket's expectations list only the `files:` entry; `_digests` also returns every `include:` match, so
`resources/load.yaml` is a bundle file as well. It appears in the expected rows below (stardelt/stardelt#657).
"""

from stardelt import bundle

MANIFEST = """
bundle: {name: sales-etl, ownerGroup: sales}
include: [resources/*.yaml]
files: [sql/*.sql]
targets:
  dev:  {mode: development}
  prod: {mode: production, runAs: pipeline-sales}
"""
PIPELINE = "name: load\nownerGroup: sales\nsqlFile: sql/load.sql\n"  # the file is created by the test (new)


class Nova:
    """A Nova that answers one ``deployments_list`` page and remembers the query."""

    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def call(self, op, path=None, query=None, body=None, headers=None):
        assert op == "deployments_list"
        self.queries.append(query)
        return self.rows


def make(tmp_path, files=None):
    (tmp_path / "bundle.yaml").write_text(MANIFEST)
    (tmp_path / "resources").mkdir(exist_ok=True)
    (tmp_path / "resources" / "load.yaml").write_text(PIPELINE)
    for name, text in (files if files is not None else {"sql/load.sql": "select 1"}).items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return tmp_path


def test_without_a_record_every_file_is_new(tmp_path):
    make(tmp_path)
    nova = Nova([])
    files, note = bundle.plan_files(nova, tmp_path, "prod")
    assert note == ""
    assert nova.queries == [{"bundle": "sales-etl", "target": "prod"}]
    assert [(f["path"], f["state"]) for f in files] == [
        ("resources/load.yaml", "new"), ("sql/load.sql", "new"),
    ]
    assert all(len(f["sha256"]) == 64 for f in files)


def test_a_record_marks_each_file_new_changed_or_unchanged(tmp_path):
    make(tmp_path, files={"sql/a.sql": "select 1", "sql/b.sql": "select 2", "sql/c.sql": "select 3"})  # created by the test (new)
    import hashlib
    b = hashlib.sha256(b"select 2").hexdigest()
    old = hashlib.sha256(b"select 2 but older").hexdigest()
    nova = Nova([
        {"status": "failed", "files": [{"path": "sql/a.sql", "sha256": "x" * 64}]},
        {"status": "applied", "files": [
            {"path": "sql/a.sql", "sha256": hashlib.sha256(b"select 1").hexdigest()},
            {"path": "sql/b.sql", "sha256": old},
        ]},
    ])
    files, note = bundle.plan_files(nova, tmp_path, "prod")
    assert note == ""
    assert [(f["path"], f["state"]) for f in files] == [
        ("resources/load.yaml", "new"),
        ("sql/a.sql", "unchanged"), ("sql/b.sql", "changed"), ("sql/c.sql", "new"),
    ]


def test_the_markdown_carries_a_files_table(tmp_path):
    make(tmp_path)
    files, _ = bundle.plan_files(Nova([]), tmp_path, "prod")
    text = bundle.plan_markdown("prod", [{"action": "would-create", "name": "load"}], files)
    assert "### Bundle files · `prod`" in text
    assert "| File | sha256 | State |" in text
    load = next(f for f in files if f["path"] == "sql/load.sql")
    assert f"| sql/load.sql | {load['sha256'][:12]} | new |" in text
    assert bundle.plan_markdown("prod", []) == (
        "### Bundle plan · `prod`\n\n| Action | Kind | Name | Notes |\n|---|---|---|---|\n"
    )


def test_a_stream_and_a_migration_carry_their_kind():
    text = bundle.plan_markdown("prod", [
        {"action": "would-create", "name": "load", "kind": "Pipeline"},
        {"action": "would-create", "name": "feed", "kind": "StreamApp"},
        {"action": "would-apply-migration", "name": "001_init.sql", "class": "additive", "kind": "Migration"},
    ])
    assert "| would-create | StreamApp | feed | — |" in text
    assert "| would-apply-migration | Migration | 001_init.sql | — |" in text
