"""`stardelt bundle evals`: the questions with known answers a bundle's `evals:` block stores (#125)."""

import io

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

QUESTION = "How many customers have placed more than one order?"
EXPECTED = {"kind": "value", "value": 42}


class Nova:
    """A Nova that answers `catalog_eval_list` from a list and records every call."""

    def __init__(self, stored=None):
        self.stored = list(stored or [])  # rows: {"id", "ownerGroup", "question", "expected"}
        self.calls = []

    def call(self, op, path=None, query=None, body=None, headers=None):
        self.calls.append((op, path, query, body))
        if op == "catalog_eval_list":
            return list(self.stored)
        if op == "catalog_eval_create":
            row = {"id": 700 + len(self.stored), **body}
            self.stored.append(row)
            return row
        if op == "eval_update":
            for row in self.stored:
                if row["id"] == path["id"]:
                    row.update(body)
                    return row
            raise AssertionError(f"no row {path['id']}")
        raise AssertionError(op)

    def writes(self):
        return [call for call in self.calls if call[0] in ("catalog_eval_create", "eval_update")]


def make(tmp_path, questions=None, manifest=MANIFEST, catalog="playground"):
    """A full bundle (it passes `bundle.validate`) whose `evals:` block holds `questions`."""
    questions = questions if questions is not None else [{"question": QUESTION, "expected": EXPECTED}]
    block = yaml.safe_dump(
        {"catalog": catalog, "questions": questions}, sort_keys=False, allow_unicode=True
    )
    block = "\nevals:\n" + "".join("  " + line for line in block.splitlines(keepends=True))
    (tmp_path / "bundle.yaml").write_text(manifest + block, encoding="utf-8")
    (tmp_path / "resources").mkdir(exist_ok=True)
    for name, text in {"load.yaml": LOAD, "nightly.yaml": NIGHTLY}.items():
        (tmp_path / "resources" / name).write_text(text)
    for name, text in {"sql/load.sql": "select 1", "notebooks/explore.ipynb": "{}"}.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return tmp_path


def test_a_question_the_catalog_lacks_is_created_with_the_bundles_group(tmp_path):
    directory = make(tmp_path, questions=[{"question": f"  {QUESTION}  ", "expected": EXPECTED}])
    nova = Nova([])
    problems, rows = bundle.apply_evals(nova, directory)
    assert problems == []
    assert rows == [{"question": QUESTION, "action": "created"}]
    assert nova.writes() == [
        (
            "catalog_eval_create",
            None,
            {"catalog": "playground"},
            {"ownerGroup": "sales", "question": QUESTION, "expected": EXPECTED},
        )
    ]


def test_a_correct_question_is_unchanged_and_writes_nothing(tmp_path):
    directory = make(tmp_path)
    nova = Nova([{"id": 7, "ownerGroup": "sales", "question": QUESTION, "expected": EXPECTED}])
    problems, rows = bundle.apply_evals(nova, directory)
    assert problems == []
    assert rows == [{"question": QUESTION, "action": "unchanged"}]
    assert nova.writes() == []


def test_a_different_expected_is_corrected_in_place(tmp_path):
    directory = make(tmp_path)
    nova = Nova([{"id": 7, "ownerGroup": "sales", "question": QUESTION, "expected": {"kind": "value", "value": 1}}])
    problems, rows = bundle.apply_evals(nova, directory)
    assert problems == []
    assert rows == [{"question": QUESTION, "action": "corrected"}]
    assert nova.writes() == [
        ("eval_update", {"id": 7}, None, {"question": QUESTION, "expected": EXPECTED}),
    ]
    assert nova.stored[0]["question"] == QUESTION


def test_check_answers_would_create_and_would_update_and_writes_nothing(tmp_path, monkeypatch):
    second = "Which table holds the orders?"
    directory = make(
        tmp_path,
        questions=[{"question": QUESTION, "expected": EXPECTED}, {"question": second, "expected": {"kind": "value", "value": 3}}],
    )
    nova = Nova([{"id": 7, "ownerGroup": "sales", "question": second, "expected": {"kind": "value", "value": 1}}])
    problems, rows = bundle.apply_evals(nova, directory, check=True)
    assert problems == []
    assert rows == [
        {"question": QUESTION, "action": "would-create"},
        {"question": second, "action": "would-update"},
    ]
    assert nova.writes() == []
    monkeypatch.setattr(api, "Client", lambda **kwargs: nova)
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(
        ["bundle", "evals", str(directory), "--check"],
        environ={"STARDELT_NOVA_URL": "https://nova.example", "STARDELT_TOKEN": "t"},
        stdout=out, stderr=err, session=object(),
    )
    assert code == 1
    assert out.getvalue() == f"{QUESTION}  would-create\n{second}  would-update\n"
    assert nova.writes() == []


def test_a_question_owned_by_another_group_is_a_problem(tmp_path):
    directory = make(tmp_path)
    nova = Nova([{"id": 7, "ownerGroup": "other", "question": QUESTION, "expected": EXPECTED}])
    problems, rows = bundle.apply_evals(nova, directory)
    assert rows == []
    assert len(problems) == 1
    assert problems[0][0] == "evals"
    assert "other" in problems[0][1]
    assert nova.writes() == []


def test_two_stored_rows_with_one_question_are_a_problem(tmp_path):
    directory = make(tmp_path)
    nova = Nova(
        [
            {"id": 7, "ownerGroup": "sales", "question": QUESTION, "expected": EXPECTED},
            {"id": 8, "ownerGroup": "sales", "question": QUESTION, "expected": EXPECTED},
        ]
    )
    problems, rows = bundle.apply_evals(nova, directory)
    assert rows == []
    assert len(problems) == 1
    assert "twice" in problems[0][1]
    assert nova.writes() == []


def test_an_expected_outside_the_three_shapes_is_refused_offline(tmp_path):
    directory = make(tmp_path, questions=[{"question": QUESTION, "expected": {"kind": "rows", "rows": []}}])
    nova = Nova([])
    problems, rows = bundle.apply_evals(nova, directory)
    assert rows == []
    assert problems == [
        (
            "evals.questions[0].expected",
            "expected.rows must hold 1 to 100 rows of 1 to 50 values, each a string of at most 500 characters, a number, a boolean or null",
        )
    ]
    assert nova.calls == []
