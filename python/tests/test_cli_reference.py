"""stardelt/cli.json lists every command of the CLI and is never stale (scripts/cli_reference.py)."""

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).parent.parent


def reference():
    spec = importlib.util.spec_from_file_location("cli_reference", ROOT / "scripts" / "cli_reference.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_committed_cli_json_is_current(capsys):
    assert reference().main(["--check"]) == 0, capsys.readouterr().err


def test_every_command_and_sub_command_is_listed():
    doc = json.loads((ROOT / "stardelt" / "cli.json").read_text(encoding="utf-8"))
    names = {c["name"]: {s["name"] for s in c["commands"]} for c in doc["commands"]}
    assert {"login", "doctor", "sql", "bundle", "samples", "mcp"} <= set(names)
    assert {"init", "validate", "plan", "deploy", "rollback", "generate"} <= names["bundle"]


def test_options_carry_their_flags_choices_and_defaults():
    doc = reference().document()
    sql = next(c for c in doc["commands"] if c["name"] == "sql")
    fmt = next(o for o in sql["options"] if "--format" in o["flags"])
    assert fmt["choices"] == ["table", "json", "csv"] and fmt["default"] == "table"
    assert next(o for o in sql["options"] if "--long" in o["flags"])["flag"] is True
