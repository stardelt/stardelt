"""scripts/check_doc_commands.py: every `stardelt …` line in the docs' shell blocks names a command this CLI has."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).parent.parent


def checker():
    spec = importlib.util.spec_from_file_location("check_doc_commands", ROOT / "scripts" / "check_doc_commands.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def docs_with(tmp_path, body):
    (tmp_path / "page.md").write_text(body, encoding="utf-8")
    return tmp_path


def test_commands_shortcuts_options_and_placeholders_pass(tmp_path):
    docs = docs_with(
        tmp_path,
        "```bash\n"
        "$ stardelt bundle validate .\n"
        "stardelt --host https://nova.example whoami\n"
        "STARDELT_TOKEN=x stardelt sql 'select 1' | head\n"
        "stardelt pipelines get etl daily\n"
        "stardelt mcp serve \\\n"
        "  --help\n"
        "stardelt <command>\n"
        "```\n",
    )
    assert checker().check(docs) == ([], 6, 1)


def test_an_unknown_command_and_an_unknown_sub_command_are_named_with_their_line(tmp_path):
    docs = docs_with(tmp_path, "Text.\n\n```bash\nstardelt frobnicate now\n```\n\n```sh\nstardelt bundle frobnicate\n```\n")
    problems, seen, pages = checker().check(docs)
    assert problems == [
        f"{docs / 'page.md'}:4: stardelt has no command 'frobnicate'",
        f"{docs / 'page.md'}:8: stardelt bundle has no command 'frobnicate'",
    ]
    assert (seen, pages) == (2, 1)


def test_prose_and_blocks_that_are_not_shell_are_not_read(tmp_path):
    docs = docs_with(tmp_path, "Run `stardelt frobnicate` in prose.\n\n```python\nstardelt frobnicate\n```\n\n```yaml\nrun: stardelt frobnicate\n```\n")
    assert checker().check(docs) == ([], 0, 0)


def test_main_prints_each_problem_and_exits_1(tmp_path, capsys):
    docs = docs_with(tmp_path, "```bash\nstardelt frobnicate\n```\n")
    assert checker().main([str(docs)]) == 1
    assert "stardelt has no command 'frobnicate'" in capsys.readouterr().out
