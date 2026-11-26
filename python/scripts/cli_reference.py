"""Write stardelt/cli.json: every command of the `stardelt` CLI, as the parser defines it.

    python scripts/cli_reference.py          # rewrite stardelt/cli.json
    python scripts/cli_reference.py --check  # exit 1 when stardelt/cli.json is stale

The docs render this file as the command-line reference, so the reference lists exactly the commands
and options of the release it describes (stardelt/stardelt-docs#1145). It is generated from
`stardelt.cli._parser()`; never edit it by hand. A test fails when it is stale.
"""

import argparse
import json
import pathlib
import sys

from stardelt import cli

OUT = pathlib.Path(__file__).resolve().parents[1] / "stardelt" / "cli.json"


def _option(action):
    entry = {"flags": list(action.option_strings), "help": action.help or ""}
    if action.metavar:
        entry["metavar"] = action.metavar if isinstance(action.metavar, str) else " ".join(action.metavar)
    if action.choices:
        entry["choices"] = [str(c) for c in action.choices]
    if action.required:
        entry["required"] = True
    if action.nargs in (0,) or isinstance(action, (argparse._StoreTrueAction, argparse._StoreFalseAction)):
        entry["flag"] = True
    if isinstance(action, argparse._AppendAction):
        entry["repeatable"] = True
    if action.default not in (None, False, argparse.SUPPRESS) and not isinstance(action.default, (list, dict)):
        entry["default"] = str(action.default)
    return entry


def _argument(action):
    entry = {"name": action.metavar or action.dest, "help": action.help or ""}
    if action.choices:
        entry["choices"] = [str(c) for c in action.choices]
    if action.nargs in ("?", "*"):
        entry["optional"] = True
    if action.nargs in ("*", "+", argparse.REMAINDER):
        entry["many"] = True
    return entry


def describe(parser, name, help_text):
    node = {"name": name, "help": help_text or "", "arguments": [], "options": [], "commands": []}
    for action in parser._actions:
        if isinstance(action, argparse._HelpAction):
            continue
        if isinstance(action, argparse._SubParsersAction):
            helps = {choice.dest: choice.help for choice in action._choices_actions}
            for sub_name, sub in action.choices.items():
                node["commands"].append(describe(sub, sub_name, helps.get(sub_name, "")))
        elif action.option_strings:
            node["options"].append(_option(action))
        else:
            node["arguments"].append(_argument(action))
    return node


def document():
    parser = cli._parser()
    root = describe(parser, parser.prog, parser.description)
    return {"generated_by": "python/scripts/cli_reference.py", **root}


def main(argv=None):
    args = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    args.add_argument("--check", action="store_true", help="exit 1 when stardelt/cli.json is stale")
    check = args.parse_args(argv).check
    text = json.dumps(document(), indent=2, ensure_ascii=False) + "\n"
    if check:
        if not OUT.exists() or OUT.read_text(encoding="utf-8") != text:
            print("stardelt/cli.json is stale: run python scripts/cli_reference.py and commit it", file=sys.stderr)
            return 1
        print("stardelt/cli.json is current")
        return 0
    OUT.write_text(text, encoding="utf-8")
    print(f"wrote {OUT.relative_to(OUT.parents[1])}: {count(document())} commands")
    return 0


def count(node):
    return sum(1 + count(sub) for sub in node["commands"])


if __name__ == "__main__":
    sys.exit(main())
