"""Every `stardelt …` command in the docs' shell examples exists in this checkout's CLI.

    python scripts/check_doc_commands.py DOCS_DIR

Reads the fenced `bash`, `sh`, `shell` and `console` blocks of every Markdown page under DOCS_DIR and
follows the words after `stardelt` through the command tree of `stardelt.cli` and the resource
shortcuts of `stardelt.shortcuts`. A word the tree does not know is a problem: the page tells a reader
to run a command their release does not have (docs#1144 named `stardelt samples load` on the same day
v0.0.4 shipped without it). Values, options and placeholders (`<catalog>`, `$TABLE`) are not checked.
Prints one line per problem and exits 1, or prints `ok: …` and exits 0.
"""

import argparse
import pathlib
import re
import shlex
import sys

from stardelt import api, cli, shortcuts

SHELLS = {"bash", "sh", "shell", "console", "zsh"}
WORD = re.compile(r"[a-z][a-z0-9-]*\Z")
ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=")


def command_tree(parser):
    """Nested dict of sub-command names: {"bundle": {"init": {}, ...}, "whoami": {}, ...}."""
    tree = {}
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, sub in action.choices.items():
                tree[name] = command_tree(sub)
    return tree


def shell_lines(text):
    """(line number, command) for each command in a shell block; `\\` continuations are joined."""
    fence, start, pending = None, 0, ""
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if line.startswith("```"):
            fence = None if fence is not None else line[3:].split(" ")[0].strip().lower()
            pending = ""
            continue
        if fence not in SHELLS:
            continue
        if not pending:
            start = number
        if line.endswith("\\"):
            pending += line[:-1] + " "
            continue
        yield start, pending + line
        pending = ""


def segments(command):
    """The simple commands of one line: split at `|`, `&&`, `||` and `;`, prompt and assignments dropped."""
    command = command[2:] if command.startswith("$ ") else command
    for part in re.split(r"\|\||&&|[|;]", command):
        try:
            words = shlex.split(part, comments=True)
        except ValueError:
            continue
        while words and ASSIGNMENT.match(words[0]):
            words = words[1:]
        if words:
            yield words


def problem(words, tree, resources):
    """None when `stardelt WORDS…` names a known command, else what is wrong."""
    if words[:1] == ["--host"]:
        words = words[2:]
    if not words or not WORD.match(words[0]):
        return None
    first = words[0]
    if first in tree:
        node, path = tree[first], [first]
        for word in words[1:]:
            if not node or not WORD.match(word):
                return None
            if word not in node:
                return f"stardelt {' '.join(path)} has no command {word!r}"
            node, path = node[word], path + [word]
        return None
    if first in resources:
        verbs = resources[first]
        rest = words[1:]
        if not rest or not WORD.match(rest[0]):
            return None
        if rest[0] in verbs or "" in verbs:
            return None
        return f"stardelt {first} has no verb {rest[0]!r}"
    return f"stardelt has no command {first!r}"


def check(docs):
    tree = command_tree(cli._parser())
    resources = shortcuts.resources(api.operations())
    problems, seen, pages = [], 0, 0
    for page in sorted(pathlib.Path(docs).rglob("*.md*")):
        found = False
        for number, command in shell_lines(page.read_text(encoding="utf-8")):
            for words in segments(command):
                if words[0] != "stardelt":
                    continue
                seen, found = seen + 1, True
                wrong = problem(words[1:], tree, resources)
                if wrong:
                    problems.append(f"{page}:{number}: {wrong}")
        pages += found
    return problems, seen, pages


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("docs", help="the folder of Markdown pages to read")
    args = parser.parse_args(argv)
    problems, seen, pages = check(args.docs)
    for line in problems:
        print(line)
    if problems:
        print(f"{len(problems)} of {seen} commands name nothing this CLI has")
        return 1
    print(f"ok: {seen} commands on {pages} pages, each one this CLI has")
    return 0


if __name__ == "__main__":
    sys.exit(main())
