"""scripts/*.sh: no Secret is written with a client-side `kubectl apply` (stardelt/stardelt#1655).

A client-side apply stores the whole applied object — `data` included — in the
`kubectl.kubernetes.io/last-applied-configuration` annotation, so anyone allowed to read a
Secret's metadata, but not its data, reads the credential. `--server-side` never writes that
annotation.

Two shapes exist in these scripts and both are checked:

1. `kubectl create secret … --dry-run=client -o yaml | kubectl apply -f -` — the pipeline form.
   Each `create secret` line and the 3 lines after it are read together, because the pipeline
   wraps over two or three lines.
2. `kubectl create secret … -o json > file` … `kubectl apply -f file` — the file form
   (`seed-dev-access.sh`). The receiver sits further down, so the rule is per shell function:
   a function that creates a Secret and applies a file must apply it server-side.

Scope is `platform/scripts/*.sh` and `platform/dev/*.sh`; `spike-*.sh` are scratch (`make lint`
skips them too). `dev/ghcr-pull-secret.sh` writes the GHCR pull Secret with a client-side apply
until #1663, which is exactly the shape this file exists to find.

The scan reads the real scripts, so a fourth `create secret … | kubectl apply -f -` fails this
test the moment it lands.

Standard library only, like the neighbours; no cluster, no credential.
"""

import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = (REPO / "scripts", REPO / "dev")

CREATE_SECRET = re.compile(r"\bcreate\s+secret\b")
# One pipeline segment that applies a manifest from stdin to the server.
APPLY_STDIN = re.compile(r"\bapply\b.*-f[ =]+-")
# `kubectl apply … -f "$file"` (a file, not stdin): the target does not start with `-`.
APPLY_FILE = re.compile(r"\bapply\b(?!.*-f[ =]+-).*-f[ =]+[\"']?[^\s\"'-]")
# The create command writes the manifest somewhere for a later apply.
FILE_WRITE = re.compile(r">\s*[\"']?[^\s\"']+\.(?:json|ya?ml)")
PIPE = re.compile(r"\|")
FUNCTION = re.compile(r"^([A-Za-z_]\w*)\s*\(\)\s*\{")


def _comment(line):
    return line.lstrip().startswith("#")


def _functions(lines):
    """Yield `(first index, last index)` for each `name() {` … `}` block."""
    index = 0
    while index < len(lines):
        if FUNCTION.match(lines[index]):
            end = index + 1
            while end < len(lines) and lines[end].rstrip() != "}":
                end += 1
            yield index, min(end, len(lines) - 1)
            index = end + 1
        else:
            index += 1


def _logical(lines):
    """`(first physical line index, text)` per shell logical line, with `\\` continuations joined.

    A `kubectl create secret` that wraps over six lines (one flag per line, as
    `dev/ghcr-pull-secret.sh` writes it) is one command; reading physical lines with a fixed
    window missed it until #1663.
    """
    joined = []
    buffer, start = "", None
    for index, line in enumerate(lines):
        if start is None:
            start = index
        stripped = line.rstrip()
        if stripped.endswith("\\"):
            buffer += stripped[:-1] + " "
            continue
        buffer += stripped
        joined.append((start, buffer))
        buffer, start = "", None
    if buffer:
        joined.append((start, buffer))
    return joined


def offending_commands(text, window=3):
    """Return `(line number, line)` for every client-side apply of a Secret in `text`."""
    lines = text.splitlines()
    logical = _logical(lines)
    found = {}

    # Shape 1: the create command's own pipeline applies stdin.
    for position, (index, command) in enumerate(logical):
        if _comment(command) or not CREATE_SECRET.search(command):
            continue
        joined = " ".join([command] + [later for _, later in logical[position + 1 : position + 1 + window]])
        for segment in PIPE.split(joined):
            if APPLY_STDIN.search(segment) and "--server-side" not in segment:
                found[index + 1] = lines[index].strip()
                break

    # Shape 2: the create command writes a file that the same function applies.
    for start, end in _functions(lines):
        block = lines[start : end + 1]
        joined = "\n".join(block)
        if not any(
            CREATE_SECRET.search(line) and FILE_WRITE.search(joined)
            for line in block
            if not _comment(line)
        ):
            continue
        for offset, line in enumerate(block):
            if _comment(line) or "--server-side" in line:
                continue
            if APPLY_FILE.search(line) and (start + offset + 1) not in found:
                found[start + offset + 1] = line.strip()

    return sorted(found.items())


def script_files():
    return sorted(
        path
        for directory in SCRIPTS
        for path in directory.glob("*.sh")
        if not path.name.startswith("spike-")
    )


# A value that must never be a process argument: `--docker-password=…` or a literal Secret value
# is readable from /proc/<pid>/cmdline while the command runs (#1682).
CREDENTIAL_ARGV = re.compile(
    r"--(?:docker-password|from-literal=[^\s\"']*(?:password|token|secret))", re.IGNORECASE
)


def credential_arguments(text):
    """Return `(line number, line)` for every credential passed as a command argument."""
    return [
        (number, line.strip())
        for number, line in enumerate(text.splitlines(), start=1)
        if not _comment(line) and CREDENTIAL_ARGV.search(line)
    ]


class NoClientApplyOfSecretsTest(unittest.TestCase):
    def test_repository_scripts_apply_secrets_server_side(self):
        self.assertNotEqual(script_files(), [], "no scripts/*.sh or dev/*.sh found — wrong REPO?")
        offences = []
        for path in script_files():
            for number, line in offending_commands(path.read_text()):
                offences.append(f"{path.name}:{number}: {line}")
        self.assertEqual(
            offences,
            [],
            "a Secret is applied client-side, which copies its data into "
            "kubectl.kubernetes.io/last-applied-configuration; add "
            "`--server-side --field-manager=stardelt-scripts`:\n" + "\n".join(offences),
        )

    def test_detector_flags_a_client_side_apply(self):
        text = (
            '  kubectl -n "$NS" create secret generic "$S" \\\n'
            '    --from-literal=password="$p" --dry-run=client -o yaml \\\n'
            "  | kubectl -n \"$NS\" apply -f - >/dev/null\n"
        )
        self.assertEqual(len(offending_commands(text)), 1)

    def test_detector_flags_the_one_line_form(self):
        text = 'kubectl create secret generic x --from-file=t=./t -o yaml | kubectl apply -f -\n'
        self.assertEqual(len(offending_commands(text)), 1)

    def test_detector_accepts_server_side(self):
        text = (
            'kubectl -n "$NS" create secret generic "$S" --from-file=t=./t -o yaml \\\n'
            "  | kubectl apply --server-side --field-manager=stardelt-scripts -f -\n"
        )
        self.assertEqual(offending_commands(text), [])

    def test_detector_ignores_secrets_written_directly(self):
        text = 'kubectl -n "$NS" create secret generic "$S" --from-env-file="$f" >/dev/null\n'
        self.assertEqual(offending_commands(text), [])

    def test_detector_ignores_other_resources(self):
        # A ConfigMap holds no credential; the guard is about Secrets (#1655).
        text = 'kubectl create configmap "$C" --from-file=x=./x -o yaml | kubectl apply -f -\n'
        self.assertEqual(offending_commands(text), [])

    def test_detector_flags_create_to_file_then_apply_file_in_a_function(self):
        # seed-dev-access.sh's `apply_seed_secret` shape.
        text = (
            "apply_seed_secret() {\n"
            '  kubectl -n "$NS" create secret generic "$S" "${args[@]}" \\\n'
            '    --dry-run=client -o json >"$tmp/secret.json"\n'
            '  python3 - "$tmp/secret.json" >"$tmp/secret-final.json" <<\'PY\'\n'
            "import json\n"
            "PY\n"
            '  kubectl -n "$NS" apply -f "$tmp/secret-final.json"\n'
            "}\n"
        )
        self.assertEqual(len(offending_commands(text)), 1)

    def test_detector_ignores_an_unrelated_apply_in_another_function(self):
        text = (
            "apply_seed_secret() {\n"
            '  kubectl -n "$NS" create secret generic "$S" \\\n'
            '    --from-file=a=./a --dry-run=client -o json >"$tmp/secret.json"\n'
            '  kubectl -n "$NS" apply --server-side -f "$tmp/secret.json"\n'
            "}\n"
            "\n"
            "apply_warehouses() {\n"
            '  kubectl apply -f "$WAREHOUSES_FILE"\n'
            "}\n"
        )
        self.assertEqual(offending_commands(text), [])


class NoCredentialInArgvTest(unittest.TestCase):
    def test_dev_scripts_keep_credentials_off_the_command_line(self):
        offenders = []
        for path in sorted((REPO / "dev").glob("*.sh")):
            if path.name.startswith("spike-"):
                continue
            for number, line in credential_arguments(path.read_text()):
                offenders.append(f"{path.name}:{number}: {line}")
        self.assertEqual(
            offenders,
            [],
            "a credential is passed as a command argument, where `ps` can read it; pass it in "
            "the environment or on stdin:\n" + "\n".join(offenders),
        )

    def test_detector_flags_docker_password(self):
        self.assertEqual(
            len(credential_arguments('  --docker-password="$token" \\\n')), 1
        )

    def test_detector_flags_a_literal_secret(self):
        self.assertEqual(
            len(credential_arguments('kubectl create secret generic s --from-literal=password="$p"\n')),
            1,
        )

    def test_detector_accepts_a_manifest_on_stdin(self):
        self.assertEqual(
            credential_arguments('printf \'%s\' "$m" | kubectl apply --server-side -f -\n'), []
        )


if __name__ == "__main__":
    unittest.main()
