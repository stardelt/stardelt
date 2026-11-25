"""scripts/*.sh: no Secret is written with a client-side `kubectl apply` (stardelt/stardelt#1655).

A client-side apply stores the whole applied object — `data` included — in the
`kubectl.kubernetes.io/last-applied-configuration` annotation, so anyone allowed to read a
Secret's metadata, but not its data, reads the credential. `--server-side` never writes that
annotation.

The scan reads the real scripts (`platform/scripts/*.sh`), so a ticket that adds a fourth
`create secret … | kubectl apply -f -` fails this test the moment it lands. `spike-*.sh` are
scratch and out of scope, like `make lint` treats them.

Standard library only, like the neighbours; no cluster, no credential.
"""

import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"

CREATE_SECRET = re.compile(r"\bcreate\s+secret\b")
# One pipeline segment that applies a manifest from stdin to the server.
APPLY_STDIN = re.compile(r"\bapply\b.*-f[ =]+-")
PIPE = re.compile(r"\|")


def offending_commands(text, window=3):
    """Return `(first line number, line)` for every client-side apply of a Secret.

    Each `create secret` line and the `window` lines after it are read together: a script
    wraps the pipeline over two or three lines, and the receiving `kubectl apply` is what
    matters.
    """
    lines = text.splitlines()
    found = []
    for index, line in enumerate(lines):
        if not CREATE_SECRET.search(line) or line.lstrip().startswith("#"):
            continue
        joined = " ".join(lines[index : index + 1 + window])
        for segment in PIPE.split(joined):
            if APPLY_STDIN.search(segment) and "--server-side" not in segment:
                found.append((index + 1, line.strip()))
                break
    return found


def script_files():
    return sorted(
        path
        for path in SCRIPTS.glob("*.sh")
        if not path.name.startswith("spike-")
    )


class NoClientApplyOfSecretsTest(unittest.TestCase):
    def test_repository_scripts_apply_secrets_server_side(self):
        self.assertNotEqual(script_files(), [], "no scripts/*.sh found — wrong REPO?")
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


if __name__ == "__main__":
    unittest.main()
