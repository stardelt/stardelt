"""The throwaway-cluster wrapper: it must be unable to reach the dev cluster.

``dev/hetzner-k3s/fresh-node.sh`` renders its own config and never updates DNS, so a
fresh-node run cannot orphan the dev cluster or point ``alpha.dev`` at a server that is
deleted an hour later (platform#1018). Two things are easy to get wrong and expensive
when they are:

* the dev config — ``dev/hetzner-k3s/cluster.yaml`` is the only record ``make dev-down``
  has of what to delete, so a fresh-node call must leave it byte-identical (or, on a
  checkout that has none, must not create it);
* the name — everything that is not ``stardelt-fresh-*`` is refused before
  ``destroy.sh`` is reached, so passing ``FRESH_NAME=stardelt-dev`` (or a config that
  describes it) deletes nothing.

Every case here runs without network or cloud access: the ``up`` cases use ``DRY_RUN=1``,
and the teardown case runs ``down`` with ``kubectl``, ``helm``, ``hcloud`` and
``hetzner-k3s`` replaced by fakes on ``PATH``. No server is created and the real
``hetzner-k3s``, ``hcloud`` and DNS are never called. ``ALLOW_IPV4``/``ALLOW_IPV6`` and
``CF_ENV`` are set so the scripts do not look up the caller's address or read the
caller's credentials.
"""

import filecmp
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "dev" / "hetzner-k3s" / "fresh-node.sh"
DEV_CONFIG = REPO / "dev" / "hetzner-k3s" / "cluster.yaml"


class FreshNode(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.home = self.tmp / "home"
        self.home.mkdir()
        self.config = self.tmp / "fresh.yaml"
        self.kubeconfig = self.tmp / "kubeconfig.yaml"

    def tearDown(self):
        self._tmp.cleanup()

    def run_script(self, *args, **env):
        """Run fresh-node.sh with every path pointed inside the temporary directory."""
        base = {
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
            "HOME": str(self.home),
            "CF_ENV": str(self.tmp / "absent-cloudflare.env"),
            "HETZNER_ENV": str(self.tmp / "absent-hetzner.env"),
            # No network: the allowed networks are the ones a real run would discover.
            "ALLOW_IPV4": "203.0.113.7/32",
            "ALLOW_IPV6": "2001:db8::/128",
            "FRESH_NAME": "stardelt-fresh-test",
            "FRESH_CONFIG": str(self.config),
            "FRESH_KUBECONFIG": str(self.kubeconfig),
            "DRY_RUN": "1",
        }
        base.update(env)
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            cwd=str(REPO),
            env=base,
            capture_output=True,
            text=True,
        )

    def write_config(self, cluster_name):
        self.config.write_text(
            "---\n"
            "cluster_name: {name}\n"
            "kubeconfig_path: {kube}\n".format(name=cluster_name, kube=self.kubeconfig)
        )

    # -- up -----------------------------------------------------------------
    def test_dry_run_up_does_not_touch_the_dev_config(self):
        before = self.tmp / "cluster.yaml.before"
        existed = DEV_CONFIG.exists()
        if existed:
            before.write_bytes(DEV_CONFIG.read_bytes())

        result = self.run_script("up")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            "would write {} for cluster stardelt-fresh-test".format(self.config),
            result.stdout,
        )
        self.assertIn("would run: hetzner-k3s create", result.stdout)
        # Nothing was written: no config, no kubeconfig directory, no dev config.
        self.assertFalse(self.config.exists())
        self.assertFalse(self.kubeconfig.exists())
        self.assertFalse((self.home / ".kube").exists())
        if existed:
            self.assertTrue(
                filecmp.cmp(before, DEV_CONFIG, shallow=False),
                "the dev cluster.yaml changed during a fresh-node dry run",
            )
        else:
            self.assertFalse(DEV_CONFIG.exists(), "the dev cluster.yaml was created")

    def test_up_refuses_an_existing_config(self):
        self.write_config("stardelt-fresh-test")

        result = self.run_script("up")

        self.assertEqual(result.returncode, 1)
        self.assertIn("refusing:", result.stderr)
        # It stopped before rendering: no plan of a second cluster in the output.
        self.assertNotIn("would run: hetzner-k3s create", result.stdout)

    # -- down ---------------------------------------------------------------
    def test_down_refuses_the_dev_cluster(self):
        self.write_config("stardelt-dev")

        result = self.run_script("down")

        self.assertEqual(result.returncode, 1)
        self.assertIn("refusing:", result.stderr)
        # destroy.sh was never reached: it would have printed its plan first.
        self.assertNotIn("About to delete", result.stdout)
        self.assertTrue(self.config.exists())

    def test_dry_run_down_keeps_the_config(self):
        self.write_config("stardelt-fresh-test")

        result = self.run_script("down")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("would run: hetzner-k3s delete", result.stdout)
        self.assertTrue(self.config.exists(), "a dry run removed the config")

    # -- teardown -----------------------------------------------------------
    def fake_cloud_bin(self):
        """kubectl, helm, hcloud and hetzner-k3s on PATH; every call is logged."""
        fake_bin = self.tmp / "bin"
        fake_bin.mkdir()
        calls = self.tmp / "calls.log"
        for name in ("kubectl", "helm", "hcloud", "hetzner-k3s"):
            script = fake_bin / name
            if name == "kubectl":
                script.write_text(
                    "#!/usr/bin/env bash\n"
                    'printf "%s %s\\n" "kubectl" "$*" >> "{calls}"\n'
                    'case " $* " in\n'
                    '  *" get pv "*) printf "101\\n102\\n" ;;\n'
                    "esac\n"
                    "exit 0\n".format(calls=calls)
                )
            else:
                script.write_text(
                    "#!/usr/bin/env bash\n"
                    'printf "%s %s\\n" "{name}" "$*" >> "{calls}"\n'
                    "exit 0\n".format(name=name, calls=calls)
                )
            script.chmod(0o755)
        return fake_bin, calls

    def test_down_deletes_the_recorded_volumes_after_the_cluster(self):
        fake_bin, calls = self.fake_cloud_bin()
        self.kubeconfig.write_text("fake\n")
        self.write_config("stardelt-fresh-test")

        result = self.run_script(
            "down",
            DRY_RUN="0",
            PATH="{}:{}".format(fake_bin, os.environ.get("PATH", "/usr/bin:/bin")),
            FRESH_VOLUME_SLEEP="0",
            FORCE="1",
            # hcloud reads the token from the environment; the teardown fails fast without
            # one (#1632).
            HCLOUD_TOKEN="fake-token",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        lines = calls.read_text().splitlines()
        self.assertIn("hcloud volume delete 101", lines)
        self.assertIn("hcloud volume delete 102", lines)
        cluster_at = next(i for i, l in enumerate(lines) if l.startswith("hetzner-k3s delete"))
        volume_at = next(i for i, l in enumerate(lines) if l.startswith("hcloud volume delete"))
        self.assertLess(cluster_at, volume_at, "the volumes were deleted before the server")
        self.assertFalse([l for l in lines if "--wait=true" in l])
        self.assertFalse((self.tmp / "fresh.volumes").exists())

    def test_down_without_a_token_warns_once_and_keeps_the_volume_ids(self):
        # #1632: without HCLOUD_TOKEN every `hcloud volume delete` failed, the retry loop ran
        # 12 times per volume and the warning did not say why. One sentence, no retries.
        fake_bin, calls = self.fake_cloud_bin()
        self.kubeconfig.write_text("fake\n")
        self.write_config("stardelt-fresh-test")

        result = self.run_script(
            "down",
            DRY_RUN="0",
            PATH="{}:{}".format(fake_bin, os.environ.get("PATH", "/usr/bin:/bin")),
            FRESH_VOLUME_SLEEP="0",
            FORCE="1",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("warning: no HCLOUD_TOKEN", result.stderr)
        self.assertIn("101 102", result.stderr)
        # No attempt, so no retry loop: the ids stay on disk for the by-hand delete.
        self.assertFalse(
            [l for l in calls.read_text().splitlines() if l.startswith("hcloud volume delete")]
        )
        self.assertTrue((self.tmp / "fresh.volumes").exists())

    def test_down_reads_the_token_from_hetzner_env(self):
        # #1632: the maintainer keeps the token in ~/.config/stardelt/hetzner.env, which
        # provision.sh loads and fresh-node.sh did not — so hcloud ran without it.
        fake_bin, calls = self.fake_cloud_bin()
        self.kubeconfig.write_text("fake\n")
        self.write_config("stardelt-fresh-test")
        env_file = self.tmp / "hetzner.env"
        env_file.write_text("HCLOUD_TOKEN=fake-token-from-env-file\n")
        env_file.chmod(0o600)

        result = self.run_script(
            "down",
            DRY_RUN="0",
            PATH="{}:{}".format(fake_bin, os.environ.get("PATH", "/usr/bin:/bin")),
            FRESH_VOLUME_SLEEP="0",
            FORCE="1",
            HETZNER_ENV=str(env_file),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("no HCLOUD_TOKEN", result.stderr)
        lines = calls.read_text().splitlines()
        self.assertIn("hcloud volume delete 101", lines)
        self.assertIn("hcloud volume delete 102", lines)
        self.assertFalse((self.tmp / "fresh.volumes").exists())


if __name__ == "__main__":
    unittest.main()

