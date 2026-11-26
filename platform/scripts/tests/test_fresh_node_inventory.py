"""Unit tests for scripts/fresh_node_inventory.py (#1803).

Standard library only, like the script: ``python3 -m unittest scripts.tests.test_fresh_node_inventory``
runs them in CI. Every case writes its inputs into a temporary directory; none needs a cluster,
CoreDNS or the internet.
"""

import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "fresh_node_inventory.py"
_spec = importlib.util.spec_from_file_location("fresh_node_inventory", SCRIPT)
inventory = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(inventory)

HEADER_EGRESS = "# name\tkind\tcallers"
HEADER_IMAGES = "# image\timageID"


def query(ip, name, code="NOERROR", qtype="A"):
    return (
        f'[INFO] {ip}:41234 - 4567 "{qtype} IN {name} udp 59 false 1232" {code} qr,rd,ra 135 0.020s\n'
    )


def pod(name, namespace="stardelt", ip=None, containers=(), init_containers=(), host_network=False):
    status = {}
    if ip:
        status["podIP"] = ip
    if containers:
        status["containerStatuses"] = list(containers)
    if init_containers:
        status["initContainerStatuses"] = list(init_containers)
    return {
        "metadata": {"name": name, "namespace": namespace},
        "spec": {"hostNetwork": host_network},
        "status": status,
    }


def container(image, image_id="sha256:" + "0" * 64):
    return {"name": image.rsplit("/", 1)[-1], "image": image, "imageID": image_id}


class Inventory(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.log = self.tmp / "coredns.log"
        self.pods = self.tmp / "pods.json"
        self.out = self.tmp / "out"

    def write(self, log_lines=(), pods=()):
        self.log.write_text("".join(log_lines), encoding="utf-8")
        self.pods.write_text(json.dumps({"items": list(pods)}), encoding="utf-8")

    def run_main(self, own_domain=""):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            rc = inventory.main([
                "--coredns-log", str(self.log),
                "--pods-json", str(self.pods),
                "--own-domain", own_domain,
                "--out-dir", str(self.out),
            ])
        return rc, stdout.getvalue(), stderr.getvalue()

    def egress(self):
        return (self.out / "egress.txt").read_text(encoding="utf-8").splitlines()

    def images(self):
        return (self.out / "images.txt").read_text(encoding="utf-8").splitlines()

    def test_external_and_own_names_are_split(self):
        self.write(
            log_lines=[
                query("10.42.0.12", "acme-v02.api.letsencrypt.org."),
                query("10.42.0.12", "id.example.test."),
                query("10.42.0.12", "nova.stardelt.svc.cluster.local."),
            ],
            pods=[pod("cert-manager-abc", "cert-manager", ip="10.42.0.12")],
        )
        rc, _, _ = self.run_main(own_domain="example.test")
        self.assertEqual(rc, 0)
        lines = self.egress()
        self.assertEqual(len(lines), 3)
        self.assertEqual(lines[0], HEADER_EGRESS)
        by_name = {line.split("\t")[0]: line.split("\t") for line in lines[1:]}
        self.assertEqual(by_name["acme-v02.api.letsencrypt.org"][1], "external")
        self.assertEqual(by_name["id.example.test"][1], "own")
        self.assertNotIn("nova.stardelt.svc.cluster.local", by_name)

    def test_failed_lookups_are_dropped(self):
        self.write(
            log_lines=[
                query("10.42.0.12", "missing.example.test.", code="NXDOMAIN"),
                query("10.42.0.12", "present.example.test.", code="NOERROR"),
            ],
            pods=[],
        )
        rc, _, _ = self.run_main()
        self.assertEqual(rc, 0)
        names = [line.split("\t")[0] for line in self.egress()[1:]]
        self.assertEqual(names, ["present.example.test"])

    def test_callers_are_named_by_pod(self):
        self.write(
            log_lines=[
                query("10.42.0.12", "a.example.test."),
                query("10.42.0.99", "b.example.test."),
            ],
            pods=[pod("cert-manager-abc", "cert-manager", ip="10.42.0.12")],
        )
        rc, _, _ = self.run_main()
        self.assertEqual(rc, 0)
        by_name = {line.split("\t")[0]: line.split("\t")[2] for line in self.egress()[1:]}
        self.assertEqual(by_name["a.example.test"], "cert-manager/cert-manager-abc")
        self.assertEqual(by_name["b.example.test"], "node:10.42.0.99")

    def test_images_are_listed_once(self):
        image = "ghcr.io/stardelt/nova:0.0.4"
        init = "ghcr.io/stardelt/stardelt-init:0.0.4"
        self.write(
            log_lines=[],
            pods=[
                pod("nova-a", containers=[container(image)], init_containers=[container(init)]),
                pod("nova-b", containers=[container(image)]),
            ],
        )
        rc, stdout, _ = self.run_main()
        self.assertEqual(rc, 0)
        lines = self.images()
        self.assertEqual(len(lines), 3)
        self.assertEqual(lines[0], HEADER_IMAGES)
        self.assertIn(f"{image}\tsha256:{'0' * 64}", lines[1:])
        self.assertIn(f"{init}\tsha256:{'0' * 64}", lines[1:])
        self.assertIn("egress: 0 external name(s), 0 own (egress.txt)", stdout)
        self.assertIn("images: 2 (images.txt)", stdout)

    def test_missing_input_fails(self):
        self.pods.unlink(missing_ok=True)
        self.log.write_text("", encoding="utf-8")
        rc, _, stderr = self.run_main()
        self.assertEqual(rc, 1)
        self.assertIn("cannot read", stderr)


if __name__ == "__main__":
    unittest.main()
