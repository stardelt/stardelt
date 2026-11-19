"""The demo Spark sandbox declares its containment (stardelt/stardelt#201 slice 1)."""

import pathlib
import re
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "manifests" / "spark-demo-sandbox.yaml"


class SparkDemoSandboxTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = MANIFEST.read_text()
        cls.docs = [d for d in yaml.safe_load_all(cls.text) if d]

    def by_kind(self, kind):
        return [d for d in self.docs if d.get("kind") == kind]

    def one(self, kind, name=None):
        found = [d for d in self.by_kind(kind)
                 if name is None or d["metadata"]["name"] == name]
        self.assertEqual(len(found), 1, f"expected exactly one {kind} {name or ''} in {MANIFEST}")
        return found[0]

    def test_the_namespace_comes_first(self):
        # kubectl applies the documents in file order; the namespaced objects need it to exist.
        self.assertEqual(self.docs[0].get("kind"), "Namespace")

    def test_it_declares_exactly_the_ten_objects(self):
        for kind in ("Namespace", "ServiceAccount", "ConfigMap", "Deployment", "Service",
                     "NetworkPolicy", "Role", "RoleBinding"):
            self.assertTrue(self.by_kind(kind), f"expected a {kind} in {MANIFEST}")
        # Two of each Role kind since nova#271: the nightly reset, and Nova's read.
        self.assertEqual(len(self.by_kind("Role")), 2)
        self.assertEqual(len(self.by_kind("RoleBinding")), 2)
        self.assertEqual(len(self.docs), 10)

    def test_it_grants_no_cluster_rights(self):
        for kind in ("ClusterRole", "ClusterRoleBinding"):
            self.assertEqual(self.by_kind(kind), [], f"the sandbox must declare no {kind}")

    def test_the_reset_role_patches_one_deployment_and_nothing_else(self):
        # stardelt/stardelt#201 item 6: the reset's ServiceAccount, in another namespace,
        # may do nothing but bump this Deployment's pod template.
        role = self.one("Role", "demo-reset-spark")
        self.assertEqual(role["metadata"]["namespace"], "stardelt-demo-spark")
        self.assertEqual(
            role["rules"],
            [{"apiGroups": ["apps"], "resources": ["deployments"],
              "resourceNames": ["spark-demo"], "verbs": ["get", "patch"]}],
        )
        binding = self.one("RoleBinding", "demo-reset-spark")
        self.assertEqual(binding["metadata"]["namespace"], "stardelt-demo-spark")
        self.assertEqual(binding["roleRef"],
                         {"kind": "Role", "name": "demo-reset-spark",
                          "apiGroup": "rbac.authorization.k8s.io"})
        self.assertEqual(
            binding["subjects"],
            [{"kind": "ServiceAccount", "name": "demo-reset", "namespace": "stardelt"}],
        )

    def test_nova_may_read_the_server_and_its_config_and_nothing_else(self):
        # nova#271: the Compute page lists the labelled Deployment and reads
        # `spark-demo-config`. It never writes, and the sandbox's own ServiceAccount gains
        # nothing from it — the binding names `nova` in `stardelt`.
        role = self.one("Role", "nova-compute-reader")
        self.assertEqual(role["metadata"]["namespace"], "stardelt-demo-spark")
        self.assertEqual(
            role["rules"],
            [{"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get", "list"]},
             {"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get", "list"]}],
        )
        binding = self.one("RoleBinding", "nova-compute-reader")
        self.assertEqual(binding["metadata"]["namespace"], "stardelt-demo-spark")
        self.assertEqual(binding["roleRef"],
                         {"kind": "Role", "name": "nova-compute-reader",
                          "apiGroup": "rbac.authorization.k8s.io"})
        self.assertEqual(
            binding["subjects"],
            [{"kind": "ServiceAccount", "name": "nova", "namespace": "stardelt"}],
        )

    def test_the_service_account_mounts_no_token(self):
        sa = self.one("ServiceAccount", "spark-demo")
        self.assertIs(sa.get("automountServiceAccountToken"), False)

    def test_the_deployment_mounts_no_token_either(self):
        pod = self.one("Deployment", "spark-demo")["spec"]["template"]["spec"]
        self.assertIs(pod.get("automountServiceAccountToken"), False)
        self.assertEqual(pod["serviceAccountName"], "spark-demo")

    def test_the_namespace_enforces_restricted_pod_security(self):
        labels = self.one("Namespace", "stardelt-demo-spark")["metadata"]["labels"]
        self.assertEqual(labels["pod-security.kubernetes.io/enforce"], "restricted")

    def test_the_driver_runs_in_local_mode(self):
        conf = self.one("ConfigMap", "spark-demo-config")["data"]["spark-defaults.conf"]
        self.assertRegex(conf, r"spark\.master\s+local\[2\]")

    def test_the_driver_configures_no_kubernetes_cluster(self):
        conf = self.one("ConfigMap", "spark-demo-config")["data"]["spark-defaults.conf"]
        self.assertNotIn("spark.kubernetes.namespace", conf,
                         "local mode takes no Kubernetes client, so it takes no namespace")

    def test_the_network_policy_covers_ingress_and_egress(self):
        spec = self.one("NetworkPolicy", "spark-demo")["spec"]
        self.assertEqual(spec["policyTypes"], ["Ingress", "Egress"])

    def test_the_egress_reaches_the_s3_gateway(self):
        spec = self.one("NetworkPolicy", "spark-demo")["spec"]
        ports = {p["port"] for rule in spec["egress"] for p in rule.get("ports") or []}
        self.assertIn(8333, ports)

    def test_the_egress_admits_no_public_range(self):
        spec = self.one("NetworkPolicy", "spark-demo")["spec"]
        for rule in spec["egress"]:
            for peer in rule.get("to") or []:
                block = peer.get("ipBlock") or {}
                self.assertNotIn(block.get("cidr"), ("0.0.0.0/0", "::/0"),
                                 "the sandbox must not reach the internet")
        self.assertNotIn("0.0.0.0/0", self.text)

    def test_the_image_is_a_placeholder_the_installer_renders(self):
        self.assertIn("__SPARK_IMAGE__", self.text)
        self.assertNotIn("sha256:", self.text,
                         "the digest comes from manifests/spark.yaml at install time")


if __name__ == "__main__":
    unittest.main()
