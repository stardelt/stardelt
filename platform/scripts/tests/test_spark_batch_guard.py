"""The Spark batch driver's pods and ConfigMaps are admission-bounded (platform#1302).

The batch driver runs the pipeline's Spark code, so its `pods/create` and
`configmaps/create+delete` rights are bounded by copies of the Connect policies
(`manifests/spark-connect-pod-guard.yaml`). The executor allow-list is pinned to the
Connect one on purpose: both paths run the same measured executor shape
(`chart/files/spark-runtimes.json`).
"""

import pathlib
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
MANIFEST = REPO_ROOT / "manifests" / "spark-batch-rbac.yaml"
CONNECT_MANIFEST = REPO_ROOT / "manifests" / "spark-connect-pod-guard.yaml"

# The five objects the batch path already owned before the guards were added.
EXISTING = [
    ("Role", "airflow-spark-submitter"),
    ("RoleBinding", "airflow-spark-submitter"),
    ("ServiceAccount", "spark-batch-driver"),
    ("Role", "spark-batch-driver"),
    ("RoleBinding", "spark-batch-driver"),
]

NEW = [
    ("ValidatingAdmissionPolicy", "spark-batch-executor-pods"),
    ("ValidatingAdmissionPolicyBinding", "spark-batch-executor-pods"),
    ("ValidatingAdmissionPolicy", "spark-batch-configmaps"),
    ("ValidatingAdmissionPolicyBinding", "spark-batch-configmaps"),
]


def _by_identity(path):
    docs = [d for d in yaml.safe_load_all(path.read_text()) if d]
    return {(d["kind"], d["metadata"]["name"]): d for d in docs}


def _pairs(policy):
    return [(v["expression"], v.get("message")) for v in policy["validations"]]


def _is_secret_rule(pair):
    expression = pair[0]
    return "secretKeyRef" in expression or "secretRef" in expression


def _plain(policy):
    return [pair for pair in _pairs(policy) if not _is_secret_rule(pair)]


def _secret(policy):
    return [pair for pair in _pairs(policy) if _is_secret_rule(pair)]


class SparkBatchGuardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.objects = _by_identity(MANIFEST)
        cls.connect = _by_identity(CONNECT_MANIFEST)

    def policy(self, name):
        return self.objects[("ValidatingAdmissionPolicy", name)]["spec"]

    def binding(self, name):
        return self.objects[("ValidatingAdmissionPolicyBinding", name)]["spec"]

    def test_the_existing_objects_are_unchanged(self):
        for identity in EXISTING:
            self.assertIn(identity, self.objects)

    def test_the_driver_role_keeps_its_verbs(self):
        rules = self.objects[("Role", "spark-batch-driver")]["rules"]
        self.assertEqual(
            rules,
            [
                {"apiGroups": [""], "resources": ["pods"],
                 "verbs": ["get", "list", "watch", "create", "delete", "deletecollection"]},
                {"apiGroups": [""], "resources": ["configmaps"],
                 "verbs": ["get", "list", "create", "delete"]},
            ],
        )

    def test_the_four_new_objects_exist(self):
        for identity in NEW:
            self.assertIn(identity, self.objects)

    def test_both_policies_fail_closed(self):
        self.assertEqual(self.policy("spark-batch-executor-pods")["failurePolicy"], "Fail")
        self.assertEqual(self.policy("spark-batch-configmaps")["failurePolicy"], "Fail")

    def test_both_policies_match_the_batch_driver_only(self):
        for name in ("spark-batch-executor-pods", "spark-batch-configmaps"):
            conditions = self.policy(name)["matchConditions"]
            self.assertEqual(len(conditions), 1)
            self.assertEqual(conditions[0]["name"], "created-by-spark-batch-driver")
            self.assertIn(":spark-batch-driver'", conditions[0]["expression"])

    def test_the_executor_validations_equal_the_connect_ones(self):
        batch = _plain(self.policy("spark-batch-executor-pods"))
        connect = _plain(self.connect[("ValidatingAdmissionPolicy",
                                       "spark-connect-executor-pods")]["spec"])
        self.assertEqual(batch, connect)

    def test_the_secret_rules_keep_the_platform_key_on_the_batch_driver_only(self):
        # A batch driver still reads the platform key until #494; the shared
        # Connect driver admits no Secret at all (#75).
        batch = _secret(self.policy("spark-batch-executor-pods"))
        connect = _secret(self.connect[("ValidatingAdmissionPolicy",
                                        "spark-connect-executor-pods")]["spec"])
        self.assertEqual(len(batch), 2)
        self.assertEqual(len(connect), 2)
        for expression, _ in batch:
            self.assertIn("stardelt-s3-creds", expression)
        for expression, _ in connect:
            self.assertNotIn("stardelt-s3-creds", expression)

    def test_no_batch_validation_allows_platform_pipeline_secrets(self):
        for name in ("spark-batch-executor-pods", "spark-batch-configmaps"):
            for validation in self.policy(name)["validations"]:
                self.assertNotIn("sp-pipeline-", validation["expression"])

    def test_the_config_map_validation_is_the_spark_name_filter(self):
        self.assertEqual(
            [v["expression"] for v in self.policy("spark-batch-configmaps")["validations"]],
            ["request.name.startsWith('spark-') && request.name.endsWith('-conf-map')"],
        )

    def test_both_bindings_deny_with_their_policy_in_the_platform_namespace(self):
        for name in ("spark-batch-executor-pods", "spark-batch-configmaps"):
            spec = self.binding(name)
            self.assertEqual(spec["policyName"], name)
            self.assertEqual(spec["validationActions"], ["Deny"])
            selector = spec["matchResources"]["namespaceSelector"]
            self.assertEqual(selector["matchLabels"]["kubernetes.io/metadata.name"], "stardelt")
            self.assertNotIn("namespace", self.objects[("ValidatingAdmissionPolicyBinding", name)]
                             ["metadata"])


if __name__ == "__main__":
    unittest.main()
