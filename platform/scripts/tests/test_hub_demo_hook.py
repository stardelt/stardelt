"""A visitor's `spark` is the demo sandbox (stardelt/stardelt#201 slice c, #310).

`27-demo-mode` in `helm-values/jupyterhub.yaml` sets `DEMO_MODE` for a demo visitor and
must set `SPARK_REMOTE` to the sandbox Service in `manifests/spark-demo-sandbox.yaml`:
`spark-connect-ingress` refuses a visitor-labelled pod on the shared server. A member
keeps the shared server from the single-user defaults (`:457`).

No cluster, no network: the hook string is exec'd against a fake spawner.
"""

import pathlib
import types
import unittest

import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
VALUES = REPO / "helm-values/jupyterhub.yaml"
SANDBOX = "sc://spark-demo.stardelt-demo-spark.svc.cluster.local:15002"


def _hook():
    with open(VALUES, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return data["hub"]["extraConfig"]["27-demo-mode"]


def _install():
    """exec the snippet and return (pre_spawn_hook, config)."""
    config = types.SimpleNamespace(KubeSpawner=types.SimpleNamespace())
    namespace = {"c": config}
    exec(_hook(), namespace)  # noqa: S102 - the value is the file under test
    return config.KubeSpawner.pre_spawn_hook, config


def spawner(*groups):
    user = types.SimpleNamespace(name="demo", groups=[types.SimpleNamespace(name=g) for g in groups])
    return types.SimpleNamespace(user=user, environment={}, log=types.SimpleNamespace(warning=lambda *a, **k: None))


class HubDemoHookTest(unittest.TestCase):
    def test_a_visitor_gets_the_sandbox(self):
        hook, _ = _install()
        fake = spawner("visitors")
        self.assertIsNone(hook(fake))
        self.assertEqual(fake.environment["DEMO_MODE"], "1")
        self.assertEqual(fake.environment["SPARK_REMOTE"], SANDBOX)
        self.assertEqual(_hook().count(SANDBOX), 1)

    def test_a_member_is_left_alone(self):
        hook, _ = _install()
        fake = spawner("stardelt-users")
        self.assertIsNone(hook(fake))
        self.assertEqual(fake.environment, {})

    def test_a_visitor_who_is_also_a_member_is_left_alone(self):
        hook, _ = _install()
        fake = spawner("visitors", "stardelt-users")
        self.assertIsNone(hook(fake))
        self.assertEqual(fake.environment, {})


if __name__ == "__main__":
    unittest.main()
