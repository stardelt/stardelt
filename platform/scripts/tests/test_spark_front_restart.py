"""`make install-spark` restarts the Spark Connect front when its ConfigMap changes.

Envoy reads `/etc/envoy/envoy.yaml` once, at start. A projected ConfigMap volume updates the
file on disk and nothing restarts the process. `manifests/spark-connect-auth.yaml` holds both
the ConfigMap and the Deployment, so `kubectl apply` of an unchanged Deployment spec is a
no-op and `kubectl rollout status` returns at once — measured on the lab 2026-11-19: the
`spark-connect-auth` ConfigMap changed at 17:48:14Z and the pod that served it started at
17:48:33Z only because a person ran `kubectl rollout restart`. Every security fix to the front
(`stardelt/stardelt#404`'s RBAC filter is one) therefore *looked* applied while the running
proxy kept the previous static config.

`install-spark` already does this for the Connect server itself: it hashes
`spark-connect-config`'s `spark-defaults.conf` before and after the apply and restarts only
when it changed. The front now follows the same shape. The tests below pin that shape by
reading the Makefile; they run no cluster and no `make`.
"""

import pathlib
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
MAKEFILE = REPO_ROOT / "Makefile"

APPLY = "manifests/spark-connect-auth.yaml | kubectl apply -f -"
HASH_READ = "get configmap spark-connect-auth"
RESTART = "kubectl -n $(NAMESPACE) rollout restart deploy/spark-connect-auth"
STATUS = "rollout status deploy/spark-connect-auth"
GUARD = 'if [ "$$old" != "$$new" ]; then'


def install_spark_recipe():
    """The recipe of the `install-spark: deps _context` target, verbatim.

    It runs from the target line to the first following line that starts at column 0 (a
    comment or the next target): every recipe line is tab-indented.
    """
    lines = MAKEFILE.read_text(encoding="utf-8").splitlines(keepends=True)
    start = None
    for number, line in enumerate(lines):
        if line.startswith("install-spark: deps _context"):
            start = number
            break
    if start is None:
        raise AssertionError("the Makefile has no `install-spark: deps _context` target")
    recipe = []
    for line in lines[start + 1:]:
        if line and not line[0].isspace():
            break
        recipe.append(line)
    return "".join(recipe)


class SparkFrontRestart(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.recipe = install_spark_recipe()

    def index(self, needle, after=0):
        position = self.recipe.find(needle, after)
        if position < 0:
            raise AssertionError(
                f"the install-spark recipe has no {needle!r} at or after offset {after}"
            )
        return position

    def second_hash_read(self):
        apply = self.index(APPLY)
        return self.index(HASH_READ, after=apply)

    def test_the_old_hash_is_read_before_the_apply(self):
        old = self.index(HASH_READ)
        apply = self.index(APPLY)
        new = self.index(HASH_READ, after=apply)
        self.assertLess(old, apply, "the old envoy.yaml hash must be read before the apply")
        self.assertLess(apply, new, "a new envoy.yaml hash must be read after the apply")

    def test_the_front_restarts_when_the_hash_changed(self):
        new = self.second_hash_read()
        restart = self.index(RESTART, after=new)
        guard = self.index(GUARD, after=new)
        self.assertLess(guard, restart, "the restart must sit under the old/new hash test")

    def test_an_unchanged_config_does_not_restart(self):
        new = self.second_hash_read()
        guard = self.index(GUARD, after=new)
        restart = self.index(RESTART, after=guard)
        end = self.index("fi; \\", after=guard)
        self.assertLess(restart, end, "the restart must sit inside the front's if/fi")
        status = self.index(STATUS, after=end)
        self.assertLess(end, status, "rollout status must still be waited for after the fi")


if __name__ == "__main__":
    unittest.main()
