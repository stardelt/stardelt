"""Tests for scripts/verify-fresh-node.sh (platform#1017, platform#1019, platform#1480) and
scripts/check-ingress-certificate.sh (stardelt#2190).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl`, `curl`, `helm` and `openssl` are replaced by small shell stubs first on
PATH, so no cluster and no network are touched: `kubectl get` prints a fixture from `$FAKE_DIR`,
`wait`/`rollout status` succeed, `delete platforminstance` removes the `instance_present` marker
(so the following `get` is a NotFound) and `port-forward` just sleeps until the script's trap kills
it; `curl` prints the endpoint fixture that matches its URL; `helm` records its argv in
`$FAKE_DIR/helm_argv`; `openssl` prints the certificate transcript named by `$FAKE_DIR/certificate`.
"""
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-fresh-node.sh"
CERT_SCRIPT = REPO / "scripts" / "check-ingress-certificate.sh"

KUBECTL_STUB = """#!/usr/bin/env bash
# Fake kubectl: fixtures come from $FAKE_DIR, so each test case answers differently.
set -u
joined=" $* "
case "$joined" in
  *" wait "*) exit 0 ;;
  *" rollout "*) exit 0 ;;
  *" port-forward "*) exec sleep 60 ;;
  *" get crd "*) echo "customresourcedefinition.apiextensions.k8s.io/platforminstances.stardelt.io"; exit 0 ;;
esac
case "$joined" in
  *" delete "*)
    [ -f "$FAKE_DIR/delete_fail" ] && { echo "Error from server: the delete was refused" >&2; exit 1; }
    case "$joined" in
      *"delete platforminstance"*) rm -f "$FAKE_DIR/instance_present" ;;
    esac
    exit 0
    ;;
esac
case "$joined" in
  # the namespace's Pod Security floor (platform#1480): each label is asked for on its own.
  *" get namespace "*)
    case "$joined" in
      *"/enforce"*) printf '%s' "${FAKE_ENFORCE-restricted}" ;;
      *"/warn"*) printf '%s' "${FAKE_WARN-restricted}" ;;
      *"/audit"*) printf '%s' "${FAKE_AUDIT-restricted}" ;;
      *) exit 1 ;;
    esac
    exit 0
    ;;
  *" get lakehouse/"*)
    [ -f "$FAKE_DIR/lakehouse" ] && exit 0
    exit 1
    ;;
  *" get lakehouse "*)
    [ -f "$FAKE_DIR/lakehouse" ] && exit 0
    exit 1
    ;;
  *" get platforminstance "*)
    case "$joined" in
      *status.components*) cat "$FAKE_DIR/components"; exit 0 ;;
      *baseDomain*) if [ -f "$FAKE_DIR/base_domain" ]; then cat "$FAKE_DIR/base_domain"; fi; exit 0 ;;
    esac
    [ -f "$FAKE_DIR/instance_fail" ] && exit 1
    [ -f "$FAKE_DIR/instance_present" ] && exit 0
    exit 1
    ;;
  *" get deploy -o name "*)
    cat "$FAKE_DIR/deploy_names"
    exit 0
    ;;
  *" get deploy -o "*)
    cat "$FAKE_DIR/deploy"
    exit 0
    ;;
  *" get deploy stardelt-operator "*)
    if [ -f "$FAKE_DIR/operator_image" ]; then cat "$FAKE_DIR/operator_image"; fi
    exit 0
    ;;
  *" get deploy/"*)
    name="${joined##* get deploy/}"
    name="${name%% *}"
    [ -f "$FAKE_DIR/deploy_$name" ] && exit 0
    exit 1
    ;;
  *" get deploy "*)
    name="${joined##* get deploy }"
    name="${name%% *}"
    [ -f "$FAKE_DIR/deploy_$name" ] && exit 0
    exit 1
    ;;
  *" get pvc -o name "*)
    cat "$FAKE_DIR/pvc_names"
    exit 0
    ;;
  *" get pvc/"*)
    name="${joined##* get pvc/}"
    name="${name%% *}"
    if [ -f "$FAKE_DIR/pvc_$name" ]; then cat "$FAKE_DIR/pvc_$name"; exit 0; fi
    exit 1
    ;;
  *" get secret/"*)
    name="${joined##* get secret/}"
    name="${name%% *}"
    [ -f "$FAKE_DIR/secret_$name" ] && exit 0
    exit 1
    ;;
  *" get secret "*)
    name="${joined##* get secret }"
    name="${name%% *}"
    [ -f "$FAKE_DIR/secret_$name" ] && exit 0
    exit 1
    ;;
  *" get pods "*)
    cat "$FAKE_DIR/pods"
    exit 0
    ;;
esac
exit 1
"""

CURL_STUB = """#!/usr/bin/env bash
# Fake curl: the last argument is the URL; print the fixture that belongs to it. A `-w` call and
# a body read both print the same fixture: the code fixtures (`lakekeeper_management_code`,
# `nova_page_code`) carry exactly what `%{http_code}`/`%{content_type}` would print.
set -u
url="${!#}"
case "$url" in
  */api/health) cat "$FAKE_DIR/health" ;;
  */api/v2/monitor/health) cat "$FAKE_DIR/airflow_health" ;;
  *:18083/health) cat "$FAKE_DIR/superset_health" ;;
  *:18084/health) cat "$FAKE_DIR/lakekeeper_health" ;;
  *:18084/management/v1/warehouse) cat "$FAKE_DIR/lakekeeper_management_code" ;;
  http://127.0.0.1:18080/) cat "$FAKE_DIR/nova_page_code" ;;
  */v1/info) cat "$FAKE_DIR/trino_info" ;;
  */.well-known/openid-configuration) cat "$FAKE_DIR/oidc" ;;
  *) exit 1 ;;
esac
"""

HELM_STUB = """#!/usr/bin/env bash
# Fake helm: record the argv so a test can read what the chart would have consumed.
set -u
printf '%s\\n' "$*" > "$FAKE_DIR/helm_argv"
[ -f "$FAKE_DIR/helm_fail" ] && { echo "Error: UPGRADE FAILED: the chart was rejected" >&2; exit 1; }
exit 0
"""

OPENSSL_STUB = """#!/usr/bin/env bash
# Fake openssl: `x509 -noout -issuer` prints $FAKE_DIR/cert_issuer; `s_client` prints the
# transcript named by $FAKE_DIR/certificate — `trusted`, `untrusted` or `silent` (no verify line).
set -u
case " $* " in
  *" x509 "*) cat >/dev/null 2>&1; cat "$FAKE_DIR/cert_issuer" 2>/dev/null; exit 0 ;;
esac
case "$(cat "$FAKE_DIR/certificate" 2>/dev/null || echo trusted)" in
  untrusted) printf 'CONNECTED\\nverify error:num=18:self-signed certificate\\nVerify return code: 18 (self-signed certificate)\\n' ;;
  silent) printf 'CONNECTED\\nVerify return code: 1 (unknown)\\n' ;;
  *) printf 'CONNECTED\\nverify error:num=0:ok\\nVerify return code: 0 (ok)\\n' ;;
esac
exit 0
"""

GOOD_FILES = {
    "components": "airflow\ttrue\nkeycloak\ttrue\nnova\ttrue\ntrino\ttrue\n",
    "deploy": "airflow-dag-processor\t1\t1\nnova\t2\t2\ntrino\t1\t1\n",
    "pods": "nova-7d9f\t\t\ntrino-0\t\t\n",
    "base_domain": "dev.stardelt.io",
    "health": '{"status":"ok","db":"up"}',
    "trino_info": '{"nodeId":"trino-0","state":"ACTIVE","starting":false,"coordinator":true}',
    "oidc": '{"issuer":"https://id.dev.stardelt.io/realms/stardelt"}',
    # the product phase (#614): the four Services the operator renders
    "airflow_health": '{"metadatabase":{"status":"healthy"},"scheduler":{"status":"healthy"}}',
    "superset_health": "OK",
    "lakekeeper_health": '{"health":"ok","services":{}}',
    "lakekeeper_management_code": "401",
    "nova_page_code": "200 text/html",
    # the ingress certificate (stardelt#2190): the openssl stub answers `trusted` from a
    # transcript with `Verify return code: 0 (ok)` and an issuer line.
    "certificate": "trusted",
    "cert_issuer": "issuer=C=US, O=Let's Encrypt, CN=YR1\n",
    # lifecycle fixtures (#1019): the instance exists until `delete` removes the marker, the
    # operator owns three Deployments and the data is two PVCs plus the two Secrets.
    "instance_present": "",
    "deploy_names": "deployment.apps/nova\ndeployment.apps/keycloak\ndeployment.apps/trino\n",
    "pvc_names": "persistentvolumeclaim/data-nova-0\npersistentvolumeclaim/data-trino-0\n",
    "pvc_data-nova-0": "Bound\n",
    "pvc_data-trino-0": "Bound\n",
    "secret_stardelt-realm-admin": "",
    "secret_stardelt-s3-creds": "",
}

STUBS = {"kubectl": KUBECTL_STUB, "curl": CURL_STUB, "helm": HELM_STUB, "openssl": OPENSSL_STUB}


class Result:
    """What a run left behind: the exit code, the merged output and the fixture directory."""

    def __init__(self, code, out, artifacts):
        self.code = code
        self.out = out
        self.artifacts = artifacts


class FreshNodeTest(unittest.TestCase):
    """Runs the script against stub tools; every case starts from the all-good fixtures."""

    def run_script_full(
        self, args=(), overrides=None, remove=None, marker=None, env_extra=None, merge_streams=None
    ):
        files = dict(GOOD_FILES)
        files.update(overrides or {})
        for name in remove or []:
            files.pop(name, None)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            fake = root / "fixtures"
            fake.mkdir()
            for name, content in STUBS.items():
                path = bin_dir / name
                path.write_text(content)
                path.chmod(0o755)
            for name, content in files.items():
                (fake / name).write_text(content)
            if marker:
                (fake / marker).write_text("")
            env = dict(os.environ)
            env.pop("BASE_DOMAIN", None)
            env.update(
                {
                    "PATH": f"{bin_dir}{os.pathsep}{env.get('PATH', '')}",
                    "FAKE_DIR": str(fake),
                    "NAMESPACE": "stardelt",
                    "WAIT": "5s",
                    "PORT_WAIT_SECONDS": "2",
                }
            )
            env.update(env_extra or {})
            run_kwargs = {"capture_output": True}
            if merge_streams:
                # stderr into stdout, so "the namespace line comes before the instance line" is a
                # real ordering assertion and not two separately captured buffers.
                run_kwargs = {"stdout": subprocess.PIPE, "stderr": subprocess.STDOUT}
            done = subprocess.run(
                ["bash", str(SCRIPT), *args],
                env=env,
                text=True,
                timeout=120,
                **run_kwargs,
            )
            artifacts = {path.name: path.read_text() for path in fake.iterdir() if path.is_file()}
        merged = done.stdout if merge_streams else done.stdout + done.stderr
        return Result(done.returncode, merged, artifacts)

    def run_script(self, overrides=None, remove=None, marker=None):
        result = self.run_script_full(overrides=overrides, remove=remove, marker=marker)
        return result.code, result.out

    def test_all_good(self):
        code, out = self.run_script()
        self.assertEqual(code, 0, out)
        self.assertIn("verify-fresh-node: PASS", out)
        self.assertIn("✓ deploy 3/3 available", out)
        self.assertIn("✓ pods none waiting", out)
        self.assertIn("✓ components 4/4 (airflow, keycloak, nova, trino)", out)
        self.assertIn("✓ nova answers", out)
        self.assertIn("✓ trino answers", out)
        self.assertIn("✓ keycloak answers", out)
        self.assertIn(
            "✓ certificate id.dev.stardelt.io is trusted (C=US, O=Let's Encrypt, CN=YR1)", out
        )
        self.assertIn("? lakehouse \"not installed\"", out)
        self.assertIn("the fresh-node run still needs a person (not checked here):", out)
        self.assertIn(
            "1. sign in at https://nova.dev.stardelt.io as the realm admin "
            "(Secret stardelt-realm-admin)",
            out,
        )
        self.assertIn("2. a Trino query answers in Nova's SQL editor", out)
        self.assertIn("3. a second group is refused a table it has no grant for", out)
        self.assertIn(
            "4. a scheduled pipeline runs and its log shows in Nova; a notebook publishes",
            out,
        )

    def test_a_short_deployment_fails(self):
        code, out = self.run_script(
            overrides={"deploy": "nova\t2\t2\ntrino\t1\t1\nbroken\t\t1\n"}
        )
        self.assertEqual(code, 1, out)
        self.assertIn("✗ deploy broken 0/1", out)
        self.assertIn("verify-fresh-node: FAIL", out)

    def test_unreadable_instance_is_unverified(self):
        code, out = self.run_script(marker="instance_fail")
        self.assertEqual(code, 3, out)
        self.assertIn("? instance", out)
        self.assertIn("verify-fresh-node: UNVERIFIED", out)

    def test_nova_degraded_fails(self):
        code, out = self.run_script(overrides={"health": '{"status":"degraded"}'})
        self.assertEqual(code, 1, out)
        self.assertIn("✗ nova", out)
        self.assertIn("verify-fresh-node: FAIL", out)

    # -- the namespace's Pod Security floor (platform#1480) -------------------
    # The stub answers the three label queries from $FAKE_ENFORCE/$FAKE_WARN/$FAKE_AUDIT, which
    # default to the floor the operator writes; an empty value stands for a label that is absent.
    def test_a_namespace_with_the_floor_is_reported(self):
        result = self.run_script_full(
            merge_streams=True,
            remove=["instance_present"],
            env_extra={
                "FAKE_ENFORCE": "restricted",
                "FAKE_WARN": "restricted",
                "FAKE_AUDIT": "restricted",
            },
        )
        # No PlatformInstance on this cluster is the script's existing UNVERIFIED rule (exit 3),
        # not a failure of this check.
        self.assertEqual(result.code, 3, result.out)
        self.assertIn("✓ namespace enforce=restricted, warn/audit=restricted", result.out)
        self.assertIn("? instance", result.out)
        self.assertIn(
            "verify-fresh-node: UNVERIFIED (no PlatformInstance on this cluster)", result.out
        )
        self.assertLess(
            result.out.index("✓ namespace"),
            result.out.index("? instance"),
            "the namespace check must run before the PlatformInstance probe",
        )

    def test_a_namespace_without_the_floor_is_a_failure(self):
        result = self.run_script_full(
            merge_streams=True,
            remove=["instance_present"],
            env_extra={"FAKE_ENFORCE": "", "FAKE_WARN": "restricted", "FAKE_AUDIT": "restricted"},
        )
        self.assertEqual(result.code, 3, result.out)
        self.assertIn('✗ namespace "expected enforce=restricted', result.out)
        self.assertIn("got enforce=none warn=restricted audit=restricted", result.out)
        self.assertLess(
            result.out.index("✗ namespace"),
            result.out.index("? instance"),
            "the namespace check must run before the PlatformInstance probe",
        )

    def test_a_base_domain_that_is_not_set_prints_the_placeholder(self):
        code, out = self.run_script(remove=["base_domain"])
        self.assertEqual(code, 0, out)
        self.assertIn("1. sign in at https://nova.<base domain> as the realm admin", out)
        # Without a domain there is nothing to ask the ingress for (#2190).
        self.assertIn('? certificate "no baseDomain to check"', out)

    # -- the ingress certificate (stardelt#2190): a Let's Encrypt refusal must be the failing
    # line, not three pod logs. The openssl stub answers from $FAKE_DIR/certificate.
    def test_an_untrusted_certificate_is_a_failure(self):
        code, out = self.run_script(overrides={"certificate": "untrusted"})
        self.assertEqual(code, 1, out)
        self.assertIn('✗ certificate "id.dev.stardelt.io is not trusted for this name', out)
        self.assertIn(
            "the ingress serves its default (self-signed) certificate; an ACME rate limit? "
            "(verify error:num=18:self-signed certificate)",
            out,
        )

    def test_a_transcript_without_a_verify_line_still_fails(self):
        code, out = self.run_script(overrides={"certificate": "silent"})
        self.assertEqual(code, 1, out)
        self.assertIn("(no verify error line in the openssl output)", out)

    def test_upgrade_refuses_the_same_image(self):
        image = "ghcr.io/stardelt/stardelt-operator:dev"
        result = self.run_script_full(
            args=["upgrade"],
            overrides={"operator_image": image},
            env_extra={
                "OPERATOR_IMAGE": image,
                "OPERATOR_CHART": "/nonexistent/stardelt-operator/chart",
                "UPGRADE_WAIT": "5s",
            },
        )
        self.assertEqual(result.code, 3, result.out)
        self.assertIn("nothing to upgrade", result.out)

    def test_upgrade_sets_the_digest(self):
        digest = "sha256:" + "a" * 64
        result = self.run_script_full(
            args=["upgrade"],
            overrides={"operator_image": "ghcr.io/stardelt/stardelt-operator:dev"},
            env_extra={
                "OPERATOR_IMAGE": f"ghcr.io/stardelt/stardelt-operator@{digest}",
                "OPERATOR_CHART": "/nonexistent/stardelt-operator/chart",
                "UPGRADE_WAIT": "5s",
            },
        )
        self.assertEqual(result.code, 0, result.out)
        self.assertIn("✓ upgrade", result.out)
        self.assertIn(digest, result.out)
        argv = result.artifacts.get("helm_argv", "")
        self.assertIn(f"--set image.tag=dev@{digest}", argv)

    def test_delete_reports_a_surviving_deployment(self):
        result = self.run_script_full(args=["delete"], overrides={"deploy_nova": ""})
        self.assertEqual(result.code, 1, result.out)
        self.assertIn("✗ delete nova still present", result.out)
        self.assertIn("verify-fresh-node: FAIL", result.out)

    def test_delete_retains_secrets_and_pvcs(self):
        result = self.run_script_full(args=["delete"])
        self.assertEqual(result.code, 0, result.out)
        self.assertIn("✓ delete workloads removed, secrets and PVCs retained", result.out)

    # -- the product phase (#614): the four Services #48's acceptance names -------------------
    def test_product_phase_reports_the_four_services(self):
        result = self.run_script_full(args=["product"])
        self.assertEqual(result.code, 0, result.out)
        self.assertIn("✓ airflow healthy (metadatabase, scheduler)", result.out)
        self.assertIn("✓ superset answers OK", result.out)
        self.assertIn(
            "✓ lakekeeper healthy, and the management API refuses a request without a token (401)",
            result.out,
        )
        self.assertIn("✓ signin Nova's page is served (200, text/html)", result.out)
        self.assertIn("verify-fresh-node: PASS", result.out)

    def test_a_failed_superset_is_a_failure(self):
        result = self.run_script_full(
            args=["product"], overrides={"superset_health": "Internal Server Error"}
        )
        self.assertEqual(result.code, 1, result.out)
        self.assertIn("✗ superset", result.out)
        self.assertIn("✓ airflow healthy", result.out)
        self.assertIn("✓ lakekeeper healthy", result.out)
        self.assertIn("✓ signin Nova's page is served", result.out)

    def test_lakekeeper_that_answers_without_a_token_is_a_failure(self):
        result = self.run_script_full(
            args=["product"], overrides={"lakekeeper_management_code": "200"}
        )
        self.assertEqual(result.code, 1, result.out)
        self.assertIn("the management API answers without a token", result.out)

    def test_an_unhealthy_airflow_is_a_failure(self):
        result = self.run_script_full(
            args=["product"],
            overrides={
                "airflow_health": '{"metadatabase":{"status":"healthy"},'
                '"scheduler":{"status":"unhealthy"}}'
            },
        )
        self.assertEqual(result.code, 1, result.out)
        self.assertIn("✗ airflow", result.out)


class IngressCertificateTest(unittest.TestCase):
    """scripts/check-ingress-certificate.sh against the openssl stub: no network is touched."""

    def run_cert(self, args, mode="trusted"):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            fake = root / "fixtures"
            fake.mkdir()
            stub = bin_dir / "openssl"
            stub.write_text(OPENSSL_STUB)
            stub.chmod(0o755)
            (fake / "certificate").write_text(mode)
            (fake / "cert_issuer").write_text("issuer=C=US, O=Let's Encrypt, CN=YR1\n")
            env = dict(os.environ)
            env.update(
                {"PATH": f"{bin_dir}{os.pathsep}{env.get('PATH', '')}", "FAKE_DIR": str(fake)}
            )
            return subprocess.run(
                ["bash", str(CERT_SCRIPT), *args], env=env, text=True, capture_output=True
            )

    def test_a_trusted_name_names_the_issuer(self):
        done = self.run_cert(["id.dev.stardelt.io"])
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(
            done.stdout,
            "id.dev.stardelt.io is trusted (C=US, O=Let's Encrypt, CN=YR1)\n",
        )

    def test_a_name_the_ingress_does_not_serve_names_the_acme_rate_limit(self):
        done = self.run_cert(["no-such-host.dev.stardelt.io", "dev.stardelt.io"], mode="untrusted")
        self.assertEqual(done.returncode, 1, done.stderr)
        self.assertEqual(
            done.stdout,
            "no-such-host.dev.stardelt.io is not trusted for this name — the ingress serves its "
            "default (self-signed) certificate; an ACME rate limit? "
            "(verify error:num=18:self-signed certificate)\n",
        )

    def test_a_transcript_without_a_verify_line_still_fails(self):
        done = self.run_cert(["id.dev.stardelt.io"], mode="silent")
        self.assertEqual(done.returncode, 1, done.stderr)
        self.assertIn("(no verify error line in the openssl output)", done.stdout)

    def test_no_openssl_is_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            # A PATH that holds no openssl: the script must not try the network.
            done = subprocess.run(
                [shutil.which("bash"), str(CERT_SCRIPT), "id.dev.stardelt.io"],
                env={"PATH": tmp},
                text=True,
                capture_output=True,
            )
        self.assertEqual(done.returncode, 3, done.stderr)
        self.assertIn("openssl is not installed", done.stderr)
        self.assertEqual(done.stdout, "")


if __name__ == "__main__":
    unittest.main()
