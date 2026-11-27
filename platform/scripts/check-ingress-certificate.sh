#!/usr/bin/env bash
# Say whether the ingress serves a certificate a client trusts for a name (stardelt/stardelt#2190).
#
#   scripts/check-ingress-certificate.sh <host> [connect-host]
#
# `connect-host` defaults to `<host>`; it lets a test ask one address for a name it does not serve.
# Exit codes: 0 trusted, 1 not trusted for this name, 3 openssl is not installed.
set -euo pipefail

host="${1:?usage: check-ingress-certificate.sh <host> [connect-host]}"
connect="${2:-$host}"

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is not installed" >&2
  exit 3
fi

probe="$(openssl s_client -connect "${connect}:443" -servername "$host" -verify_return_error \
  -verify_hostname "$host" </dev/null 2>&1 || true)"
issuer_line="$(openssl s_client -connect "${connect}:443" -servername "$host" -verify_return_error \
  -verify_hostname "$host" </dev/null 2>/dev/null | openssl x509 -noout -issuer 2>/dev/null || true)"

case "$probe" in
  *"Verify return code: 0 (ok)"*)
    printf '%s is trusted (%s)\n' "$host" "${issuer_line#issuer=}"
    exit 0
    ;;
esac

verify_err="$(printf '%s\n' "$probe" | grep -m1 'verify error' || true)"
printf '%s is not trusted for this name — the ingress serves its default (self-signed) certificate; an ACME rate limit? (%s)\n' \
  "$host" "${verify_err:-no verify error line in the openssl output}"
exit 1
