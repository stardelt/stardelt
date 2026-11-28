#!/usr/bin/env bash
# The shipped CLI signs in as the demo visitor and uses the platform (stardelt/stardelt#1483).
#
#   make verify-cli
#   bash scripts/verify-cli.sh            (STARDELT_NOVA_URL=…, ISSUER=… to override)
#
# `verify-product-path.sh` walks the HTTP contract, `verify-mcp.sh`/`verify-mcp-tools.sh` the
# hosted agent surface and `verify-pipeline-validate.sh` the bundle validators offline — no row
# runs the CLI the product ships (D24's third leg, `python/pyproject.toml`'s
# `[project.scripts] stardelt`). Without this row a release can ship with `stardelt sql` broken,
# the Skill empty or `bundle validate` failing and every row stays green.
#
#   sign-in     `stardelt whoami`             the minted token: demo, visitor, `via` bearer
#   sql         `stardelt sql "SELECT …"`     customers = 200 from bronze.shop.customers
#   api         `stardelt api --list`         the shipped operations (at least 100)
#   bundle      `stardelt bundle validate …`  dev and prod, the bundle's pipelines each, offline
#   skill       `stardelt skill`              the agent Skill (at least 90 lines, sql_read …)
#   history     `stardelt deployments list`   the deployment record's header, exit 0
#   signed-out  `stardelt whoami`             exit 3 and the sign-in hint, with no token
#
# The password is `demo` and it is PUBLIC ON PURPOSE: `scripts/public-demo.sh` creates that
# person and prints it, so it protects nothing. The password and the token live in two `mktemp`
# files (0600, removed on every exit) and nothing here prints the token. The token comes from
# `scripts/mint-user-token.py` on the public client `stardelt-dev` — the client `stardelt login`
# uses — so the browser device flow is not needed here; its own code stays unit-tested in
# `python/tests/test_login.py`.
#
# The CLI is installed into a cached virtualenv (STARDELT_CLI_VENV, default
# `$TMPDIR/stardelt-cli-venv`), so a second run installs nothing. `bundle`, `skill` and
# `signed-out` need no cluster; the other rows do.
#
# Exit 0 = PASS (7 of 7), 1 = FAIL (every row still runs; each ✗ line names why). The number in
# the last line is the rows that produced a verdict: after the sign-in row fails, every later
# row prints `not run` and does not count, so a dead host reads `FAIL (1 of 7)`.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"; NOVA="${NOVA%/}"
ISSUER="${ISSUER:-https://id.alpha.dev.stardelt.io/realms/stardelt}"; ISSUER="${ISSUER%/}"
repo_dir="$(cd .. && pwd)"          # platform/ is a folder of the product repository

# The CLI: one virtualenv, cached by path, installed from this checkout's python/ folder.
VENV="${STARDELT_CLI_VENV:-${TMPDIR:-/tmp}/stardelt-cli-venv}"
if [ ! -x "$VENV/bin/stardelt" ]; then
  python3 -m venv "$VENV" || { echo "verify-cli: FAIL cannot create $VENV"; exit 1; }
  "$VENV/bin/pip" install --quiet --disable-pip-version-check -e "$repo_dir/python" \
    || { echo "verify-cli: FAIL pip install of $repo_dir/python"; exit 1; }
fi
CLI="$VENV/bin/stardelt"

umask 077
tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-cli.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
trap 'exit 130' INT TERM

PASSWORD_FILE="$tmp/password"
TOKEN_FILE="$tmp/token"
BODY_FILE="$tmp/body"
OUT_FILE="$tmp/out"
ERR_FILE="$tmp/err"
: > "$PASSWORD_FILE"
: > "$TOKEN_FILE"
: > "$BODY_FILE"
: > "$OUT_FILE"
: > "$ERR_FILE"
chmod 600 "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE" "$OUT_FILE" "$ERR_FILE"
# `mint-user-token.py` strips the trailing newline; the realm compares the exact string.
printf 'demo\n' > "$PASSWORD_FILE"

PASSED=0
FAILED=0
RAN=0
ok()  { printf '✓ %-14s %s\n' "$1" "$2"; PASSED=$((PASSED + 1)); RAN=$((RAN + 1)); }
bad() {
  printf '✗ %-14s %s\n' "$1" "$2"
  FAILED=$((FAILED + 1))
  case "$2" in "not run"*) ;; *) RAN=$((RAN + 1)) ;; esac
}
not_run() { bad "$1" "not run: sign-in minted no token"; }
first_line() { printf '%s\n' "$1" | head -n 1 | cut -c1-200; }

# cli <args…>: run the shipped CLI. stdout in $out, stderr in $err, exit code in $rc.
out=""
err=""
rc=0
cli() {
  : > "$OUT_FILE"
  : > "$ERR_FILE"
  if "$CLI" "$@" >"$OUT_FILE" 2>"$ERR_FILE"; then rc=0; else rc=$?; fi
  out="$(cat "$OUT_FILE")"
  err="$(cat "$ERR_FILE")"
}

# ---- the token: minted once, on the public client the CLI signs in with ----------------------

TOKEN_OK=0
if ! ISSUER="$ISSUER" USER=demo PASSWORD_FILE="$PASSWORD_FILE" TOKEN_FILE="$TOKEN_FILE" \
       CLIENT_ID=stardelt-dev python3 scripts/mint-user-token.py >"$tmp/mint.log" 2>&1; then
  bad sign-in "cannot mint a token for demo: $(first_line "$(cat "$tmp/mint.log")")"
elif [ ! -s "$TOKEN_FILE" ]; then
  bad sign-in "minting a token for demo wrote no token"
else
  STARDELT_TOKEN="$(cat "$TOKEN_FILE")"
  export STARDELT_TOKEN
  export STARDELT_NOVA_URL="$NOVA"
  TOKEN_OK=1
fi

# ---- signed-out: the CLI without a token refuses before it asks the host ----------------------
# `env -u` removes the token and HOME is an empty directory, so no stored sign-in is found either.

EMPTY_HOME="$tmp/empty-home"
mkdir -p "$EMPTY_HOME"
chmod 700 "$EMPTY_HOME"
SIGN_IN_HINT="authentication needed: run \`stardelt login\` or set STARDELT_TOKEN"

row_signed_out() {
  if env -u STARDELT_TOKEN HOME="$EMPTY_HOME" "$CLI" whoami >"$OUT_FILE" 2>"$ERR_FILE"; then
    rc=0
  else
    rc=$?
  fi
  out="$(cat "$OUT_FILE")"
  err="$(cat "$ERR_FILE")"
  if [ "$rc" != 3 ]; then
    bad signed-out "whoami without a token exited $rc, not 3: $(first_line "$err")"
  elif ! printf '%s\n' "$err" | grep -qF "$SIGN_IN_HINT"; then
    bad signed-out "whoami without a token did not name the sign-in hint: $(first_line "$err")"
  elif [ -n "$out" ]; then
    bad signed-out "whoami without a token still printed an answer: $(first_line "$out")"
  else
    ok signed-out "exit 3 and the sign-in hint, with no token"
  fi
}

# ---- sign-in: the CLI says who Nova thinks the token belongs to -------------------------------

row_sign_in() {
  cli whoami
  if [ "$rc" != 0 ]; then
    bad sign-in "whoami exited $rc: $(first_line "$err")"
    TOKEN_OK=0
  elif ! printf '%s\n' "$out" | grep -q '"user": "demo"'; then
    bad sign-in "whoami is not demo: $(first_line "$out")"
    TOKEN_OK=0
  elif ! printf '%s\n' "$out" | grep -q '"visitors"'; then
    bad sign-in "whoami does not name the visitors group: $(first_line "$out")"
    TOKEN_OK=0
  elif ! printf '%s\n' "$out" | grep -q '"via": "bearer"'; then
    bad sign-in "whoami does not name the bearer source: $(first_line "$out")"
    TOKEN_OK=0
  else
    ok sign-in "whoami as demo (visitors, via bearer)"
  fi
}

# ---- sql: one number out of a table the visitor may read --------------------------------------

row_sql() {
  cli sql "SELECT count(*) AS customers FROM bronze.shop.customers"
  if [ "$rc" != 0 ]; then
    bad sql "sql exited $rc: $(first_line "$err")"
  elif printf '%s\n' "$out" | awk 'wanted && $0 == "200" { found = 1 } $0 == "customers" { wanted = 1; next } { wanted = 0 } END { exit !found }'; then
    ok sql "customers=200"
  else
    bad sql "the answer is not a customers column of 200: $(first_line "$out")"
  fi
}

# ---- api: the operations the CLI advertises ---------------------------------------------------

row_api() {
  cli api --list
  if [ "$rc" != 0 ]; then
    bad api "api --list exited $rc: $(first_line "$err")"
    return
  fi
  local count
  count="$(wc -l < "$OUT_FILE")"
  if [ "$count" -ge 100 ]; then
    ok api "$count operations"
  else
    bad api "api --list lists $count operations, fewer than 100"
  fi
}

# ---- bundle: the sample bundle validates offline ----------------------------------------------

row_bundle() {
  cli bundle validate "$repo_dir/demos/examples/shop-bundle"
  if [ "$rc" != 0 ]; then
    bad bundle "bundle validate exited $rc: $(first_line "$err")"
    return
  fi
  local pipelines
  pipelines="$(for f in "$repo_dir"/demos/examples/shop-bundle/resources/*.yaml; do
    awk '/^name:/ { sub(/^name:[[:space:]]*/, ""); print; exit }' "$f"
  done | paste -sd, - | sed 's/,/, /g')"
  if [ -z "$pipelines" ]; then
    bad bundle "no pipeline name was read from $repo_dir/demos/examples/shop-bundle/resources"
    return
  fi
  if printf '%s\n' "$out" | grep -qxF "dev (development): $pipelines" \
     && printf '%s\n' "$out" | grep -qxF "prod (production): $pipelines"; then
    ok bundle "dev and prod: $pipelines"
  else
    bad bundle "the two targets are not dev/prod with $pipelines: $(first_line "$out")"
  fi
}

# ---- skill: the agent Skill the release ships --------------------------------------------------

row_skill() {
  cli skill
  if [ "$rc" != 0 ]; then
    bad skill "skill exited $rc: $(first_line "$err")"
    return
  fi
  local count missing
  count="$(wc -l < "$OUT_FILE")"
  missing=""
  for name in sql_read sql_explain catalog_search; do
    printf '%s\n' "$out" | grep -q "$name" || missing="$missing $name"
  done
  if [ -n "$missing" ]; then
    bad skill "the Skill does not name$missing ($count lines)"
  elif [ "$count" -lt 90 ]; then
    bad skill "the Skill is $count lines, fewer than 90"
  else
    ok skill "$count lines, sql_read/sql_explain/catalog_search named"
  fi
}

# ---- history: the deployment record's read half, and its empty answer is not an error ---------

row_history() {
  cli deployments list
  if [ "$rc" != 0 ]; then
    bad history "deployments list exited $rc: $(first_line "$err")"
    return
  fi
  local header
  header="$(printf '%s\n' "$out" | head -n 1)"
  if [ "$header" = "id  bundle  target  commit  status  by  when" ]; then
    if [ -n "$err" ]; then
      ok history "header printed, exit 0 ($(first_line "$err"))"
    else
      ok history "header printed, exit 0"
    fi
  else
    bad history "stdout starts with '$(printf '%s' "$header")', not the record's header"
  fi
}

# ---- the seven rows, in the order the header names them ----------------------------------------

if [ "$TOKEN_OK" != 1 ]; then
  not_run sign-in
  not_run sql
  not_run api
  not_run bundle
  not_run skill
  not_run history
  not_run signed-out
else
  row_sign_in
  if [ "$TOKEN_OK" != 1 ]; then
    not_run sql
    not_run api
    not_run bundle
    not_run skill
    not_run history
    not_run signed-out
  else
    row_sql
    row_api
    row_bundle
    row_skill
    row_history
    row_signed_out
  fi
fi

if [ "$PASSED" -eq 7 ] && [ "$FAILED" -eq 0 ]; then
  echo "verify-cli: PASS (7 of 7)"
  exit 0
fi
echo "verify-cli: FAIL ($RAN of 7)"
exit 1
