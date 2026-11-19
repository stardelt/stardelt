# The dbt tool image: dbt-core + the Trino adapter in their own virtualenv, plus the
# licence gate that keeps the dependency tree permissive (see README.md and
# demos/images/dbt/license-exceptions.txt).
#
# Base: pinned by digest. Re-check with `docker buildx imagetools inspect python:3.12-slim`;
# replace the digest only when it stops resolving, and say so in the PR.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b AS licences
COPY images/dbt/check-licenses.py /opt/check-licenses.py
COPY images/dbt/license-exceptions.txt /opt/license-exceptions.txt
# The exceptions file starts with a header line of prose (`# …`), so the file cannot live
# inside the runtime image as a pip requirement; the gate reads it directly.
# hadolint ignore=DL3013
RUN python -m pip install --no-cache-dir pip-licenses \
    && python -m pip install --no-cache-dir --only-binary=:all: \
         "dbt-core==1.11.17" "dbt-trino==1.10.6" \
    && pip-licenses --format=json > /opt/licenses.json \
    && pip-licenses --format=plain-vertical --with-license-file --no-license-path > /opt/LICENSES.txt \
    && python /opt/check-licenses.py /opt/licenses.json /opt/license-exceptions.txt

FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
RUN python -m venv /opt/dbt \
    && /opt/dbt/bin/pip install --no-cache-dir --only-binary=:all: "dbt-core==1.11.17" "dbt-trino==1.10.6" \
    && /opt/dbt/bin/python -m pip check \
    && /opt/dbt/bin/dbt --version
# The sample project the demo's `dbt` task names (`project: shop` in its config). Customer
# projects arrive as pinned files later (#104). Read-only for the pod: dbt writes /tmp only.
COPY examples/shop /opt/dbt/projects/shop
COPY --from=licences /opt/LICENSES.txt /opt/licenses.json /opt/check-licenses.py /opt/license-exceptions.txt /opt/dbt/
COPY images/dbt/stardelt-dbt /usr/local/bin/stardelt-dbt
RUN chmod 0755 /usr/local/bin/stardelt-dbt
# A pipeline mounts the project read-only (or root-owned), so dbt's default `logs/` and
# `target/` inside the project would fail to write: both live in /tmp, which uid 1000 owns.
ENV PATH=/opt/dbt/bin:$PATH \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DBT_USE_COLORS=false \
    DBT_LOG_PATH=/tmp/dbt-logs \
    DBT_TARGET_PATH=/tmp/dbt-target
USER 1000
WORKDIR /work
ENTRYPOINT ["/usr/local/bin/stardelt-dbt"]
