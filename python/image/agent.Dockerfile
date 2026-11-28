# The agent harness (stardelt/stardelt#2344 slice 3b): `stardelt agent` with the Spark Connect
# client and dbt, the same image a person runs locally and the platform runs managed.
# No credential is baked in: sign in inside, or mount ~/.config/stardelt read-only.
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/home/agent

ARG REVISION=
LABEL org.opencontainers.image.title="stardelt-agent" \
      org.opencontainers.image.source="https://github.com/stardelt/stardelt" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision=$REVISION

COPY . /app
RUN pip install --no-cache-dir "/app[spark]" "dbt-core==1.11.17" "dbt-trino==1.10.6" \
    && mkdir -p /home/agent/.config /work \
    && chown -R 65532:65532 /home/agent /work

USER 65532:65532
WORKDIR /work
ENTRYPOINT ["stardelt"]
CMD ["agent"]
