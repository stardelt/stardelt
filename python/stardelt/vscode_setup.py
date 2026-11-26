"""`stardelt setup vscode`: the kernelspec that runs a notebook as you, on your laptop.

The written `kernel.json` starts `python -m stardelt.kernel` and carries the Nova address and the
Spark Connect endpoint in its `env`; the token stays a placeholder, because the kernel resolves the
stored sign-in itself — nothing secret is written here.
"""

import json
import re
import sys
from pathlib import Path

from stardelt import login

TOKEN_PLACEHOLDER = "__TOKEN__"


def data_dir(environ):
    """Jupyter's data directory: `$JUPYTER_DATA_DIR`, else `$XDG_DATA_HOME/jupyter`, else `$HOME/.local/share/jupyter`."""
    if environ.get("JUPYTER_DATA_DIR"):
        return Path(environ["JUPYTER_DATA_DIR"])
    if environ.get("XDG_DATA_HOME"):
        return Path(environ["XDG_DATA_HOME"]) / "jupyter"
    if environ.get("HOME"):
        return Path(environ["HOME"]) / ".local" / "share" / "jupyter"
    raise ValueError("no data directory: set JUPYTER_DATA_DIR or XDG_DATA_HOME")


def nova_host(environ, host=None):
    """Nova's address: the argument, else `$STARDELT_NOVA_URL`, else the host of the stored sign-in."""
    address = host or environ.get("STARDELT_NOVA_URL")
    if not address:
        stored = login.load(environ)
        address = (stored or {}).get("host")
    if not address:
        raise ValueError("no Nova address: pass --host, or run `stardelt --host https://<nova> login`")
    return address.rstrip("/")


def spark_host(nova, override=None):
    """The Spark Connect host: the argument, else Nova's hostname with `nova.` dropped and `spark.` prefixed."""
    if override:
        return override.rstrip("/")
    hostname = nova.split("://", 1)[-1].split("/", 1)[0]
    if hostname.startswith("nova."):
        hostname = hostname[len("nova.") :]
    return "spark." + hostname


_spark_host_of = spark_host  # `install()` takes a parameter of that name, so it cannot call it directly


def remote_for(spark):
    """The `SPARK_REMOTE` value the kernel starts from; the token placeholder is resolved when a session opens."""
    return f"sc://{spark}:443/;use_ssl=true;token={TOKEN_PLACEHOLDER}"


def name_for(nova):
    """The kernelspec's directory name: `stardelt-` plus Nova's hostname, with everything else as `-`."""
    hostname = nova.split("://", 1)[-1].split("/", 1)[0]
    return "stardelt-" + re.sub(r"[^a-z0-9]+", "-", hostname.lower()).strip("-")


def install(environ, host=None, spark_host=None):
    """Write the kernelspec and return the path of its `kernel.json`."""
    nova = nova_host(environ, host)
    spark = _spark_host_of(nova, spark_host)
    spec = {
        "argv": [sys.executable, "-m", "stardelt.kernel", "-f", "{connection_file}"],
        "display_name": f"stardelt — {nova.split('://', 1)[-1].split('/', 1)[0]}",
        "language": "python",
        "env": {"STARDELT_NOVA_URL": nova, "SPARK_REMOTE": remote_for(spark)},
    }
    path = data_dir(environ) / "kernels" / name_for(nova) / "kernel.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(spec, indent=2) + "\n")
    return path
