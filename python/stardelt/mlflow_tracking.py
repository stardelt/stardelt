"""Point a notebook at the group's MLflow tracking server (#40).

    import stardelt
    stardelt.mlflow()                 # the connection named "mlflow"
    import mlflow
    mlflow.autolog()

The connection is read through :func:`stardelt.connections.connection`, so the store decides
who may use it and the token is read when this is called, never kept here. The function sets
``MLFLOW_TRACKING_URI`` and, when the connection carries a ``token`` field,
``MLFLOW_TRACKING_TOKEN``; the MLflow client reads both. Nothing is installed and no model is
served: #40 connects a customer's server.

SPDX-License-Identifier: MIT
"""

import os

from stardelt.connections import ConnectionUnavailable, connection

URI_ENV = "MLFLOW_TRACKING_URI"
TOKEN_ENV = "MLFLOW_TRACKING_TOKEN"


def mlflow(name="mlflow", *, scope=None, group=None):
    """Set the MLflow tracking environment from the connection ``name``; return its URL."""
    conn = connection(name, scope=scope, group=group)
    if conn.type != "mlflow":
        raise ConnectionUnavailable(
            f"connection {name!r} is a {conn.type!r} connection, not an MLflow tracking server"
        )
    url = str(conn.settings.get("trackingUrl") or "").strip()
    if not url:
        raise ConnectionUnavailable(f"connection {name!r} has no trackingUrl setting")
    os.environ[URI_ENV] = url
    if "token" in conn.keys:
        os.environ[TOKEN_ENV] = conn.value("token")
    else:
        os.environ.pop(TOKEN_ENV, None)
    return url
