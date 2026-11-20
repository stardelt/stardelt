"""A `quality` task's checks: SQL assertions that pass when they return no rows (#114)."""
from __future__ import annotations

import logging
from dataclasses import dataclass

SEVERITIES = ("error", "warn")

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    name: str
    sql: str
    severity: str = "error"


def evaluate(checks, run_one, log=None):
    """Run every check; `run_one(sql)` returns the number of rows the assertion found.

    A check passes at zero rows. An `error` check with rows fails the task; a `warn` check
    with rows is logged and the task continues. Returns the failed checks as a list of
    `(name, rows)` — empty means the gate passed. Every result is logged as one line:
    `check <name>: <severity>, <rows> row(s)`.
    """
    logger = log if log is not None else _log
    failed: list[tuple[str, int]] = []
    for check in checks:
        rows = run_one(check.sql)
        logger.info("check %s: %s, %d row(s)", check.name, check.severity, rows)
        if check.severity == "error" and rows > 0:
            failed.append((check.name, rows))
    return failed
