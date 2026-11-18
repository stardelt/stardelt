"""Run-time expansion of `${run.date}`, `${run.id}` and `${params.<name>}` in task statements."""
from __future__ import annotations

import datetime
import re
from collections.abc import Mapping

_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_PARAM_NAME = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
MAX_RUN_PARAMS = 16
MAX_RUN_PARAM_VALUE = 256


def expand(text: str, *, run_date: str, run_id: str, params: dict[str, str]) -> str:
    """Replace every placeholder; an unknown one is an error, never silently left in the SQL."""
    values = {"run.date": run_date, "run.id": run_id}
    values.update({f"params.{key}": value for key, value in params.items()})

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise ValueError(f"unknown placeholder ${{{key}}}")
        return values[key]

    return _PLACEHOLDER.sub(replace, text)


def run_values(
    conf: Mapping[str, object] | None, fallback_date: str
) -> tuple[str, dict[str, str]]:
    """The run date and parameter overrides a manual trigger carried, or `(fallback_date, {})`."""
    if not isinstance(conf, Mapping):
        return fallback_date, {}

    run_date = fallback_date
    raw_date = conf.get("runDate")
    if raw_date is not None:
        if not isinstance(raw_date, str) or not _DATE.match(raw_date):
            raise ValueError("runDate is a date (YYYY-MM-DD)")
        try:
            datetime.date.fromisoformat(raw_date)
        except ValueError:
            raise ValueError("runDate is a date (YYYY-MM-DD)") from None
        run_date = raw_date

    parameters = ("the run's parameters are at most 16 lower-case names with values of at "
                  "most 256 characters")
    overrides: dict[str, str] = {}
    raw_params = conf.get("params")
    if raw_params is not None:
        if not isinstance(raw_params, Mapping) or len(raw_params) > MAX_RUN_PARAMS:
            raise ValueError(parameters)
        for key, value in raw_params.items():
            if not isinstance(key, str) or not _PARAM_NAME.match(key):
                raise ValueError(parameters)
            if (
                not isinstance(value, str)
                or len(value) > MAX_RUN_PARAM_VALUE
                or "\x00" in value
            ):
                raise ValueError(parameters)
            overrides[key] = value

    # Unknown keys in `conf` are ignored: Nova owns that schema.
    return run_date, overrides
