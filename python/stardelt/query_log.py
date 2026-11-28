"""Archive the query log before Nova's retention deletes it (#2262; #31's read half)."""

import json

from stardelt import api


class ExportError(RuntimeError):
    """The export stopped; the message names the file and the cursor to continue from."""


def export(nova, out, *, from_ms=None, cursor=None, limit=None, out_path=None):
    """Page through ``query_log_export`` and write one JSON object per row.

    ``out`` is a text file opened by the caller; ``out_path`` is that file's name, so
    the failure sentence can say where the rows went. Answers ``{"rows": <written>,
    "nextCursor": <cursor or None>}``. A request that fails raises ``ExportError``
    naming how many rows were written and the cursor of the next page, so the run can
    continue with ``--cursor``.
    """
    written, pages = 0, 0
    while True:
        query = {}
        if from_ms is not None and pages == 0:
            query["from"] = from_ms
        if cursor:
            query["cursor"] = cursor
        if limit is not None:
            query["limit"] = limit
        try:
            page = nova.call("query_log_export", query=query) or {}
        except api.StardeltApiError as err:
            where = f" to {out_path}" if out_path else ""
            reach = f"continue with --cursor {cursor}" if cursor else "restart with --from"
            raise ExportError(f"{written} rows were written{where}; {reach}: {err}") from err
        rows = page.get("rows") or []
        for row in rows:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
        written += len(rows)
        pages += 1
        cursor = page.get("nextCursor")
        if not cursor:
            return {"rows": written, "nextCursor": None}
