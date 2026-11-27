"""Run the catalog's evals (#125): ask, execute read-only, post the verdict."""

from stardelt import api, statements


class EvalRunError(RuntimeError):
    """The run could not be recorded; the message is one sentence for the person."""


def list_evals(nova, catalog):
    return nova.call("catalog_eval_list", query={"catalog": catalog})


def ask_and_run(nova, ev, catalog):
    """One eval: the answer body slice 2 expects, or an ``error`` body."""
    answer = nova.call("ask_answer", body={"question": ev["question"]}, query={"catalog": catalog})
    body = {"model": answer.get("model"), "endpoint": answer.get("endpoint")}
    sql = answer.get("sql")
    if not sql:
        body["error"] = (answer.get("sentence") or "Ask answered no SQL")[:2000]
        return body
    body["sql"] = sql
    try:
        result = statements.run(nova, sql)
    except (statements.StatementError, api.StardeltApiError) as e:
        body["error"] = str(e)[:2000]
        return body
    if result.truncated:
        body["error"] = "the answer was truncated"
        return body
    body["rows"] = [list(row) for row in result.rows]
    return body


def run(nova, catalog, only=None):
    """Every eval of the catalog: returns ``[(eval, run_row), …]``."""
    out = []
    for ev in list_evals(nova, catalog):
        if only is not None and ev["id"] != only:
            continue
        body = ask_and_run(nova, ev, catalog)
        out.append((ev, nova.call("eval_run_create", path={"id": ev["id"]}, body=body)))
    return out
