"""Suggest a class for a table's unclassed columns, from names and types (#491 slice 3).

The four classes are the ones the Columns tab offers (#2172): ``public``, ``personal``,
``financial`` and ``secret``. A class is a table property (``stardelt.column-classes``,
#2171), so the suggestion is written with the documentation update the table page uses —
one PUT that replaces the map. A column that already carries a class is never overwritten:
a person's decision outlives a rule.

The rules read the column's name only, which is the design's proposal for the default
("names and types by default; sampling is opt-in", #491 Q3). They are deliberately narrow:
a wrong suggestion a person must reject is worse than a missed one.
"""

import re

from stardelt import api

CLASSES = ("public", "personal", "financial", "secret")

# The first rule that matches a name wins; the list is ordered from the narrowest. The
# names are the design's examples (#491): "names and types by default".
RULES = (
    ("secret", re.compile(r"(^|_)(pass(word|wd)?|secret|token|api_?key|private_?key)($|_)", re.IGNORECASE)),
    ("financial", re.compile(r"(iban|bic|credit_?card|card_?number|account_?number|salary)", re.IGNORECASE)),
    ("personal", re.compile(r"(e?mail|phone|mobile|birth(_?date|day)?|ssn|tax_?id|address|postcode|zip_?code)", re.IGNORECASE)),
)


def suggest(name):
    """The class a column's name suggests, or ``None``."""
    for wanted, rule in RULES:
        if rule.search(name or ""):
            return wanted
    return None


def proposals(columns):
    """``[{"column", "class"}]`` for the columns that carry no class, in schema order."""
    out = []
    for column in columns:
        if column.get("class"):
            continue
        wanted = suggest(column.get("name"))
        if wanted:
            out.append({"column": column["name"], "class": wanted})
    return out


def scan(nova, table, apply=False, owner=None):
    """Read one table's columns and (with ``apply``) write the suggested classes.

    ``table`` is ``catalog.schema.name``. Answers ``(problems, rows)``: each row
    ``{"column", "class", "action"}`` with action ``would-set`` (the default) or ``set``
    (with ``apply``); a column that already has a class is absent from the rows and a
    problem explains a refusal.
    """
    parts = (table or "").split(".")
    if len(parts) != 3 or not all(parts):
        return [("classify", "name the table as catalog.schema.table")], []
    catalog, ns, name = parts
    try:
        doc = nova.call(
            "table_documentation_get",
            path={"ns": ns, "tbl": name},
            query={"catalog": catalog},
        )
    except api.StardeltApiError as err:
        return [("classify", str(err))], []
    if not doc.get("canEdit"):
        return [("classify", doc.get("editRule") or "you may not edit this table")], []
    if not doc.get("owner") and not owner:
        return [("classify", "the table has no owner; pass --owner with one of your groups")], []
    found = proposals(doc.get("columns") or [])
    if not found:
        return [], []
    if not apply:
        return [], [{**row, "action": "would-set"} for row in found]
    existing = {
        column["name"]: column["class"]
        for column in doc.get("columns") or []
        if column.get("class")
    }
    merged = {**existing, **{row["column"]: row["class"] for row in found}}
    body = {"columnClasses": merged}
    if not doc.get("owner"):
        body["owner"] = owner
    try:
        nova.call(
            "table_documentation_update",
            path={"ns": ns, "tbl": name},
            query={"catalog": catalog},
            body=body,
        )
    except api.StardeltApiError as err:
        return [("classify", str(err))], []
    return [], [{**row, "action": "set"} for row in found]
