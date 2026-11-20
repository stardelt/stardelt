"""The demo's report notebook: valid nbformat, reads the demo's gold tables, writes nothing."""

import json
from pathlib import Path

NOTEBOOK = Path(__file__).resolve().parents[1] / "examples" / "shop" / "notebooks" / "shop-report.ipynb"


def document():
    return json.loads(NOTEBOOK.read_text(encoding="utf-8"))


def test_the_report_is_a_valid_nbformat_4_notebook():
    doc = document()
    assert doc["nbformat"] == 4 and doc["nbformat_minor"] >= 5
    for cell in doc["cells"]:
        assert cell["cell_type"] in ("markdown", "code")
        assert cell["id"]


def test_the_report_reads_the_demo_gold_tables_and_writes_nothing():
    code = "\n".join(
        "".join(cell["source"]) for cell in document()["cells"] if cell["cell_type"] == "code"
    )
    assert "%sql" in code
    assert "gold.shop.daily_revenue" in code and "gold.shop.customer_ltv" in code
    lowered = code.lower()
    assert not any(word in lowered for word in ("insert ", "create ", "update ", "delete ", "drop "))
