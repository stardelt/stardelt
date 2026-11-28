"""`stardelt classify columns`: a class suggested from a column's name, written only with --apply (#491)."""

from stardelt import classify


class Nova:
    def __init__(self, doc):
        self.doc = doc
        self.calls = []

    def call(self, op, path=None, query=None, body=None):
        self.calls.append((op, path, query, body))
        if op == "table_documentation_get":
            return self.doc
        if op == "table_documentation_update":
            return self.doc
        raise AssertionError(op)


def doc(columns, owner="shop", can_edit=True, edit_rule=None):
    return {
        "columns": columns,
        "owner": owner,
        "canEdit": can_edit,
        "editRule": edit_rule,
    }


def test_a_telling_name_is_proposed_and_a_plain_one_is_not():
    assert classify.suggest("email") == "personal"
    assert classify.suggest("password") == "secret"
    assert classify.suggest("iban") == "financial"
    assert classify.suggest("id") is None
    assert classify.suggest("amount") is None


def test_a_column_that_already_carries_a_class_is_never_proposed():
    nova = Nova(doc([
        {"name": "email", "r#type": "varchar", "class": "public"},
        {"name": "phone", "r#type": "varchar"},
    ]))
    problems, rows = classify.scan(nova, "gold.shop.customers", apply=True)
    assert problems == []
    assert rows == [{"column": "phone", "class": "personal", "action": "set"}]
    update = [call for call in nova.calls if call[0] == "table_documentation_update"]
    assert len(update) == 1
    assert update[0][3] == {"columnClasses": {"email": "public", "phone": "personal"}}


def test_the_default_writes_nothing():
    nova = Nova(doc([
        {"name": "email", "r#type": "varchar"},
        {"name": "id", "r#type": "bigint"},
    ]))
    problems, rows = classify.scan(nova, "gold.shop.customers")
    assert problems == []
    assert rows == [{"column": "email", "class": "personal", "action": "would-set"}]
    assert [call[0] for call in nova.calls] == ["table_documentation_get"]


def test_apply_writes_one_map_with_the_existing_classes():
    nova = Nova(doc([
        {"name": "email", "r#type": "varchar", "class": "public"},
        {"name": "password", "r#type": "varchar"},
        {"name": "amount", "r#type": "decimal"},
    ]))
    problems, rows = classify.scan(nova, "gold.shop.customers", apply=True)
    assert problems == []
    assert rows == [{"column": "password", "class": "secret", "action": "set"}]
    update = [call for call in nova.calls if call[0] == "table_documentation_update"]
    assert len(update) == 1
    assert update[0][3] == {"columnClasses": {"email": "public", "password": "secret"}}


def test_an_ownerless_table_needs_an_owner():
    nova = Nova(doc([{"name": "email", "r#type": "varchar"}], owner=None))
    problems, rows = classify.scan(nova, "gold.shop.customers")
    assert problems == [("classify", "the table has no owner; pass --owner with one of your groups")]
    assert rows == []
    assert [call[0] for call in nova.calls] == ["table_documentation_get"]
    problems, rows = classify.scan(nova, "gold.shop.customers", apply=True, owner="analytics")
    assert problems == []
    update = [call for call in nova.calls if call[0] == "table_documentation_update"]
    assert len(update) == 1
    assert update[0][3] == {"columnClasses": {"email": "personal"}, "owner": "analytics"}


def test_a_table_you_may_not_edit_is_a_problem():
    nova = Nova(doc(
        [{"name": "email", "r#type": "varchar"}],
        can_edit=False,
        edit_rule="only the owner group may edit this table",
    ))
    problems, rows = classify.scan(nova, "gold.shop.customers", apply=True)
    assert problems == [("classify", "only the owner group may edit this table")]
    assert rows == []
    assert [call[0] for call in nova.calls] == ["table_documentation_get"]


def test_a_table_name_without_three_parts_is_refused():
    nova = Nova(doc([{"name": "email", "r#type": "varchar"}]))
    problems, rows = classify.scan(nova, "gold.customers")
    assert problems == [("classify", "name the table as catalog.schema.table")]
    assert rows == []
    assert nova.calls == []
