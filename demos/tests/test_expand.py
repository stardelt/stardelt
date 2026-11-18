import pytest
from stardelt_pipelines.expand import expand, run_values


def test_all_three_kinds_of_placeholder_are_replaced():
    out = expand(
        "SELECT * FROM t WHERE ds = DATE '${run.date}' AND run = '${run.id}' AND n = ${params.days}",
        run_date="2026-10-19",
        run_id="scheduled__2026-10-19T03:00",
        params={"days": "7"},
    )
    assert out == (
        "SELECT * FROM t WHERE ds = DATE '2026-10-19' AND run = 'scheduled__2026-10-19T03:00' AND n = 7"
    )


def test_text_without_placeholders_is_unchanged():
    assert expand("SELECT '$x' || '{y}'", run_date="d", run_id="r", params={}) == "SELECT '$x' || '{y}'"


def test_an_unknown_placeholder_is_an_error_not_left_in_the_sql():
    with pytest.raises(ValueError, match=r"unknown placeholder \$\{params.missing\}"):
        expand("SELECT ${params.missing}", run_date="d", run_id="r", params={})


def test_no_run_conf_means_the_fallback_date_and_no_overrides():
    assert run_values(None, "2026-11-18") == ("2026-11-18", {})


def test_an_empty_run_conf_means_the_fallback_date_and_no_overrides():
    assert run_values({}, "2026-11-18") == ("2026-11-18", {})


def test_a_run_conf_carries_the_date_and_the_parameter_values():
    assert run_values({"runDate": "2026-11-13", "params": {"days": "7"}}, "2026-11-18") == (
        "2026-11-13",
        {"days": "7"},
    )


def test_a_run_date_that_is_not_iso_is_an_error():
    with pytest.raises(ValueError, match="runDate is a date"):
        run_values({"runDate": "13.11.2026"}, "2026-11-18")


def test_more_than_sixteen_run_parameters_are_an_error():
    with pytest.raises(ValueError, match="at most 16"):
        run_values({"params": {f"p{i}": "1" for i in range(17)}}, "2026-11-18")


def test_a_run_parameter_name_that_is_not_lower_case_is_an_error():
    with pytest.raises(ValueError, match="the run's parameters are at most 16"):
        run_values({"params": {"Days": "7"}}, "2026-11-18")
