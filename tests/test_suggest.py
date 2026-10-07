import pandas as pd
import pytest

from pipeline_sentinel.contract_io import load_contract
from pipeline_sentinel.contracts import check_contract
from pipeline_sentinel.suggest import suggest_contract


@pytest.fixture
def frame():
    n = 200
    return pd.DataFrame({
        "order_id": range(1, n + 1),
        "status": ["paid", "open", "void", "paid"] * (n // 4),
        "amount": [100.0 + (i % 50) * 10 for i in range(n)],
        "note": [None] * 150 + ["x"] * 50,
        "created_at": pd.date_range("2026-01-01", periods=n, freq="h", tz="UTC"),
        "flag": [True, False] * (n // 2),
    })


def test_suggested_contract_is_valid_and_passes_on_its_own_sample(frame):
    suggestion = suggest_contract(frame)
    assert suggestion.contract["expected_schema"] == {
        "order_id": "integer", "status": "string", "amount": "float",
        "note": "string", "created_at": "datetime", "flag": "boolean"}
    assert check_contract(frame, suggestion.contract) == []


def test_suggested_rules(frame):
    rules = {(r["type"], r["column"]): r for r in suggest_contract(frame).contract["rules"]}
    assert rules[("not_null", "order_id")]["max_null_ratio"] == 0.0
    assert ("uniqueness", "order_id") in rules
    assert ("range", "order_id") not in rules, "anahtar kolonlara range önerilmez"
    assert rules[("allowed_values", "status")]["values"] == ["open", "paid", "void"]
    amount = rules[("range", "amount")]
    assert amount["min"] == 0 and amount["max"] > 590
    assert ("not_null", "note") not in rules, "%75 boş kolona not_null önerilmez"


def test_suggestion_catches_a_hundredfold_shift(frame):
    contract = suggest_contract(frame).contract
    broken = frame.assign(amount=frame["amount"] * 100)
    assert any(v.rule_type == "range" for v in check_contract(broken, contract))


def test_yaml_roundtrip_keeps_notes_as_comments(frame):
    suggestion = suggest_contract(frame)
    text = suggestion.to_yaml()
    assert text.startswith("# Taslak sözleşme")
    assert "# NOT: 'created_at' için SLA" in text
    assert load_contract(text) == suggestion.contract


def test_small_samples_only_get_schema_and_not_null():
    small = pd.DataFrame({"id": [1, 2, 3], "amount": [1.0, 2.0, 3.0]})
    suggestion = suggest_contract(small)
    assert {r["type"] for r in suggestion.contract["rules"]} == {"not_null"}
    assert any("3 satır" in note for note in suggestion.notes)


def test_negative_values_get_a_lower_bound():
    frame = pd.DataFrame({"delta": [(-1) ** i * (i % 40) for i in range(100)]})
    rule = [r for r in suggest_contract(frame).contract["rules"] if r["type"] == "range"][0]
    assert rule["min"] < -39 and rule["max"] > 39


@pytest.mark.parametrize("bad", [pd.DataFrame(), pd.DataFrame({"a": []})])
def test_empty_input_is_rejected(bad):
    with pytest.raises(ValueError):
        suggest_contract(bad)
