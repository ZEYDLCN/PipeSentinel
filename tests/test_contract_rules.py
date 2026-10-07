import pandas as pd
import pytest

from pipeline_sentinel.contract_io import ContractError, load_contract, validate_contract
from pipeline_sentinel.contracts import check_contract
from pipeline_sentinel.detector import signals_from_violations
from pipeline_sentinel.rca import generate_report

SCHEMA = {"order_id": "integer", "start": "datetime", "end": "datetime", "gross": "float", "net": "float"}


def contract(*rules):
    return validate_contract({"expected_schema": SCHEMA, "rules": list(rules)})


@pytest.fixture
def frame():
    start = pd.date_range("2026-09-01", periods=5, freq="D", tz="UTC")
    return pd.DataFrame({"order_id": range(5), "start": start, "end": start + pd.Timedelta(hours=2),
                         "gross": [10.0, 20, 30, 40, 50], "net": [8.0, 18, 25, 36, 45]})


def test_row_count_rule(frame):
    rule = {"type": "row_count", "min_rows": 3, "max_rows": 10}
    assert check_contract(frame, contract(rule)) == []
    low = check_contract(frame.head(2), contract(rule))
    assert [(v.rule_type, v.column) for v in low] == [("row_count", None)]
    assert check_contract(pd.concat([frame] * 3), contract(rule))[0].evidence["row_count"] == 15
    assert check_contract(frame.iloc[:0], contract({"type": "row_count", "min_rows": 1}))


def test_column_compare_numeric_and_datetime(frame):
    net_le_gross = {"type": "column_compare", "column": "net", "operator": "<=", "other": "gross"}
    end_after_start = {"type": "column_compare", "column": "end", "operator": ">", "other": "start"}
    assert check_contract(frame, contract(net_le_gross, end_after_start)) == []
    broken = frame.assign(net=[8.0, 18, 35, 36, None], end=frame["start"] - pd.Timedelta(minutes=1))
    found = {v.column: v for v in check_contract(broken, contract(net_le_gross, end_after_start))}
    assert found["net"].evidence["violation_count"] == 1, "boş değerler karşılaştırılmaz"
    assert found["end"].evidence["violation_count"] == 5


def test_column_compare_survives_type_mismatch(frame):
    rule = {"type": "column_compare", "column": "net", "operator": "<=", "other": "gross"}
    broken = frame.assign(net=["a", "b", "c", "d", "e"])
    kinds = {v.rule_type for v in check_contract(broken, contract(rule))}
    assert "schema_type_mismatch" in kinds and "column_compare" not in kinds


@pytest.mark.parametrize("rule", [
    {"type": "row_count"},
    {"type": "row_count", "min_rows": -1},
    {"type": "row_count", "min_rows": 5, "max_rows": 1},
    {"type": "row_count", "min_rows": True},
    {"type": "row_count", "column": "order_id", "min_rows": 1},
    {"type": "column_compare", "column": "net", "operator": "<=", "other": "missing"},
    {"type": "column_compare", "column": "net", "operator": "<=", "other": "net"},
    {"type": "column_compare", "column": "net", "operator": "~", "other": "gross"},
    {"type": "column_compare", "column": "net", "operator": "<=", "other": "start"},
    {"type": "column_compare", "column": "net", "operator": "<="},
])
def test_invalid_new_rules_are_rejected(rule):
    with pytest.raises(ContractError):
        contract(rule)


def test_yaml_roundtrip():
    text = ("expected_schema: {a: float, b: float}\nrules:\n"
            "  - {type: row_count, min_rows: 1}\n"
            "  - {type: column_compare, column: a, operator: '<=', other: b}\n")
    assert [r["type"] for r in load_contract(text)["rules"]] == ["row_count", "column_compare"]


def test_rca_explains_new_signals(frame):
    rules = [{"type": "row_count", "min_rows": 10},
             {"type": "column_compare", "column": "net", "operator": "<=", "other": "gross"}]
    broken = frame.assign(net=frame["gross"] * 2)
    signals = [{**s.to_dict(), "id": f"s{i}"} for i, s in enumerate(signals_from_violations(check_contract(broken, contract(*rules))))]
    report = generate_report("orders", "run", signals, {}, []).to_dict()
    texts = " ".join(h["hypothesis"] for h in report["root_causes"])
    assert "sınırın dışında" in texts and "'net' ile 'gross'" in texts
    assert all(h["evidence_ids"] for h in report["root_causes"])
