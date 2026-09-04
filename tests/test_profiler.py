import pandas as pd

from pipeline_sentinel.profiler import profile_column, profile_dataset


def test_profile_column_numeric():
    series = pd.Series([10.0, 20.0, 30.0, None], name="amount")
    profile = profile_column(series)
    assert profile["row_count"] == 4
    assert profile["null_ratio"] == 0.25
    assert profile["mean"] == 20.0
    assert profile["min"] == 10.0
    assert profile["max"] == 30.0


def test_profile_column_categorical_top_values():
    series = pd.Series(["a", "a", "b", "c", "c", "c"], name="status")
    profile = profile_column(series)
    assert profile["null_ratio"] == 0.0
    top = {row["value"]: row["count"] for row in profile["top_values"]}
    assert top["c"] == 3
    assert top["a"] == 2


def test_profile_column_all_null():
    series = pd.Series([None, None, None], name="x")
    profile = profile_column(series)
    assert profile["null_ratio"] == 1.0
    assert "mean" not in profile


def test_profile_dataset_covers_all_columns():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    profile = profile_dataset(df)
    assert set(profile.keys()) == {"a", "b"}
    assert profile["a"]["row_count"] == 3
