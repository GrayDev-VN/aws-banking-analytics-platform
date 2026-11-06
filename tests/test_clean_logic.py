"""
Unit tests for data cleaning logic in clean_load_staging
"""

import importlib

import pandas as pd
import pytest

clean_dataframe = importlib.import_module("scripts.2_clean_load_staging").clean_dataframe


def test_clean_dataframe_strip_whitespace():
    raw_data = {
        "customer_id": [" C1001 ", "C1002  "],
        "first_name": [" John ", " Jane "],
        "email": [" john@example.com ", "jane@example.com "]
    }
    config = {"columns": ["customer_id", "first_name", "email"]}
    df = pd.DataFrame(raw_data)
    cleaned_df = clean_dataframe(df, config)

    assert cleaned_df["customer_id"].iloc[0] == "C1001"
    assert cleaned_df["first_name"].iloc[1] == "Jane"
    assert cleaned_df["email"].iloc[0] == "john@example.com"


def test_clean_dataframe_date_formatting():
    raw_data = {
        "account_id": ["A1", "A2"],
        "open_date": ["2022/01/15", "15-08-2023"]
    }
    config = {
        "columns": ["account_id", "open_date"],
        "date_cols": ["open_date"]
    }
    df = pd.DataFrame(raw_data)
    cleaned_df = clean_dataframe(df, config)

    assert cleaned_df["open_date"].iloc[0] == "2022-01-15"
    assert cleaned_df["open_date"].iloc[1] == "2023-08-15"


def test_clean_dataframe_datetime_formatting():
    raw_data = {
        "transaction_id": ["TX100"],
        "transaction_date": ["2023-05-10 14:30:00.123456"]
    }
    config = {
        "columns": ["transaction_id", "transaction_date"],
        "datetime_cols": ["transaction_date"]
    }
    df = pd.DataFrame(raw_data)
    cleaned_df = clean_dataframe(df, config)

    assert cleaned_df["transaction_date"].iloc[0] == "2023-05-10 14:30:00"


@pytest.mark.parametrize("dtype", [object, "string"])
def test_clean_dataframe_preserves_missing_strings(dtype):
    df = pd.DataFrame({"name": pd.Series([None, " Alice "], dtype=dtype)})
    result = clean_dataframe(df, {})
    assert pd.isna(result.loc[0, "name"])
    assert result.loc[1, "name"] == "Alice"


@pytest.mark.parametrize("dayfirst, expected", [
    (True, "2023-04-03"), (False, "2023-03-04"),
])
def test_clean_dataframe_ambiguous_date_policy(dayfirst, expected):
    df = pd.DataFrame({"open_date": ["03/04/2023"]})
    result = clean_dataframe(df, {"date_cols": ["open_date"], "dayfirst": dayfirst})
    assert result.loc[0, "open_date"] == expected


def test_clean_dataframe_invalid_and_missing_dates():
    df = pd.DataFrame({"open_date": ["not-a-date", None, "2023-02-30"]})
    result = clean_dataframe(df, {"date_cols": ["open_date"]})
    assert result["open_date"].isna().all()


def test_clean_dataframe_mixed_datetime_formats():
    df = pd.DataFrame({"transaction_date": [
        "2023-05-10 14:30:00.123456", "15-08-2023 09:15:00", None
    ]})
    result = clean_dataframe(df, {"datetime_cols": ["transaction_date"]})
    assert result.loc[0, "transaction_date"] == "2023-05-10 14:30:00"
    assert result.loc[1, "transaction_date"] == "2023-08-15 09:15:00"
    assert pd.isna(result.loc[2, "transaction_date"])
