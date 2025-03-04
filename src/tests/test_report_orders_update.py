import os
import json
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime
from src.scripts.report_orders_update import Report_Order_Update

@pytest.fixture
def sample_df():
    data = {
        "Дата отхода с/з": ["01.01.2023", "15.02.2023"],
        "№ пор.": ["12345", "67890"],
        "Экспедитор": ["Company A", "Company B"],
        "№ конт.": ["ABC123", "XYZ789"],
        "Тип": ["20GP", "40HQ"],
        "Груз": ["Coal", "Electronics"],
        "Брутто": [1000, 2000],
    }
    return pd.DataFrame(data)

@pytest.fixture
def report_order():
    with patch.object(Report_Order_Update, "connect_clickhouse", return_value=MagicMock()):
        return Report_Order_Update("input.xlsx", "output_folder")

def test_convert_format_date(report_order):
    assert report_order.convert_format_date("16.08.2024") == "2024-08-16"
    assert report_order.convert_format_date("2024-08-16") == "2024-08-16"
    with pytest.raises(TypeError):
        report_order.convert_format_date(None)

@pytest.mark.parametrize("value,expected", [
    (0.0, False),
    (1.0, True),
    (None, None),
    (5.0, 5.0)
])
def test_convert_format_bool(value, expected, report_order):
    assert report_order.convert_format_bool(value) == expected

@patch("os.path.basename", return_value="test_file.xlsx")
def test_add_new_columns(mock_basename, sample_df, report_order):
    report_order.add_new_columns(sample_df)
    assert "original_file_name" in sample_df.columns
    assert "original_file_parsed_on" in sample_df.columns
    assert sample_df["original_file_name"].iloc[0] == "test_file.xlsx"

@pytest.mark.parametrize("value,flag,expected", [
    ("text", True, "'text'"),
    (None, False, "NULL"),
    (123, False, 123),
    ("O'Reilly", True, "'O''Reilly'")
])
def test_change_format_value(value, flag, expected, report_order):
    assert report_order.change_format_value(value, flag) == expected


