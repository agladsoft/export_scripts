import pytest
import os
import json
import pandas as pd
from datetime import datetime
from unittest.mock import patch, mock_open
from src.scripts.flat_export import Export
from src.scripts.parsed import ParsedDf


@pytest.fixture
def sample_dataframe():
    data = {
        "shipment_date": ["2024-08-16", "15.06.2023", "2022-01-01 12:00:00"],
        "container_count": [10, 20, 30],
        "container_size": ["40", "20", "40"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def temp_output_folder(tmp_path):
    return tmp_path / "output"


@pytest.fixture
def export_instance(tmp_path):
    input_file = tmp_path / "test.xlsx"
    output_folder = tmp_path / "output"
    output_folder.mkdir()
    return Export(str(input_file), str(output_folder))


@pytest.mark.parametrize("date_str, expected", [
    ("2024-08-16", "2024-08-16"),
    ("15.06.2023", "2023-06-15"),
    ("2022-01-01 12:00:00", "2022-01-01"),
    ("invalid-date", None)
])
def test_convert_format_date(date_str, expected):
    assert Export.convert_format_date(date_str) == expected


def test_change_type_and_values(export_instance, sample_dataframe):
    export_instance.change_type_and_values(sample_dataframe)
    assert sample_dataframe["shipment_date"].iloc[0] == "2024-08-16"
    assert sample_dataframe["shipment_date"].iloc[1] == "2023-06-15"
    assert sample_dataframe["shipment_date"].iloc[2] == "2022-01-01"


def test_add_new_columns(export_instance, sample_dataframe):
    parsed_on = "2025-01-01"
    export_instance.add_new_columns(sample_dataframe, parsed_on)
    assert "gtd_number" in sample_dataframe.columns
    assert "parsed_on" in sample_dataframe.columns
    assert "original_file_name" in sample_dataframe.columns
    assert sample_dataframe["parsed_on"].iloc[0] == "2025-01-01"


def test_check_date_in_begin_file(export_instance, monkeypatch):
    monkeypatch.setattr(export_instance, "input_file_path", "2024.08_data.xlsx")
    assert export_instance.check_date_in_begin_file() == "2024-08-01"

    monkeypatch.setattr(export_instance, "input_file_path", "invalid_data.xlsx")
    with pytest.raises(AssertionError, match="Date not in file name!"):
        export_instance.check_date_in_begin_file()


def test_write_to_json(export_instance, sample_dataframe, temp_output_folder):
    parsed_data = sample_dataframe.to_dict("records")
    with patch("builtins.open", mock_open()) as mock_file:
        export_instance.write_to_json(parsed_data)
        mock_file.assert_called_once_with(os.path.join(export_instance.output_folder, "test.xlsx.json"), "w",
                                          encoding="utf-8")


