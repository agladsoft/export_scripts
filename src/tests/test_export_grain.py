import os
import json
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from src.scripts.export_grain import ExportGrain

@pytest.fixture
def sample_dataframe():
    data = {
        "Дата": ["01.01.2024", "15.02.2024"],
        "Заказчик": ["ООО Тест", "АО Клиент"],
        "Наименование груза": ["Пшеница", "Ячмень"],
        "Отправитель": ["Завод 1", "Завод 2"],
        "Получатель": ["Компания А", "Компания Б"],
        "Вид транспорта": ["Авто", "ЖД"],
        "Количество, тонн": [1000, 2000]
    }
    return pd.DataFrame(data)

@pytest.fixture
def export_grain():
    return ExportGrain("test_input.xlsx", "output_folder")

@pytest.mark.parametrize("date_str, expected", [
    ("01.01.2024", "2024-01-01"),
    ("15.02.2024 12:34:56", "2024-02-15"),
    ("invalid_date", None)
])
def test_convert_format_date(date_str, expected):
    assert ExportGrain.convert_format_date(date_str) == expected

def test_rename_columns(sample_dataframe, export_grain):
    export_grain.rename_columns(sample_dataframe)
    expected_columns = {"date", "customer", "goods_name", "shipper_name", "consignee_name", "transport_type", "goods_weight_tonne"}
    assert set(sample_dataframe.columns) == expected_columns

def test_add_new_columns(sample_dataframe, export_grain):
    sample_dataframe["date"] = sample_dataframe["Дата"].apply(ExportGrain.convert_format_date)
    export_grain.add_new_columns(sample_dataframe)
    assert "month" in sample_dataframe.columns
    assert "year" in sample_dataframe.columns
    assert "original_file_name" in sample_dataframe.columns
    assert "original_file_parsed_on" in sample_dataframe.columns


@patch("pandas.read_excel")
@patch.object(ExportGrain, "write_to_json")
def test_main(mock_write_to_json, mock_read_excel, export_grain, sample_dataframe):
    mock_read_excel.return_value = sample_dataframe
    export_grain.main()
    mock_read_excel.assert_called_once_with("test_input.xlsx")
    mock_write_to_json.assert_called_once()
