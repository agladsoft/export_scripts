import pytest
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from src.scripts.parsed import ParsedDf
from src.scripts.report_order import Report_Order, MissingCulumnName

@pytest.fixture
def sample_dataframe():
    data = {
        "Дата отхода с/з": ["2024-08-16"],
        "№ пор.": ["12345"],
        "Дата пор.": ["16.08.2024"],
        "Тип": ["40 HC"],
        "№ конт.": ["1234567"],
        "Инд.":["ABCD1234567"],
        "Груз": [None],
        "Отгружен": ["2024-08-16"],
        "Прибыл": ["2024-08-16"],
        "Дата Док.": ["2024-08-16"],
    }
    return pd.DataFrame(data)

@pytest.fixture
def temp_excel_file(tmp_path, sample_dataframe):
    file_path = tmp_path / "2023.01_test.xlsx"
    sample_dataframe.to_excel(file_path, index=False)
    return file_path

@pytest.fixture
def temp_output_folder(tmp_path):
    return tmp_path / "output"

@pytest.fixture
def report_order(temp_excel_file, temp_output_folder):
    temp_output_folder.mkdir()
    return Report_Order(str(temp_excel_file), str(temp_output_folder))

def test_convert_format_date():
    assert Report_Order.convert_format_date("16.08.2024") == "2024-08-16"
    assert Report_Order.convert_format_date("2024-08-16") == "2024-08-16"
    with pytest.raises(TypeError):
        Report_Order.convert_format_date(None)

def test_rename_columns(report_order, sample_dataframe):
    report_order.rename_columns(sample_dataframe)
    assert "shipment_date" in sample_dataframe.columns
    assert "order_number_full" in sample_dataframe.columns

def test_rename_columns_missing_column(report_order, sample_dataframe):
    sample_dataframe["Неизвестный столбец"] = "Данные"
    with pytest.raises(MissingCulumnName):
        report_order.rename_columns(sample_dataframe)

def test_change_type(report_order, sample_dataframe):
    report_order.rename_columns(sample_dataframe)
    report_order.change_type(sample_dataframe)
    assert "container_type" in sample_dataframe.columns
    assert "container_size" in sample_dataframe.columns

def test_change_container(report_order, sample_dataframe):
    report_order.rename_columns(sample_dataframe)
    report_order.change_container(sample_dataframe)
    assert "container_number" in sample_dataframe.columns
    assert "№ конт." not in sample_dataframe.columns

def test_change_goods_name():
    assert Report_Order.change_goods_name(None) == "ПОРОЖНИЙ КОНТЕЙНЕР"
    assert Report_Order.change_goods_name("Товар") == "Товар"

def test_add_new_columns(report_order, sample_dataframe):
    parsed_on = "2024-08-16"
    report_order.add_new_columns(sample_dataframe, parsed_on)
    assert "parsed_on" in sample_dataframe.columns
    assert "original_file_name" in sample_dataframe.columns

def test_convert_format_to_date(report_order, sample_dataframe):
    report_order.rename_columns(sample_dataframe)
    report_order.convert_format_to_date(sample_dataframe)
    assert isinstance(sample_dataframe["shipment_date"].iloc[0], str)




