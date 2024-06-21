import os
import re
import sys
import json
import itertools
import contextlib
import numpy as np
import pandas as pd
from __init__ import *
from parsed import ParsedDf
from typing import Optional
from pandas import DataFrame
from datetime import datetime

HEADERS_ENG: dict = {
    ("Дата отхода с/з",): "shipment_date",
    ("№ пор.",): "order_number_full",
    ("Дата пор.",): "date_order",
    ("Экспедитор",): "expeditor",
    ("Рейс",): "voyage",
    ("Инд.",): "container",
    ("№ конт.",): "number",
    ("Тип",): "type",
    ("Груз",): "goods_name",
    ("МОПОГ",): "mopog",
    ("Тара",): "tare_weight",
    ("Нетто",): "net",
    ("Брутто",): "gross",
    ("Прибыл",): "arrived",
    ("Отгружен",): "shipped",
    ("Порт назначения",): "port_of_destination",
    ("Судно",): "ship_name",
    ("Линия",): "line",
    ("№ Док.",): "no_doc",
    ("Тип документа",): "doc_type",
    ("Дата Док.",): "date_doc",
    ("Тип пор.",): "order_type",
    ("ТНВЭД",): "tnved",
    ("Номер ГТД",): "gtd_number",
    ("Получатель",): "consignee_name",
    ("Отправитель",): "shipper_name",
    ("Порт выгрузки",): "tracking_seaport",
    ("Сост.пор.",): "order_status",
    ("Страна выгрузки",): "tracking_country",
}

DATE_FORMATS: tuple = ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M")


class MissingCulumnName(Exception):
    pass


class Report_Order(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def convert_format_date(date: str) -> Optional[str]:
        """
        Convert to a date type.
        """
        for date_format in DATE_FORMATS:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())

    @staticmethod
    def rename_columns(df: DataFrame) -> None:
        """
        Rename of a columns.
        """
        dict_columns_eng: dict = {}
        for column, columns in itertools.product(df.columns, HEADERS_ENG):
            for column_eng in columns:
                column_strip: str = column.strip()
                if column_strip == column_eng.strip():
                    dict_columns_eng[column] = HEADERS_ENG[columns]
        if len(dict_columns_eng) != len(df.iloc[0]):
            raise MissingCulumnName('Column naming error')
        df.rename(columns=dict_columns_eng, inplace=True)

    @staticmethod
    def change_type(df: DataFrame) -> None:
        df[['container_type', 'container_size']] = df['type'].str.split(expand=True)
        df.drop(columns=['type'], inplace=True)

    @staticmethod
    def change_container(df: DataFrame) -> None:
        df['container_number'] = df['container'].str.strip() + df['number'].str.strip()
        df.drop(columns=['container', 'number'], inplace=True)

    @staticmethod
    def change_goods_name(goods_name: str) -> str:
        if goods_name:
            return goods_name
        return "ПОРОЖНИЙ КОНТЕЙНЕР"

    def change_columns(self, df: DataFrame) -> None:
        self.change_type(df)
        self.change_container(df)

    def check_date_in_begin_file(self) -> str:
        """
        Check the date at the beginning of the file.
        """
        date_previous: re.Match = re.match(r'\d{2,4}.\d{1,2}', os.path.basename(self.input_file_path))
        date_previous: str = f'{date_previous.group()}.01' if date_previous else date_previous
        if date_previous is None:
            telegram(f'Не указана дата в файле {self.input_file_path}')
            raise AssertionError('Date not in file name!')
        else:
            return str(datetime.strptime(date_previous, "%Y.%m.%d").date())

    def add_new_columns(self, df: DataFrame, parsed_on) -> None:
        """
        Add new columns.
        """
        df['parsed_on'] = parsed_on
        df['terminal'] = 'НУТЭП'
        # df['month'] = pd.to_datetime(df['date']).dt.month
        # df['year'] = pd.to_datetime(df['date']).dt.year
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def convert_format_to_date(self, df: DataFrame) -> None:
        df["shipped"] = df["shipped"].apply(lambda x: self.convert_format_date(str(x)) if x else None)
        df["date_order"] = df["date_order"].apply(lambda x: self.convert_format_date(str(x)) if x else None)
        df["arrived"] = df["arrived"].apply(lambda x: self.convert_format_date(str(x)) if x else None)
        df["shipment_date"] = df["shipment_date"].apply(lambda x: self.convert_format_date(str(x)) if x else None)
        df["date_doc"] = df["date_doc"].apply(lambda x: self.convert_format_date(str(x)) if x else None)

    def write_to_json(self, parsed_data: list) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(self.input_file_path)
        output_file_path: str = os.path.join(self.output_folder, f'{basename}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4, default=serialize_datetime)

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        df: DataFrame = pd.read_excel(self.input_file_path, skiprows=1, dtype={"№ конт.": str})
        df = df.dropna(axis=0, how='all')
        parsed_on = self.check_date_in_begin_file()
        self.rename_columns(df)
        self.change_columns(df)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        self.add_new_columns(df, parsed_on)
        self.convert_format_to_date(df)
        df["container_size"] = pd.to_numeric(df["container_size"], errors='coerce').astype('Int64')
        df = df.replace({np.nan: None, "NaT": None})
        df["goods_name"] = df["goods_name"].apply(lambda x: self.change_goods_name(x))
        ParsedDf(df).get_port()
        df = df.replace({np.nan: None, "NaT": None})
        self.write_to_json(df.to_dict('records'))


if __name__ == "__main__":
    report_order: Report_Order = Report_Order(sys.argv[1], sys.argv[2])
    report_order.main()
