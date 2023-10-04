import re
import os
import sys
import json
import contextlib
import numpy as np
import pandas as pd
from typing import Optional
from pandas import DataFrame
from datetime import datetime

headers_eng: dict = {
    "Терминал": "terminal",
    "Линия": "line",
    "Дата отгрузки": "shipment_date",
    "Количество": "container_count",
    "Размер контейнера": "container_size",
    "TEU": "teu",
    "Тип контейнера": "container_type",
    "Порт выгрузки": "tracking_seaport",
    "Страна выгрузки": "tracking_country",
    "Судно": "ship_name",
    "Рейс": "voyage",
    "Контейнер из партии": "container_number",
    "Отправитель": "shipper_name",
    "Получатель": "consignee_name",
    "Наименование товара": "goods_name",
    "Номер коносамента": "consignment",
    "Экспедитор": "expeditor",
    "ИНН Грузоотправителя": "shipper_inn",
    "ТНВЭД": "tnved"
}

dict_types: dict = {
    "ИНН Грузоотправителя": str,
    "Рейс": str,
    "TEU": int,
    "ТНВЭД": str
}


date_formats: tuple = ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S")


class Export(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def convert_format_date(date: str) -> Optional[str]:
        """
        Convert to a date type.
        """
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())
        return None

    def change_type_and_values(self, df: DataFrame) -> None:
        """
        Change data types or changing values.
        """
        with contextlib.suppress(Exception):
            df['shipment_date'] = df['shipment_date'].apply(lambda x: self.convert_format_date(str(x)))

    def add_new_columns(self, df: DataFrame, parsed_on: str) -> None:
        """
        Add new columns.
        """
        df['gtd_number'] = 'Нет данных'
        df['parsed_on'] = parsed_on
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def check_date_in_begin_file(self) -> str:
        """
        Check the date at the beginning of the file.
        """
        date_previous: re.Match = re.match(r'\d{2,4}.\d{1,2}', os.path.basename(self.input_file_path))
        date_previous: str = f'{date_previous.group()}.01' if date_previous else date_previous
        if date_previous is None:
            raise AssertionError('Date not in file name!')
        else:
            return str(datetime.strptime(date_previous, "%Y.%m.%d").date())

    def write_to_json(self, parsed_data: list) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(self.input_file_path)
        output_file_path: str = os.path.join(self.output_folder, f'{basename}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        df: DataFrame = pd.read_excel(self.input_file_path, dtype=dict_types)
        df = df.dropna(axis=0, how='all')
        original_columns = list(df.columns)
        df = df.rename(columns=headers_eng)
        renamed_columns = list(df.columns)
        df = df.drop(columns=set(original_columns) & set(renamed_columns))
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        parsed_on: str = self.check_date_in_begin_file()
        self.add_new_columns(df, parsed_on)
        self.change_type_and_values(df)
        df = df.replace({np.nan: None, "NaT": None})
        self.write_to_json(df.to_dict('records'))


export: Export = Export(sys.argv[1], sys.argv[2])
export.main()
