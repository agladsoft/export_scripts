import re
import os
import sys
import json
import contextlib
import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime

headers_eng: dict = {
    "Терминал": "terminal",
    "Дата отправления": "shipment_date",
    "Линия": "line",
    "Количество": "container_count",
    "Размер контейнера": "container_size",
    "TEU": "teu",
    "Тип контейнера": "container_type",
    "Порт выгрузки": "tracking_seaport",
    "Страна выгрузки": "tracking_country ",
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


date_formats: tuple = ("%Y-%m-%d", "%d.%m.%Y")


class Export(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def change_type_and_values(df: DataFrame) -> None:
        """
        Change data types or changing values.
        """
        with contextlib.suppress(Exception):
            df['shipment_date'] = df['shipment_date'].dt.date.astype(str)
            df['voyage'] = df['voyage'].astype(str)

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
        df: DataFrame = pd.read_excel(self.input_file_path, dtype={"ИНН Грузоотправителя": str})
        df = df.dropna(axis=0, how='all')
        df = df.rename(columns=headers_eng)
        df = df.loc[:, ~df.columns.isin(['container_count', 'teu'])]
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        parsed_on: str = self.check_date_in_begin_file()
        self.add_new_columns(df, parsed_on)
        self.change_type_and_values(df)
        df = df.replace({np.nan: None})
        self.write_to_json(df.to_dict('records'))


export: Export = Export(sys.argv[1], sys.argv[2])
export.main()