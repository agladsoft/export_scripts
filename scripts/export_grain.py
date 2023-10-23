import os
import sys
import json
import itertools
import contextlib
import numpy as np
import pandas as pd
from typing import Optional
from pandas import DataFrame
from datetime import datetime

HEADERS_ENG: dict = {
    ("Дата (без времени)", "Дата"): "date",
    ("УНИ-Заказчик (есть вопросы по унификации; сейчас убрали кавычки, "
     "ОПФ привели в сокращенный вид, все - в верхний регистр)", "Заказчик"): "customer",
    ("Наименование груза (унифицированное)", "Наименование груза"): "goods_name",
    ("Отправитель (НА ПЕРВОМ ЭТАПЕ ПОКА ПОДГРУЖАЕМ ТО, ЧТО ЕСТЬ - после создания таблицы "
     "соединяем в представлении с REFERENCE_INN)", "Отправитель"): "shipper_name",
    ("Получатель (оставляем как есть)", "Получатель"): "consignee_name",
    ("Вид транспорта (оставляем как есть)", "Вид транспорта"): "transport_type",
    ("Количество, тонн",): "goods_weight_tonne",
    ("Местонахождение (унифицируем чеез парсинг ИНН и получаем 3 поля: ИНН / НАИМЕНОВАНИЕ / АДРЕС",
     "Местонахождение терминала"): "terminal_and_location",
    ("Наименование терминала",): "terminal_name",
    ("Адрес терминала",): "terminal_address",
    ("Отправитель адрес (оставляем как есть)", "Отправитель адрес"): "shipper_country",
    ("Получатель адрес (унифицированное)", "Получатель адрес"): "consignee_country"
}


DATE_FORMATS: tuple = ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M")


class ExportGrain(object):
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
        df.rename(columns=dict_columns_eng, inplace=True)

    def add_new_columns(self, df: DataFrame) -> None:
        """
        Add new columns.
        """
        df['month'] = pd.to_datetime(df['date']).dt.month
        df['year'] = pd.to_datetime(df['date']).dt.year
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

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
        df: DataFrame = pd.read_excel(self.input_file_path)
        df = df.dropna(axis=0, how='all')
        self.rename_columns(df)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df["date"] = df["date"].apply(lambda x: self.convert_format_date(str(x)) if x else None)
        self.add_new_columns(df)
        df = df.replace({np.nan: None, "NaT": None})
        self.write_to_json(df.to_dict('records'))


if __name__ == "__main__":
    export_grain: ExportGrain = ExportGrain(sys.argv[1], sys.argv[2])
    export_grain.main()
