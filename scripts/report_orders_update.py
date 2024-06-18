import os
import sys
import json
import itertools
import contextlib
import numpy as np
import pandas as pd
from __init__ import *
from typing import Optional
from pandas import DataFrame, Series
from datetime import datetime
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client

HEADERS_ENG: dict = {
    ("Дата отхода с/з",): "shipment_date",
    ("№ пор.",): "order_number_full",
    ("Дата пор.",): "date_order",
    ("Экспедитор",): "forwarder",
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
    ("Отправитель",): "shipper_name"
}

DATE_FORMATS: tuple = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%d.%m.%Y",
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M"
)


class Report_Order_Update(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.client = self.connect_clickhouse()
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def connect_clickhouse():
        """
                Connecting to clickhouse.
                :return: Client ClickHouse.
                """
        try:
            client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                        username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            logger.info('Connection to ClickHouse is successful')
        except Exception as ex_connect:
            logger.info(f"Error connecting to ClickHouse: {ex_connect}")
            sys.exit(1)
        return client

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

    @staticmethod
    def change_type(df: DataFrame) -> None:
        """change type column to container_type and container_size."""
        df[['container_type', 'container_size']] = df['type'].str.split(expand=True)
        df.drop(columns=['type'], inplace=True)

    @staticmethod
    def change_container(df: DataFrame) -> None:
        """change container column to container_number."""
        df['container_number'] = df['container'].str.strip() + df['number'].str.strip()
        df.drop(columns=['container', 'number'], inplace=True)

    def change_columns(self, df: DataFrame) -> None:
        """change columns."""
        self.change_type(df)
        self.change_container(df)

    def add_new_columns(self, df: DataFrame) -> None:
        """
        Add new columns.
        """
        # df['month'] = pd.to_datetime(df['date']).dt.month
        # df['year'] = pd.to_datetime(df['date']).dt.year
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def convert_format_to_date(self, df: DataFrame) -> None:
        """convert format to date."""
        logger.info("Converting the format of the date")
        df["shipment_date"] = df["shipment_date"].apply(lambda x: self.convert_format_date(str(x)) if x else None)
        df["date_order"] = df["date_order"].apply(lambda x: self.convert_format_date(str(x)) if x else None)
        df["arrived"] = df["arrived"].apply(lambda x: self.convert_format_date(str(x)) if x else None)
        df["shipped"] = df["shipped"].apply(lambda x: self.convert_format_date(str(x)) if x else None)
        df["date_doc"] = df["date_doc"].apply(lambda x: self.convert_format_date(str(x)) if x else None)
        df["parsed_on"] = df["parsed_on"].apply(lambda x: self.convert_format_date(str(x)) if x else None)

    @staticmethod
    def change_format_value(value, flag=False):
        if not value:
            return 'NULL'
        if flag:
            value = str(value).replace("'", "''")
            return f"'{value}'"
        return value

    def clickhouse_update(self, row: Series):
        query = f"""
        ALTER TABLE default.export UPDATE
        parsed_on = {self.change_format_value(row.get("parsed_on"))},
        month_parsed_on = {self.change_format_value(row.get("month_parsed_on"))},
        year_parsed_on = {self.change_format_value(row.get("year_parsed_on"))},
        terminal = {self.change_format_value(row["terminal"], True)},
        line = {self.change_format_value(row["line"], True)},
        ship_name = {self.change_format_value(row["ship_name"], True)},
        voyage = {self.change_format_value(row["voyage"], True)},
        consignment = {self.change_format_value(row["consignment"], True)},
        container_number = {self.change_format_value(row["container_number"], True)},
        container_size = {self.change_format_value(row["container_size"])},
        container_type = {self.change_format_value(row["container_type"], True)},
        container_count = {self.change_format_value(row["container_count"])},
        goods_name = {self.change_format_value(row["goods_name"], True)},
        tnved = {self.change_format_value(row["tnved"], True)},
        goods_weight_with_package = {self.change_format_value(row["goods_weight_with_package"], True)},
        shipper_name = {self.change_format_value(row["shipper_name"], True)},
        consignee_name = {self.change_format_value(row["consignee_name"], True)},
        expeditor = {self.change_format_value(row["expeditor"], True)},
        tracking_country = {self.change_format_value(row["tracking_country"], True)},
        tracking_seaport = {self.change_format_value(row["tracking_seaport"], True)},
        gtd_number = {self.change_format_value(row["gtd_number"], True)},
        shipped = {self.change_format_value(row["shipped"])},
        order_number_full = {self.change_format_value(row["order_number_full"], True)},
        booking = {self.change_format_value(row["booking"], True)},
        mopog = {self.change_format_value(row["mopog"], True)},
        tare_weight = {self.change_format_value(row["tare_weight"])},
        date_order = {self.change_format_value(row["date_order"])},
        net = {self.change_format_value(row["net"])},
        gross = {self.change_format_value(row["gross"])},
        arrived = {self.change_format_value(row["arrived"])},
        port_of_destination = {self.change_format_value(row["port_of_destination"], True)},
        no_doc = {self.change_format_value(row["no_doc"], True)},
        doc_type = {self.change_format_value(row["doc_type"], True)},
        date_doc = {self.change_format_value(row["date_doc"])},
        order_type = {self.change_format_value(row["order_type"], True)},
        order_status = {self.change_format_value(row["order_status"], True)},
        is_auto_tracking = {self.change_format_value(row["is_auto_tracking"], True)},
        is_auto_tracking_ok = {self.change_format_value(row["is_auto_tracking_ok"], True)},
        original_file_name = {self.change_format_value(row["original_file_name"], True)},
        original_file_parsed_on = {self.change_format_value(row["original_file_parsed_on"], True)}
        WHERE uuid = '{row["uuid"]}'
        """

        self.client.query(query)

    def update_date_in_table(self, df: DataFrame) -> None:
        """update data in table clickhouse to uuid."""
        errors = []
        logger.info("Updating data in the table export clickhouse")
        for index, row in df.iterrows():
            try:
                self.clickhouse_update(row)
            except Exception as ex:
                logger.info(f"Error updating data in the export clickhouse table : \n{ex}")
                errors.append(row["uuid"])
                continue
        if errors:
            errors = '\n'.join(errors)
            logger.info(f"Error updating data in the export clickhouse table : \n{errors}")
            telegram(f"Ошибка при обновлении данных в таблице export clickhouse\n"
                     f"uuid: {errors}")

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
        logger.info(f"Reading the Excel file : {os.path.basename(self.input_file_path)}")
        df: DataFrame = pd.read_excel(self.input_file_path, dtype={"№ конт.": str})
        df = df.dropna(axis=0, how='all')
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        self.convert_format_to_date(df)
        df = df.replace({np.nan: None, "NaT": None})
        self.update_date_in_table(df)
        logger.info("Finished updating data in the table export clickhouse")


if __name__ == "__main__":
    report_order: Report_Order_Update = Report_Order_Update(sys.argv[1], sys.argv[2])
    report_order.main()
