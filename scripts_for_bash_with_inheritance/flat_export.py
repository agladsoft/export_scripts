import contextlib
import datetime
import json
import os
import re
import sys
import pandas as pd
import numpy as np

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]

date_formats = ("%Y-%m-%d", "%d.%m.%Y")

headers_eng = {
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

def convert_format_date(date):
    if date_parsed := re.findall(r'\d{2,4}-\d{1,2}-\d{1,2}|\d{1,2}[.]\d{1,2}[.]\d{2,4}', date):
        for date_format in date_formats:
            with contextlib.suppress(ValueError):
                return str(datetime.datetime.strptime(date_parsed[0], date_format).date())
    return date


df = pd.read_csv(input_file_path, dtype=str)
df = df.replace({np.nan: None})
df = df.dropna(axis=0, how='all')
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
df = df.rename(columns=headers_eng)
df = df.loc[:, ~df.columns.isin(['count', 'teu'])]
parsed_data = df.to_dict('records')
date_previous = re.match('\d{2,4}.\d{1,2}', os.path.basename(input_file_path))
date_previous = f'{date_previous.group()}.01' if date_previous else date_previous
if date_previous is None:
    raise Exception('Date not in file name!')
else:
    parsed_on = str(datetime.datetime.strptime(date_previous, "%Y.%m.%d").date())
for dict_data in parsed_data:
    for key, value in dict_data.items():
        with contextlib.suppress(Exception):
            if key in ['container_size']:
                dict_data[key] = int(value)
            elif key in ['date']:
                dict_data[key] = convert_format_date(value)

    dict_data['gtd_number'] = 'Нет данных'
    dict_data['terminal'] = os.environ.get('XL_IMPORT_TERMINAL')
    dict_data['parsed_on'] = parsed_on
    dict_data['original_file_name'] = os.path.basename(input_file_path)
    dict_data['original_file_parsed_on'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.json')
with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)