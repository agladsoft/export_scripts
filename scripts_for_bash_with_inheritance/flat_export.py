import contextlib
import datetime
import json
import math
import os
import re
import sys
import pandas as pd
import numpy as np

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]

headers_eng = {
    "Терминал": "terminal",
    "Дата отправления": "date",
    "Линия": "line",
    "Количество": "count",
    "Размер контейнера": "container_size",
    "TEU": "teu",
    "Тип контейнера": "container_type",
    "Порт выгрузки": "unload_seaport",
    "Страна выгрузки": "unload_country",
    "Судно": "ship",
    "Рейс": "voyage",
    "Контейнер из партии": "container_number",
    "Отправитель": "shipper",
    "Получатель": "consignee",
    "Наименование товара": "goods_name_rus",
    "Номер коносамента": "consignment",
    "Экспедитор": "expeditor",
    "ИНН Грузоотправителя": "shipper_inn",
    "ТНВЭД": "goods_tnved",
    "Город": "city"
}

df = pd.read_csv(input_file_path)
df = df.replace({np.nan: None})
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
            if math.isnan(value):
                dict_data[key] = None
            elif key in ['shipper_inn']:
                dict_data[key] = int(value)
    dict_data['terminal'] = os.environ.get('XL_IMPORT_TERMINAL')
    dict_data['parsed_on'] = parsed_on
    dict_data['original_file_name'] = os.path.basename(input_file_path)
    dict_data['original_file_parsed_on'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.json')
with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)