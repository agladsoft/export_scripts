import os
import requests
from dotenv import load_dotenv
from datetime import datetime, date


load_dotenv()


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


class MissingEnvironmentVariable(Exception):
    pass


def telegram(message):
    # teg = get_notifier('telegram')
    # teg.notify(token=TOKEN, chat_id=CHAT_ID, message=message)
    chat_id = get_my_env_var('CHAT_ID')
    token = get_my_env_var('TOKEN')
    topic = get_my_env_var('TOPIC')
    message_id = get_my_env_var('ID')
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {"chat_id": f"{chat_id}/{topic}", "text": message,
              'reply_to_message_id': message_id}  # Добавляем /2 для указания второго подканала
    response = requests.get(url, params=params)


def serialize_datetime(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type not serializable")
