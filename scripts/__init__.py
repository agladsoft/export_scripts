TOKEN = '6557326533:AAHy6ls9LhTVTGztix8PUSK7BUSaHVEojXc'
CHAT_ID = '-1002064780308'
TOPIC = '1069'
ID = '1071'


def telegram(message):
    # teg = get_notifier('telegram')
    # teg.notify(token=TOKEN, chat_id=CHAT_ID, message=message)
    chat_id = CHAT_ID
    token = TOKEN
    topic = TOPIC
    message_id = ID
    # teg.notify(token=get_my_env_var('TOKEN'), chat_id=get_my_env_var('CHAT_ID'), message=message)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {"chat_id": f"{chat_id}/{topic}", "text": message,
              'reply_to_message_id': message_id}  # Добавляем /2 для указания второго подканала
    response = requests.get(url, params=params)


def serialize_datetime(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type not serializable")
