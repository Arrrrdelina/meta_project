from config import AIRTABLE_BASE_ID, AIRTABLE_API_KEY, AIRTABLE_TABLE_NAME
from datetime import datetime
from pony.orm import *

# -----------Получение данных с помощью http запроса get в формате json--------------
import requests
import enum

url = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}?maxRecords=3&view=Grid%20view'

# python requests headers
headers = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
}

r = requests.get(url, headers=headers)

# --------------Отправка данных в базу данных с помощью ORM PonyORM -------------------------------
db = Database()


class RawData(db.Entity):
    id = PrimaryKey(int, auto=True)
    updated_dt = Required(datetime, default=datetime.now)
    data = Required(Json)


class Therapist(db.Entity):
    id = PrimaryKey(str)
    name = Required(str)
    methods = Required(str)
    photo = Required(str)


db.bind(provider='postgres', user='adelegevorkan', host='localhost', database='meta_base')

db.generate_mapping(create_tables=True)  # создаем таблицы базы данных, в которых мы будем сохранять наши данные

BATCH_SIZE = 10
data = r.json()
records = data.get('records', [])
size = len(records)

# добавление(создание) и обновление данных в БД
for i in range(0, size, BATCH_SIZE):
    with db_session:
        RawData(data=r.json())  # добавить данные в таблицу RawData

        for j in range(i, min(size, i + BATCH_SIZE)):
            curr_record = records[j]
            therapist = Therapist.get(id=curr_record['id'])  # проверяем есть ли в нашей базе такой терапевт
            if therapist is None:
                field = curr_record['fields']
                Therapist(id=curr_record['id'], name=field['Имя'], photo=field['Фотография'][0]['url'],
                          methods='/ '.join(field['Методы']))
                print(field['Фотография'][0]['url'])
            else:
                field = curr_record['fields']
                therapist.name = field['Имя']
                therapist.photo = field['Фотография'][0]['url']
                therapist.methods = '/'.join(field['Методы'])

# удаление данных из БД
airtable_ids = {r['id'] for r in records}  # ключи терапевтов из airtable
with db_session:
    db_ids = {t.id for t in Therapist.select()}  # ключи терапевтов из базы данных

ids_to_delete = db_ids - airtable_ids
for id in ids_to_delete:
    with db_session:
        t = Therapist.get(id=id)
        t.delete()
