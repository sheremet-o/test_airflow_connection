import requests
import psycopg2

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def extract_data():
    """Перенос данных из источника в БД"""
    # извлекаем данные
    url = "https://random-data-api.com/api/cannabis/random_cannabis?size=10"
    response = requests.get(url)
    data = response.json()

    # устанавливаем соединение с бд
    conn = psycopg2.connect(
        dbname="random_data",
        user="postgres",
        password="secret",
        host="172.25.16.1",
        port="5432")
    cur = conn.cursor()

    # переносим данные
    for i in data:
        cur.execute(
            "INSERT INTO random_data (id, uid, strain,"
            "cannabinoid_abbreviation,"
            "cannabinoid, terpene, medical_use, health_benefit, category,"
            "type, buzzword, brand) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (i['id'], i['uid'],
            i['strain'],
            i['cannabinoid_abbreviation'],
            i['cannabinoid'],
            i['terpene'],
            i['medical_use'],
            i['health_benefit'],
            i['category'],
            i['type'],
            i['buzzword'],
            i['brand'],))

    # сохраняем изменения и закрываем бд
    conn.commit()
    conn.close()


def check_data():
    """Проверка, что после загрузки в БД столько же данных,
    сколько в источнике"""
    # подсчитываем количество записей в источнике
    url = "https://random-data-api.com/api/cannabis/random_cannabis?size=10"
    response = requests.get(url)
    data = response.json()
    source_count = len(data)

    # подсчитываем количество записей в бд
    conn = psycopg2.connect("dbname=random_data user=postgres password=secret")
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM random_data")
    bd_count = cur.fetchone()[0]

    conn.close()

    if bd_count == source_count:
        print("Перенос данных завершен. Количество объектов в БД"
              "соответствует источнику.")
    else:
        print("Перенос данных завершен. Количество объектов в БД не"
              "соответствует источнику!")


dag = DAG("random_data",
          schedule_interval=timedelta(hours=12),
          start_date=datetime(2024, 10, 20, 0),
          )

# определяем задачи
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

check_data_task = PythonOperator(
    task_id='check_data',
    python_callable=check_data,
    dag=dag,
)

# расставляем зависимости
extract_data_task >> check_data_task


# Создаем базу данных. Я использовала pgAdmin.
# CREATE TABLE IF NOT EXISTS random_data
# (
#     id INT PRIMARY KEY NOT NULL,
#     uid VARCHAR(255) NOT NULL,
#     strain VARCHAR(255) NOT NULL,
#     cannabinoid_abbreviation VARCHAR(255) NOT NULL,
#     cannabinoid VARCHAR(255) NOT NULL,
#     terpene VARCHAR(255) NOT NULL,
#     medical_use VARCHAR(255) NOT NULL,
#     health_benefit VARCHAR(255) NOT NULL,
#     category VARCHAR(255) NOT NULL,
#     type VARCHAR(255) NOT NULL,
#     buzzword VARCHAR(255) NOT NULL,
#     brand VARCHAR(255) NOT NULL
# )

# ALTER TABLE IF EXISTS random_data
#     OWNER to postgres;
