import pendulum
import logging
import requests
import json
import duckdb

from airflow.sdk import Variable
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Конфигурация DAG
OWNER = 'd.vor'
DAG_ID = 'github_raw_from_api_to_s3'

# Используемые таблицы в S3
BUCKET = 'prod'
LAYER = 'github'
SOURCE = 'users'

# S3
ACCESS_KEY = Variable.get('access_key')
SECRET_KEY = Variable.get('secret_key')


default_args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2026, 6, 10),
    'catchup': True,
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1),
}

def get_and_transfer_api_data_to_s3(**context):

    start_date = context['data_interval_start'].format('YYYY-MM-DD')
    logging.info(f'Начало загрузки за дату: {start_date}')

    response = requests.get(
        'https://api.github.com/users',
        params={'per_page': 10},
        timeout=30,
    )
    response.raise_for_status()
    users = response.json()
    logging.info(f'Получено пользователей {len(users)} из GitHub API')

    tmp_path = f'/tmp/github_users_{start_date}.json'
    with open(tmp_path, 'w') as f:
        json.dump(users, f)

    s3_path = f's3://{BUCKET}/{LAYER}/{SOURCE}/{start_date}/{start_date}.parquet'

    con = duckdb.connect()

    con.sql(
        f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;
        
        COPY (
            SELECT * FROM read_json_auto('{tmp_path}')
        ) TO '{s3_path}' (FORMAT PARQUET, COMPRESSION SNAPPY);
        """
    )
    con.close()

    logging.info(f'Записано {len(users)} пользователей в {s3_path}')

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule='@daily',
    catchup=True,
    tags={LAYER, SOURCE},
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    extract_load = PythonOperator(
        task_id='extract_and_load_github_users',
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> extract_load >> end



















