import pendulum
import logging
import duckdb
import requests
import time
import json

from airflow.sdk import Variable
from airflow.sdk import Variable
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# Конфигурация DAG
OWNER = 'd.vor'
DAG_ID = 'github_raw_from_s3_to_details_s3'

# Используемые таблицы в S3
BUCKET = 'prod'
LAYER = 'github'
SOURCE = 'users_details'

# S3
ACCESS_KEY = Variable.get('access_key')
SECRET_KEY = Variable.get('secret_key')

#GitHub
GITHUB_TOKEN =Variable.get('github_token')

#GitHub API
USER_FIELDS = [
    "login", "id", "node_id", "avatar_url", "gravatar_id", "url", "html_url",
    "followers_url", "following_url", "gists_url", "starred_url",
    "subscriptions_url", "organizations_url", "repos_url", "events_url",
    "received_events_url", "type", "site_admin", "name", "company", "blog",
    "location", "email", "hireable", "bio", "twitter_username",
    "public_repos", "public_gists", "followers", "following",
    "created_at", "updated_at",
    "private_gists", "total_private_repos", "owned_private_repos",
    "disk_usage", "collaborators", "two_factor_authentication",
]

default_args = {
    'owner': OWNER,
    'start_date': pendulum.datetime(2026, 6, 10),
    'catchup': True,
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1),
}


def get_user_details_to_s3(**context):

    start_date = context['data_interval_start'].format('YYYY-MM-DD')
    logging.info(f'Начало загрузки за дату: {start_date}')

    users_s3_path = f's3://{BUCKET}/{LAYER}/users/{start_date}/{start_date}.parquet'

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
        """
    )

    # Чтение данных их DAG github_raw_from_s3_to_detais_s3
    logins = [
        raw[0] for raw in con.sql(
            f"SELECT login FROM read_parquet('{users_s3_path}')"
        ).fetchall()
    ]

    con.close()
    logging.info(f'Записано {len(logins)}')

    headers = {
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }

    if GITHUB_TOKEN:
        headers['Authorization'] = f'{GITHUB_TOKEN}'

    enriched = []
    for login in logins:
        resp = requests.get(
            f'https://api.github.com/users/{login}',
            headers=headers,
            timeout=30,
        )

        if resp.status_code != 200:
            logging.warning(f'Пропуск: {login}: HTTP {resp.status_code}')
            continue

        data = resp.json()

        # Распаковываем nested "plan" в плоские поля. Postgres не хранит struct напрямую
        plan = data.get('plan') or {}

        record = {field: data.get(field) for field in USER_FIELDS}
        record['plan_name'] = plan.get('name')
        record['plan_space'] = plan.get('space')
        record['plan_private_repos'] = plan.get('private_repos')
        record['plan_collaborators'] = plan.get('collaborators')

        enriched.append(record)
        time.sleep(0.1)

    logging.info(f'Записано {len(enriched)} / {len(logins)} пользователей')

    if not enriched:
        logging.warning('Нет обогащенных записей, запись в S3 пропускается')
        return

    tmp_path = f'/tmp/github_user_details_{start_date}.json'
    with open(tmp_path, 'w') as f:
        json.dump(enriched, f)

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
    logging.info(f'Записано {len(enriched)} обогащений для записей {s3_path}')

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule='@daily',
    catchup=True,
    tags={LAYER, SOURCE},
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    wait_for_users = ExternalTaskSensor(
        task_id="wait_for_github_users_etl",
        external_dag_id="github_raw_from_api_to_s3",
        external_task_id="end",
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
    )

    extract_details = PythonOperator(
        task_id="extract_github_user_details_to_s3",
        python_callable=get_user_details_to_s3,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> wait_for_users >> extract_details >> end























