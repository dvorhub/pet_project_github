import requests
import pendulum
import duckdb
import logging
import json

from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


OWNER = "d.vor"
DAG_ID = "raw_github_from_api_to_s3"
LAYER = "raw"
SOURCE = "github"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 6, 10, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(hours=1),
}


def get_and_transfer_api_data_to_s3(**context):
    access_key = Variable.get("access_key")
    secret_key = Variable.get("secret_key")

    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    logging.info(f"Starting ETL for date: {start_date}")

    # EXTRACT
    response = requests.get(
        "https://api.github.com/users",
        params={"per_page": 10},
        timeout=30,
    )
    response.raise_for_status()
    users = response.json()
    logging.info(f"Fetched {len(users)} users from GitHub API")

    # Сохраняем во временный файл — read_json_auto принимает только путь к файлу
    tmp_path = f"/tmp/github_users_{start_date}.json"
    with open(tmp_path, "w") as f:
        json.dump(users, f)

    s3_path = f"s3://{LAYER}/{SOURCE}/users/{start_date}/{start_date}.parquet"

    # LOAD — используем con.sql() с многострочным SQL, как в рабочем примере
    con = duckdb.connect()
    con.sql(
        f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_use_ssl = FALSE;

        COPY (
            SELECT * FROM read_json_auto('{tmp_path}')
        ) TO '{s3_path}' (FORMAT PARQUET, COMPRESSION SNAPPY);
        """
    )
    con.close()

    logging.info(f"Written {len(users)} records to {s3_path}")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule="@daily",
    catchup=True,
    tags={LAYER, SOURCE},
) as dag:

    start = EmptyOperator(task_id="start")

    extract_load = PythonOperator(
        task_id="extract_and_load_github_users",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_load >> end

# import requests
# import pendulum
# import duckdb
# import logging
#
# from airflow import DAG
# from airflow.models import Variable
# from airflow.providers.standard.operators.python import PythonOperator
# from airflow.providers.standard.operators.empty import EmptyOperator
#
# # Конфигурация DAG
# OWNER = "d.vor"
# DAG_ID = "raw_github_from_api_to_s3"
#
# # Используемые таблицы в DAG
# LAYER = "raw"
# SOURCE = "github"
#
# # S3
# ACCESS_KEY = Variable.get("access_key")
# SECRET_KEY = Variable.get("secret_key")
#
# default_args = {
#     "owner": OWNER,
#     "start_date": pendulum.datetime(2026, 6, 10, tz="Europe/Moscow"),
#     "catchup": True,
#     "retries": 2,
#     "retry_delay": pendulum.duration(hours=1),
# }
#
# # Получение данных о пользователях
# # def extract_users(**context):
# #     url = "https://api.github.com/users"
# #     response = requests.get(url)
# #     response.raise_for_status()
# #     return response.json()
#
# # Нейминг дат в airflow
# def get_dates(**context) -> tuple[str, str]:
#
#     start_date = context["data_interval_start"].format("YYYY-MM-DD")
#     end_date = context["data_interval_end"].format("YYYY-MM-DD")
#
#     return start_date, end_date
#
# # Добавление данных в bucket s3
# def get_and_transfer_api_data_to_s3(**context):
#     start_date, end_date = get_dates(**context)
#     logging.info(f"Start load for dates: {start_date}/{end_date}")
#     con = duckdb.connect()
#
#     con.sql(
#         f"""
#         SET TIMEZONE='UTC';
#         INSTALL httpfs;
#         LOAD httpfs
#         SET s3_url_style = 'path';
#         SET s3_endpoint = 'minio:9000';
#         SET s3_access_key_id = '{ACCESS_KEY}';
#         SET s3_secret_access_key = '{SECRET_KEY}';
#         SET s3_use_ssl = FALSE;
#
#         COPY
#         (
#             SELECT
#                 *
#             FROM
#
#         )
#         """,
#     )
#
#     con.close()
#     logging.info(f"Download for date success: {start_date}")
#     pass
#
# # Зарузка данных в duckdb
# # def load_to_duckdb(**context):
# #     users = context["ti"].xcom_pull(task_ids="extract_users")
# #
# #     con = duckdb.connect("github_raw.duckdb")
# #
# #     con.execute("""
# #         CREATE TABLE IF NOT EXISTS raw_users (
# #             login VARCHAR,
# #             id BIGINT,
# #             type VARCHAR,
# #             site_admin BOOLEAN
# #         )
# #     """)
# #
# #     for u in users:
# #         con.execute("""
# #             INSERT INTO raw_users VALUES (?, ?, ?, ?)
# #         """, [
# #             u.get("login"),
# #             u.get("id"),
# #             u.get("type"),
# #             u.get("site_admin")
# #         ])
#
#
#
#
# # with DAG(
# #     dag_id=DAG_ID,
# #     schedule="@daily",
# #     catchup=False,
# #     default_args=default_args
# # ) as dag:
# #
# #     start = EmptyOperator(task_id="start")
# #
# #     extract_users_task = PythonOperator(
# #         task_id="extract_users",
# #         python_callable=extract_users
# #     )
# #
# #     load_task = PythonOperator(
# #         task_id="load_to_duckdb",
# #         python_callable=load_to_duckdb
# #     )
# #
# #     end = EmptyOperator(task_id="end")
# #
# #     start >> extract_users_task >> load_task >> end