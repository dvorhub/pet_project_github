import pendulum
import duckdb
import logging

from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

OWNER = "d.vor"
DAG_ID = "github_raw_from_s3_to_pg"
LAYER = "raw"
SOURCE = "github"
SCHEMA = "ods"
TARGET_TABLE = "fct_github_users"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 6, 10, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(hours=1),
}


def get_and_transfer_raw_data_to_ods_pg(**context):
    access_key = Variable.get("access_key")
    secret_key = Variable.get("secret_key")
    password = Variable.get("pg_password")

    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    logging.info(f"Start load for date: {start_date}")

    s3_path = f"s3://{LAYER}/{SOURCE}/users/{start_date}/{start_date}.parquet"

    con = duckdb.connect()
    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_use_ssl = FALSE;

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'postgres_dwh',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD '{password}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

        -- Идемпотентность: удаляем записи за текущую дату перед вставкой
        DELETE FROM dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        WHERE _loaded_at::DATE = '{start_date}'::DATE;

        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            id,
            login,
            node_id,
            type,
            site_admin,
            html_url
        )
        SELECT
            id,
            login,
            node_id,
            type,
            site_admin,
            html_url
        FROM read_parquet('{s3_path}');
        """
    )
    con.close()
    logging.info(f"Load to ODS success for date: {start_date}")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule="@daily",
    catchup=True,
    tags=[LAYER, SOURCE],
) as dag:

    start = EmptyOperator(task_id="start")

    # Ждём успешного завершения upstream DAG за тот же интервал
    wait_for_extract = ExternalTaskSensor(
        task_id="wait_for_github_users_etl",
        external_dag_id="github_users_etl",
        external_task_id="end",
        mode="reschedule",       # не блокирует worker slot во время ожидания
        timeout=3600,            # максимум 1 час ожидания
        poke_interval=60,        # проверяем каждую минуту
    )

    load_to_ods = PythonOperator(
        task_id="load_github_users_to_ods",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_extract >> load_to_ods >> end