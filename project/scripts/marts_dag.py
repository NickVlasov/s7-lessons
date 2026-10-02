from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

SCRIPTS_DIR = "/opt/airflow/scripts"
PY_FILES = f"{SCRIPTS_DIR}/mart_core.py"

default_args = {
    "owner": "s30080726",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="marts_pipeline",
    default_args=default_args,
    description="Ежедневное построение витрин: user_mart, zone_mart, friend_rec_mart",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["marts", "geo", "spark"],
) as dag:

    build_user_mart = BashOperator(
        task_id="build_user_mart",
        bash_command=f"spark-submit --py-files {PY_FILES} {SCRIPTS_DIR}/user_mart.py",
    )

    build_zone_mart = BashOperator(
        task_id="build_zone_mart",
        bash_command=f"spark-submit --py-files {PY_FILES} {SCRIPTS_DIR}/zone_mart.py",
    )

    build_friend_rec_mart = BashOperator(
        task_id="build_friend_rec_mart",
        bash_command=f"spark-submit --py-files {PY_FILES} {SCRIPTS_DIR}/friend_rec_mart.py",
    )

    # user_mart → zone_mart → friend_rec_mart
    # Запуск последовательно, чтобы не перегружать кластер
    build_user_mart >> build_zone_mart >> build_friend_rec_mart
