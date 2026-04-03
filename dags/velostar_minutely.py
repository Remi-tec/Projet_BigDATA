from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="velostar_minutely",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 2),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["velostar", "gbfs"],
) as dag:
    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="python /opt/airflow/project/pipeline_velostar.py",
    )
