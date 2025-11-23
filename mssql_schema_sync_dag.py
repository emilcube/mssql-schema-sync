from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from datetime import datetime, timedelta
import subprocess
import logging
import os
from custom_plugins.tg_notifications import send_telegram_sla_alert, send_telegram_alert

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'on_failure_callback': send_telegram_alert,
}

PROJECT_NAME = "mssql_schema_sync"

project_path = Variable.get("airflow_projects_path")

project_dir = f"{project_path}/{PROJECT_NAME}"
poetry_path = Variable.get("poetry_path")

def run_mssql_schema_sync():
    """Run the MSSQL schema sync script via poetry"""
    ## Install dependencies - in first execution
    # logging.info("Installing dependencies via poetry...")
    # try:
    #     subprocess.run(
    #         [poetry_path, "install", "--no-interaction"],
    #         cwd=project_dir,
    #         check=True,
    #         capture_output=True,
    #         text=True
    #     )
    #     logging.info("Dependencies installed")
    # except subprocess.CalledProcessError as e:
    #     raise AirflowException(f"Failed to install dependencies: {e.stderr}")
    
    try:
        result = subprocess.run(
            [poetry_path, "run", "python", "sync.py"],
            cwd=project_dir,
            check=True
        )
        logging.info(f"Sync completed successfully: {result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logging.error(f"Sync failed: {e.stderr}")
        raise AirflowException(f"MSSQL schema sync failed: {e.stderr}")

with DAG(
    dag_id=PROJECT_NAME,
    default_args=default_args,
    schedule='55 23 * * *',
    max_active_runs=1,
    start_date=datetime(2025, 11, 22),
    sla_miss_callback=send_telegram_sla_alert,
    tags=["maintenance"]
) as dag:

    start = EmptyOperator(task_id="start")
    
    # sync_schema = PythonOperator(
    #     task_id='sync_mssql_schema_to_gitlab',
    #     python_callable=run_mssql_schema_sync,
    # )
    sync_schema = BashOperator(
        task_id='sync_mssql_schema_to_gitlab',
        bash_command=f'cd {project_dir} && {poetry_path} run python sync.py',
    )
    
    end = EmptyOperator(
        task_id="end",
        trigger_rule='all_success'
    )

    start >> sync_schema >> end