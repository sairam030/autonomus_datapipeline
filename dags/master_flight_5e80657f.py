"""
Auto-generated Master Pipeline DAG
Project : flight
Chains  : bronze_flight_5e80657f
Generated: 2026-02-14 11:42:55 UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "autonomous-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def start_pipeline(**ctx):
    logger.info("🚀 Starting master pipeline: flight")


def pipeline_complete(**ctx):
    logger.info("✅ Master pipeline complete: flight")


with DAG(
    dag_id="master_flight_5e80657f",
    default_args=default_args,
    description="Master pipeline — flight",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['auto-generated', 'master', 'flight'],
    max_active_runs=1,
) as dag:

    t_start = PythonOperator(task_id="start_pipeline", python_callable=start_pipeline)
    t_done  = PythonOperator(task_id="pipeline_complete", python_callable=pipeline_complete)

    t_trigger_0 = TriggerDagRunOperator(
        task_id="trigger_bronze_flight_5e80657f",
        trigger_dag_id="bronze_flight_5e80657f",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Chain: start → bronze → silver → gold → done
    t_start >> t_trigger_0 >> t_done
