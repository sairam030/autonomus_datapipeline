"""
Auto-generated Master Pipeline DAG
Project : testing
Chains  : bronze_testing_0ced8497, bronze_silver_testing_0ced8497, bronze_gold_testing_0ced8497
Generated: 2026-02-15 11:29:19 UTC
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
    "retry_delay": timedelta(minutes=5),
}


def start_pipeline(**ctx):
    logger.info("🚀 Starting master pipeline: testing")


def pipeline_complete(**ctx):
    logger.info("✅ Master pipeline complete: testing")


with DAG(
    dag_id="master_testing_0ced8497",
    default_args=default_args,
    description="Master pipeline — testing",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['auto-generated', 'master', 'testing'],
    max_active_runs=1,
) as dag:

    t_start = PythonOperator(task_id="start_pipeline", python_callable=start_pipeline)
    t_done  = PythonOperator(task_id="pipeline_complete", python_callable=pipeline_complete)

    t_trigger_0 = TriggerDagRunOperator(
        task_id="trigger_bronze_testing_0ced8497",
        trigger_dag_id="bronze_testing_0ced8497",
        wait_for_completion=True,
        poke_interval=30,
    )

    t_trigger_1 = TriggerDagRunOperator(
        task_id="trigger_bronze_silver_testing_0ced8497",
        trigger_dag_id="bronze_silver_testing_0ced8497",
        wait_for_completion=True,
        poke_interval=30,
    )

    t_trigger_2 = TriggerDagRunOperator(
        task_id="trigger_bronze_gold_testing_0ced8497",
        trigger_dag_id="bronze_gold_testing_0ced8497",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Chain: start → bronze → silver → gold → done
    t_start >> t_trigger_0 >> t_trigger_1 >> t_trigger_2 >> t_done
