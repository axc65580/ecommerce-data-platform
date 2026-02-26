from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = '/opt/airflow'

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_pipeline',
    default_args=default_args,
    description='E-Commerce Data Platform Pipeline',
    schedule_interval='@hourly',
    catchup=False,
    tags=['ecommerce', 'delta-lake', 'dbt'],
) as dag:

    start = BashOperator(
        task_id='pipeline_start',
        bash_command='echo "Pipeline started at 02/26/2026 16:11:26"',
    )

    run_silver = BashOperator(
        task_id='run_silver_processor',
        bash_command='cd /opt/airflow && python spark_streaming/silver_processor.py',
    )

    run_gold = BashOperator(
        task_id='run_gold_processor',
        bash_command='cd /opt/airflow && python spark_streaming/gold_processor.py',
    )

    load_duckdb = BashOperator(
        task_id='load_to_duckdb',
        bash_command='cd /opt/airflow && python dbt_project/load_to_duckdb.py',
    )

    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_project/ecommerce_dbt && dbt run',
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/airflow/dbt_project/ecommerce_dbt && dbt test',
    )

    end = BashOperator(
        task_id='pipeline_end',
        bash_command='echo "Pipeline completed successfully at 02/26/2026 16:11:26"',
    )

    start >> run_silver >> run_gold >> load_duckdb >> run_dbt >> run_dbt_tests >> end
