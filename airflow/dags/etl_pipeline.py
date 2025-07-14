import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from data_pipeline.etl_pipeline.jobs import user_action_job
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='etl_pipeline_json_logs',
    default_args=default_args,
    description='ETL pipeline to process JSON logs and load into PostgreSQL',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'json', 'postgres'],
) as dag:
    
    # Step 1: Ingest raw log data
    ingest = PythonOperator(
        task_id='ingest_data',
        python_callable=user_action_job.run_ingest,
        dag=dag
    )

    # Step 2: Transform raw log data
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=user_action_job.run_transform,
        dag=dag
    )

    # Step 3: Load data into PostgreSQL
    load = PythonOperator(
        task_id='load_data',
        python_callable=user_action_job.load_to_postgres,
        dag=dag
    )

    # Task dependencies
    ingest >> transform >> load
