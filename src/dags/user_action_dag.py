from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from src.etl_pipeline.jobs import user_action_log_job
from src.database.manager import init_db

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_pipeline_json_logs',
    default_args=default_args,
    description='ETL pipeline to process JSON logs and load into PostgreSQL',
    schedule_interval=None,  # Manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'json', 'postgres'],
) as dag:
    """
    DAG to orchestrate ETL pipeline:
    - Initialize database
    - Ingest JSON logs
    - Transform data
    - Load into PostgreSQL
    """

    init_db_task = PythonOperator(
        task_id="init_db",
        python_callable=init_db,
    )

    ingest_data_task = PythonOperator(
        task_id='ingest_data',
        python_callable=user_action_log_job.run_ingest,
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=user_action_log_job.run_transform,
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=user_action_log_job.run_load,
    )

    # Define task dependencies
    init_db_task >> ingest_data_task >> transform_data_task >> load_data_task
