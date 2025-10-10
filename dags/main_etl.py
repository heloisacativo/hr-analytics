from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=5)
}


with DAG(
    dag_id='main_etl',
    default_args=default_args,
    description='Master DAG to orchestrate extract and upload dataset',
    schedule_interval=None,
    start_date=datetime(2025, 8, 10),
    catchup=False,
    tags=['etl']
) as dag:

    extract_to_bucket = TriggerDagRunOperator(
        task_id='extract_to_bucket',
        trigger_dag_id='extract_to_bucket',  
        wait_for_completion=True,  
        poke_interval=30,
        failed_states=['failed']
    )

    transform_databricks = TriggerDagRunOperator(
        task_id='transform_databricks',
        trigger_dag_id='transform_databricks', 
        wait_for_completion=True, 
        poke_interval=30,
        failed_states=['failed']
    )

    extract_to_bucket >> transform_databricks
