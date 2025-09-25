from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging

log = logging.getLogger(__name__)

def transform_to_bronze(**context):

    file_path = os.getenv('FILE_PATH')
    df = pd.read_csv(file_path)
    
    log.info(f"FILE_PATH_BRONZE: {file_path}")
 
    df['dt'] = datetime.now().strftime('%Y-%m-%d')
    df['job_satisfaction'] = df['JobSatisfaction'] 
    df['monthly_income'] = df['MonthlyIncome']

    satisfaction_feels = {
        1: 'Muito baixa',
        2: 'Baixa',
        3: 'Alta',
        4: 'Muito alta'
        }
    df['job_satisfaction_ptbr'] = df['job_satisfaction'].map(satisfaction_feels)
 
    output_path = '/opt/airflow/data/bronze/'
    os.makedirs(output_path, exist_ok=True)
    df.to_parquet(f"{output_path}/hr_data.parquet", index=False)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'transform_bronze',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 8, 10),
    catchup=False
) as dag:
    
    bronze_task = PythonOperator(
        task_id='transform_bronze',
        python_callable=transform_to_bronze
    )