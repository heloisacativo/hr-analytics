from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'transform_gold',
    default_args=default_args,
    description='Transform data to gold metrics',
    schedule_interval=None,
    start_date=datetime(2025, 8, 10),
    catchup=False,
    tags=['etl', 'gold']
) as dag:
    
    gold_by_department = OracleOperator(
        task_id='gold_by_department',
        oracle_conn_id='oracle_autonomous_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS gold_department_metrics AS
        SELECT 
            Department,
            COUNT(*) AS total_employees,
            ROUND(AVG(CASE WHEN Attrition='Yes' THEN 1 ELSE 0 END), 2) AS attrition_rate,
            ROUND(AVG(MonthlyIncome), 2) AS avg_monthly_income,
            TO_DATE(SYSDATE, 'YYYY-MM-DD') AS dt
        FROM bronze_hr_data
        GROUP BY Department
        """
    )

    gold_by_satisfaction = OracleOperator(
        task_id='gold_by_satisfaction',
        oracle_conn_id='oracle_autonomous_conn',
        sql="""
        CREATE OR REPLACE VIEW gold_satisfaction_metrics AS
        SELECT 
            job_satisfaction_ptbr,
            ROUND(AVG(MonthlyIncome), 2) AS avg_monthly_income,
            ROUND(MEDIAN(MonthlyIncome), 2) AS median_monthly_income,
            ROUND(AVG(CASE WHEN Attrition='Yes' THEN 1 ELSE 0 END), 2) AS avg_attrition,
            TO_DATE(SYSDATE, 'YYYY-MM-DD') AS dt
        FROM bronze_hr_data
        GROUP BY job_satisfaction_ptbr
        """
    )

    gold_by_department >> gold_by_satisfaction

