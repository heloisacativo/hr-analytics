from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import os
import oracledb
import pandas as pd

FILE_PATH_BRONZE = os.getenv("FILE_PATH_BRONZE")

def upload_to_autonomous_database(**_):
    log = LoggingMixin().log

    if not os.path.exists(FILE_PATH_BRONZE):
        raise FileNotFoundError(f"Input file not found: {FILE_PATH_BRONZE}")

    log.info(f"Reading Parquet file: {FILE_PATH_BRONZE}")
    df = pd.read_parquet(FILE_PATH_BRONZE)
    log.info(f"File loaded successfully. Total rows: {len(df)}")

    user = os.environ["ORACLE_USER"]
    password = os.environ["ORACLE_PASSWORD"]
    tns = os.environ["ORACLE_TNS"]
    wallet_password = os.environ.get("ORACLE_WALLET_PASSWORD")

    log.info("Initializing Oracle client...")
    oracledb.init_oracle_client(lib_dir="/opt/oracle/instantclient", config_dir="/opt/airflow/wallet")

    conn = oracledb.connect(
        user=user,
        password=password,
        dsn=tns,
        config_dir="/opt/oracle/wallet",
        wallet_password=wallet_password
    )
    cursor = conn.cursor()

    headers = df.columns.tolist()
    data = df.values.tolist()

    # Inserção em lotes
    BATCH_SIZE = 1000
    insert_sql = f"INSERT INTO HR_ANALYTICS ({', '.join(headers)}) VALUES ({', '.join([':' + str(i+1) for i in range(len(headers))])})"

    log.info("Starting batch insertion...")
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        log.info(f"Inserted batch {i // BATCH_SIZE + 1} with {len(batch)} rows")

    log.info("All data inserted successfully.")
    cursor.close()
    conn.close()

default_args = {
    "owner": "airflow",
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),  
}

with DAG(
    dag_id="upload_to_autonomous_database",
    description="Uploads dataset to Autonomous Database (oracledb)",
    schedule_interval=None,
    start_date=datetime(2025, 8, 10),
    catchup=False,
    default_args=default_args,
    tags=["etl"],
) as dag:
    upload_task = PythonOperator(
        task_id="upload_to_autonomous_database",
        python_callable=upload_to_autonomous_database,
    )
