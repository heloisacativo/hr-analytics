from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

DATABRICKS_JOB_ID = "834692105477972"  
OCI_BUCKET_NAME = os.getenv("OCI_BUCKET_NAME")
OBJECT_PREFIX = os.getenv("OBJECT_PREFIX", "hr")
OCI_TENANCY = os.getenv("OCI_TENANCY")
OCI_USER = os.getenv("OCI_USER")
OCI_FINGERPRINT = os.getenv("OCI_FINGERPRINT")
OCI_PRIVATE_KEY_PATH = './config/oci_api_key.pem' 
OCI_PASSPHRASE = os.getenv("OCI_PASSPHRASE")
OCI_NAMESPACE = os.getenv("OCI_NAMESPACE")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='transform_databricks',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 8, 10),
    catchup=False
) as dag:

    run_job = DatabricksRunNowOperator(
        task_id='run_serverless_job',
        databricks_conn_id='databricks_default',
        job_id=DATABRICKS_JOB_ID,
        notebook_params={
            "input_path": f"/raw/WA_Attrition.csv",
            "output_path": f"/bronze/hr_data.parquet",
            "oci_tenancy": OCI_TENANCY,
            "oci_user": OCI_USER,
            "oci_fingerprint": OCI_FINGERPRINT,
            "oci_private_key_path": OCI_PRIVATE_KEY_PATH,
            "oci_passphrase": OCI_PASSPHRASE,
            "oci_namespace": OCI_NAMESPACE,
            "oci_bucket": OCI_BUCKET_NAME
        }
    )