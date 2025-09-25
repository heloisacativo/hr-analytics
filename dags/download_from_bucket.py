import oci
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def download_from_bucket(**_):
    bucket = os.environ["OCI_BUCKET_NAME"]
    profile = os.getenv("OCI_PROFILE", "DEFAULT")
    cfg_path = os.path.expanduser("~/.oci/config")

    config = oci.config.from_file(cfg_path, profile_name=profile)
    client = oci.object_storage.ObjectStorageClient(config)
    namespace = client.get_namespace().data

    # Lista todos os objetos do bucket (sem prefixo)
    objects = client.list_objects(namespace, bucket, prefix="hr/").data.objects
    if not objects:
        raise FileNotFoundError("No objects found in bucket with prefix 'hr/'")
    for obj in objects:
        object_name = obj.name
        dest_path = os.path.join("/opt/airflow/data/raw", os.path.basename(object_name))
        response = client.get_object(namespace, bucket, object_name)
        with open(dest_path, "wb") as f:
            for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):
                f.write(chunk)
        print(f"Downloaded object {object_name} to {dest_path}")

with DAG(
    dag_id="download_from_bucket",
    description="Download the latest HR dataset from OCI bucket",
    schedule_interval=None,
    start_date=datetime(2025, 8, 10),
    catchup=False,
    tags=["etl"]
) as dag:
    download_task = PythonOperator(
        task_id="download_from_bucket",
        python_callable=download_from_bucket,
    )