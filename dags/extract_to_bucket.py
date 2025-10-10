from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import kagglehub
import zipfile
import os
import tempfile
import oci

def extract_dataset(**kwargs):
    path = kagglehub.dataset_download("pavansubhasht/ibm-hr-analytics-attrition-dataset")

    files = os.listdir(path)
    print(f"Arquivos no diretório após download: {files}")

    for fname in files:
        src = os.path.join(path, fname)

        if fname.lower().endswith(".zip"):
            with zipfile.ZipFile(src, "r") as zf:
                zf.extractall(path)  
                print(f"Arquivos extraídos: {zf.namelist()}")
        else:
            print(f"Arquivo {fname} não será movido.")

    extracted_file = "WA_Fn-UseC_-HR-Employee-Attrition.csv"
    extracted_path = os.path.join(path, extracted_file)

    temp_dir = tempfile.gettempdir()
    temp_file_path = os.path.join(temp_dir, extracted_file)

    # Verifique se o arquivo já foi movido
    if os.path.exists(temp_file_path):
        print(f"Arquivo já existe em {temp_file_path}. Pulando movimentação.")
        return {"file_path": temp_file_path}

    # Verifique se o arquivo existe antes de movê-lo
    if not os.path.exists(extracted_path):
        raise FileNotFoundError(f"Arquivo esperado não encontrado: {extracted_path}")

    os.rename(extracted_path, temp_file_path)
    print(f"Arquivo movido para {temp_file_path}")

    return {"file_path": temp_file_path}

def upload_to_bucket(**kwargs):
    ti = kwargs['ti']
    xcom_data = ti.xcom_pull(task_ids='extract_dataset') 
    file_path = xcom_data["file_path"]
    print(f"File to upload: {file_path}")

    bucket = os.environ["OCI_BUCKET_NAME"]
    profile = os.getenv("OCI_PROFILE", "DEFAULT")

    cfg_path = os.path.expanduser("~/.oci/config")
    config = oci.config.from_file(cfg_path, profile_name=profile)
    client = oci.object_storage.ObjectStorageClient(config)
    namespace = client.get_namespace().data

    object_name = "raw/WA_Attrition.csv"

    with open(file_path, "rb") as f:
        file_content = f.read()
        resp = client.put_object(namespace, bucket, object_name, file_content, content_type="text/csv")

    print(
        f"Uploaded to oci://{namespace}/{bucket}/{object_name} | etag={resp.headers.get('etag')} | opc-request-id={resp.headers.get('opc-request-id')}"
    )
    return {"namespace": namespace, "bucket": bucket, "object": object_name}

with DAG(
    dag_id="extract_to_bucket",
    description="Downloads, extracts, and uploads HR Analytics dataset",
    schedule_interval=None,
    start_date=datetime(2025, 8, 10),
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id="extract_dataset",
        python_callable=extract_dataset,
    )
    upload_task = PythonOperator(
        task_id="upload_to_bucket",
        python_callable=upload_to_bucket,
        provide_context=True,
    )

    extract_task >> upload_task

