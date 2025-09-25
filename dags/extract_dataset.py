from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import kagglehub
import zipfile
import os
import shutil

BASE_DIR = os.getenv("BASE_DIR")
RAW_DIR = os.path.join(BASE_DIR, "raw")

def extract_dataset(**kwargs):
    path = kagglehub.dataset_download("pavansubhasht/ibm-hr-analytics-attrition-dataset")

    os.makedirs(RAW_DIR, exist_ok=True)

    files = os.listdir(path)

    for fname in files:
        src = os.path.join(path, fname)

        if fname.lower().endswith(".zip"):
            with zipfile.ZipFile(src, "r") as zf:
                zf.extractall(RAW_DIR)
        else:
            dst = os.path.join(RAW_DIR, fname)
            try:            
                shutil.copy(src, dst)    
            except PermissionError:                
                with open(src, "rb") as r, open(dst, "wb") as w:
                    w.write(r.read())

    expected = os.path.join(RAW_DIR, "WA_Fn-UseC_-HR-Employee-Attrition.csv")

with DAG(
    dag_id="extract_dataset",
    description="Downloads and extracts HR Analytics dataset",
    schedule_interval=None,
    start_date=datetime(2025, 8, 10),
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id="extract_dataset",
        python_callable=extract_dataset,
    )
