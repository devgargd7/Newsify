from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


def check_drift():
    response = requests.get("http://recommendation-service:5001/metrics")
    if response.json()["drift_score"] > 0.1:
        requests.post("http://kubeflow-pipeline:8080/retrain")

dag = DAG("model_drift", start_date=datetime(2025, 3, 6), schedule_interval="@daily")
drift_task = PythonOperator(task_id="check_drift", python_callable=check_drift, dag=dag)