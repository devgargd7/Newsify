import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


def check_drift(**context):
    try:
        response = requests.get("http://api-service:8001/drift_score")
        response.raise_for_status()
        drift_score = float(response.json()["drift_score"])
        if drift_score > float(os.getenv("DRIFT_THRESHOLD", 0.2)):
            return 'train_model'
        return 'skip_training'
    except Exception as e:
        # Optionally, log the error and default to retraining or skipping as needed.
        return 'skip_training'

def run_recommendation_trainer():
    # Execute the recommendation_trainer script
    os.system("python recommendation_trainer.py")

with DAG(
    'recommendation_trainer',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Run recommendation trainer with drift detection',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 6),
    catchup=False,
) as dag:
    check_drift_task = BranchPythonOperator(
        task_id='check_drift',
        python_callable=check_drift,
    )
    
    train_model_task = PythonOperator(
        task_id='train_model',
        python_callable=run_recommendation_trainer,
    )
    
    skip_training_task = DummyOperator(
        task_id='skip_training',
    )
    
    check_drift_task >> [train_model_task, skip_training_task]
