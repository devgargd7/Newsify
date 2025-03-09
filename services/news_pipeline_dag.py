from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1)
}

with DAG('news_pipeline', schedule_interval='@hourly', default_args=default_args) as dag:
    ingestion = KubernetesPodOperator(
        name="ingestion",
        namespace="default",
        image="myproject/ingestion_service:latest",
        cmds=["python", "ingestion_service.py"]
    )
    
    duplication = KubernetesPodOperator(
        name="duplication",
        namespace="default",
        image="myproject/duplication_service:latest",
        cmds=["python", "duplication_service.py"]
    )
    
    clustering = KubernetesPodOperator(
        name="clustering",
        namespace="default",
        image="myproject/batch_cluster_refinement:latest",
        cmds=["python", "batch-cluster-refinement.py"]
    )
    
    summarization = KubernetesPodOperator(
        name="summarization",
        namespace="default",
        image="myproject/summarization_service:latest",
        cmds=["python", "summarization-service.py"]
    )
    
    recommendation = KubernetesPodOperator(
        name="recommendation",
        namespace="default",
        image="myproject/recommendation_trainer:latest",
        cmds=["python", "recommendation-trainer.py"]
    )
    
    # Define task dependencies as needed
    ingestion >> duplication >> clustering >> summarization >> recommendation
