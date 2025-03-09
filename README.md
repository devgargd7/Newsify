# News Recommendation Pipeline

### Overview
The News Recommendation Pipeline is a comprehensive MLOps-driven system designed to ingest news articles from RSS feeds, process and cluster them into stories, generate summaries, and provide personalized recommendations to users. Built with scalability and maintainability in mind, it leverages a Kubernetes-based deployment on a single EC2 instance using Minikube, integrating modern tools like Kafka, Redis, MongoDB, Airflow, and Kubeflow to create a robust end-to-end pipeline. The system includes advanced features such as model drift detection, automatic retraining, and bias monitoring, ensuring high-quality recommendations that adapt to changing user behavior and content trends.

## Key Features
- Real-Time Ingestion: Scrapes articles from RSS feeds and streams them via Kafka.
- Story Clustering: Groups articles into coherent stories using UMAP and HDBSCAN.
- Summarization: Generates concise summaries for stories using a lightweight transformer model (DistilBART).
- Personalized Recommendations: Combines collaborative filtering (ALS) and content-based methods (FAISS) for hybrid recommendations.
- Model Drift Detection: Monitors recommendation performance and triggers retraining when drift exceeds a threshold.
- MLOps Integration: Uses Airflow for orchestration, Kubeflow for pipeline management, and a feature store in MongoDB.
- API Gateway: Exposes RESTful endpoints for recommendations and story retrieval.
- Bias Detection: Monitors recommendation diversity and sentiment to ensure fairness.

## Project Architecture
### High-Level Architecture

The pipeline consists of several microservices interacting through Kubernetes DNS-based service discovery, with data flowing through Kafka, Redis, and MongoDB for processing and storage.

```mermaid
flowchart TD
  %% Data Ingestion Pipeline
  subgraph "Data Ingestion Pipeline"
    A[Ingestion Service<br>scrapes news data]
    B[Kafka<br>Message Broker]
    A -->|Publishes scraped news| B
    C[Duplication Service<br>creates initial story groupings]
    B -->|Consumes messages| C
  end

  %% Story Processing Pipeline
  subgraph "Story Processing Pipeline"
    D[MongoDB<br>Raw Articles Collection]
    E[MongoDB<br>Stories Collection]
    C -->|Writes grouped data| D
    C -->|Writes grouped data| E
    F[Batch Cluster Service<br>refines clusters]
    D & E -->|Reads data| F
    G[Summarization Service<br>generates summaries]
    F -->|Outputs refined stories| G
    G -->|Updates stories| E
  end

  %% User Interaction & Recommendation
  subgraph "User Interaction & Recommendation"
    H[User Behaviour Service<br>tracks user interactions]
    H -->|Stores interactions| E
    I[Recommendation Trainer<br>trains recommendation model]
    E -->|Provides story data| I
    H -->|Provides user data| I
  end

  %% API & External Access
  subgraph "API & Endpoints"
    J[API Gateway Service<br>exposes endpoints, drift checking]
    J -->|Exposes endpoints| A
    J -->|Exposes endpoints| H
  end

  %% MLops Orchestration & Components
  subgraph "MLops & Orchestration"
    K[Airflow<br>Orchestrates batch jobs]
    K --> A
    K --> F
    K --> G
    K --> I
    L[Kubeflow<br>ML pipelines, experiment tracking]
    M[Feast<br>Feature Store]
    N[MLflow<br>Model Store & Tracking]
    O[Drift Detection Endpoint<br>computes drift score]
    J --> O
  end

  %% Additional Infrastructure
  subgraph "Infrastructure"
    P[Redis<br>Cache / Session Store]
    J --> P
  end

  %% External Users
  Q[Users]
  Q -->|API calls| J
```

### Component Descriptions
- **Ingestion Service**: Scrapes news data from various sources and publishes the results to Kafka for downstream processing.

- Kafka (Message Broker): Serves as the communication backbone by asynchronously relaying news data from the Ingestion Service to the Streaming Pipeline.

- Streaming Pipeline (Duplication Service): Processes articles from Kafka, extracts features (embeddings, entities), and clusters them into stories in MongoDB.

- MongoDB: Acts as the primary datastore with separate collections for raw news articles and processed story data.

- Batch Cluster Refinement: Periodically reads grouped data from MongoDB, refines story clusters and updates the FAISS index for content-based recommendations.

- Summarization Service: Generates concise summaries for the refined story clusters using DistilBART and updates the corresponding records in MongoDB.

- User Behaviour Service: Tracks user interactions (such as clicks and views) and stores this behavioral data in MongoDB for further analysis.

- Recommendation Trainer: Periodically trains the recommendation model using both story data and user interaction data an ALS model, computes user embeddings, and precomputes hybrid recommendations, stored in MongoDB, typically orchestrated as a batch job via Airflow.

- API Gateway Service: Exposes HTTP endpoints for both external users and internal services (including model drift checking) and acts as the single entry point for API calls.

- Drift Detection Endpoint: Computes the model drift score, which is used to determine if the recommendation model needs to be retrained.

- Airflow: Orchestrates all batch jobs (ingestion, clustering, summarization, and training) through scheduled DAGs, ensuring smooth workflow execution.

- Kubeflow: Integrates with the ML pipeline for experiment tracking, model deployment, and overall ML workflow management.

- Feast (Feature Store): Centralizes feature management for machine learning, ensuring consistent access to features during both training and serving.

- MLflow (Model Store & Tracking): Provides model versioning, experiment tracking, and storage capabilities to support reproducible ML workflows.

- Redis: Functions as a caching layer, enhancing performance for the API Gateway and other components that require quick data access.

- Users: External clients that interact with the system via the UI, consuming news, summaries, and personalized recommendations.

### Data Flow

```mermaid
sequenceDiagram
    participant RSS as RSS Feeds
    participant Ingestion as Ingestion Service
    participant Kafka as Kafka Broker
    participant Streaming as Streaming Pipeline
    participant MongoDB as MongoDB
    participant Clustering as Batch Cluster Refinement
    participant FAISS as FAISS Index
    participant Summarization as Summarization Service
    participant Trainer as Recommendation Trainer
    participant API as API Service
    participant User as User

    RSS->>Ingestion: Fetch Articles
    Ingestion->>Kafka: Publish Articles
    Kafka->>Streaming: Consume Articles
    Streaming->>MongoDB: Store Articles/Stories
    Clustering->>MongoDB: Fetch Articles
    Clustering->>FAISS: Update Index
    Summarization->>MongoDB: Fetch Stories
    Summarization->>MongoDB: Update Summaries
    Trainer->>MongoDB: Fetch Interactions
    Trainer->>FAISS: Query Content-Based Recs
    Trainer->>MongoDB: Store Recommendations
    User->>API: Request Recommendations
    API->>MongoDB: Fetch Recommendations
    API->>User: Return Stories
```

## Setup
### Prerequisites
- AWS Account: For EC2 instance provisioning.
- Terraform: To deploy the EC2 instance.
- Minikube: For local Kubernetes cluster on EC2.
- Docker: For containerizing services.
- Python 3.9+: For running services locally.
- Dependencies: Listed in requirements.txt.


### 1. Provision Infrastructure
Initialize Terraform:
```bash
cd terraform
terraform init
terraform apply -auto-approve
```

SSH into EC2:
```bash
ssh -i your-key.pem ec2-user@<instance_public_ip>
```
### 2. Start Minikube
```bash
minikube start --driver=docker --memory=12000 --cpus=4
minikube addons enable ingress
minikube addons enable metrics-server
```

### 3. Deploy Dependencies
MongoDB:
```bash
kubectl apply -f k8s/mongo-deployment.yaml
```

Kafka:
```bash
kubectl apply -f k8s/kafka-deployment.yaml
```

Redis:
```bash
kubectl apply -f k8s/redis-deployment.yaml
```

PostgreSQL (for bias detection):
```bash
kubectl apply -f k8s/postgres-deployment.yaml
```

### 4. Build and Deploy Services
For each service:

Build Docker Image:
```bash
docker build -t your-registry/<service>:latest -f <service>.Dockerfile .
docker push your-registry/<service>:latest
```

Deploy to Kubernetes:
```bash
kubectl apply -f k8s/<service>-deployment.yaml
```

### 5. Configure Orchestration
Airflow:
```bash
helm install airflow apache-airflow/airflow --set executor=KubernetesExecutor
kubectl cp dags/recommendation_trainer_dag.py airflow-pod:/opt/airflow/dags/
```

Kubeflow:
```bash
curl -s "https://raw.githubusercontent.com/kubeflow/manifests/v1.8.0-rc.1/install.sh" | bash
```