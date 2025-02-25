import json
import logging
import time
from typing import Dict, Optional

import psycopg2
import uvicorn
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from psycopg2.extras import Json
from psycopg2.pool import ThreadedConnectionPool
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UserBehaviorTrackingService")

# Load external configuration
def load_config(config_file: str):
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        logger.info("Configuration loaded from %s", config_file)
        return config
    except Exception as e:
        logger.error("Error loading configuration: %s", e)
        raise e

CONFIG_FILE = "config.json"
config = load_config(CONFIG_FILE)

# Set up PostgreSQL connection pool using psycopg2
db_config = config["postgres"]
dsn = (
    f"dbname={db_config['database']} user={db_config['user']} password={db_config['password']} "
    f"host={db_config['host']} port={db_config['port']}"
)
try:
    db_pool = ThreadedConnectionPool(1, 10, dsn)
    logger.info("PostgreSQL connection pool created")
except Exception as e:
    logger.error("Error creating PostgreSQL connection pool: %s", e)
    raise e

# Create Kafka producer with the provided configuration
kafka_config = config["kafka"]
producer_conf = {
    'bootstrap.servers': kafka_config["bootstrap_servers"]
}
# Optionally include SASL settings if provided
if kafka_config.get("user"):
    producer_conf["sasl.mechanisms"] = kafka_config.get("sasl_mechanisms", "PLAIN")
    producer_conf["security.protocol"] = kafka_config.get("security_protocol", "SASL_SSL")
    producer_conf["sasl.username"] = kafka_config["user"]
    producer_conf["sasl.password"] = kafka_config["password"]

producer = Producer(producer_conf)
logger.info("Kafka producer created")

# Initialize FastAPI app
app = FastAPI(title="User Behavior Tracking Service")

# Define a Pydantic model for user events
class UserEvent(BaseModel):
    user_id: str
    event_type: str  # e.g., "click", "read", "like", "share"
    article_id: Optional[str] = None
    event_time: Optional[float] = Field(default_factory=lambda: time.time())
    metadata: Optional[Dict] = None

# Function to insert a user event into PostgreSQL
def insert_user_event(event: UserEvent):
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cursor:
            insert_query = f"""
                INSERT INTO {db_config["table_user_events"]} (user_id, event_type, article_id, event_time, metadata)
                VALUES (%s, %s, %s, to_timestamp(%s), %s);
            """
            cursor.execute(
                insert_query,
                (
                    event.user_id,
                    event.event_type,
                    event.article_id,
                    event.event_time,
                    Json(event.metadata),
                ),
            )
            conn.commit()
            logger.info("User event inserted for user_id: %s", event.user_id)
    except Exception as e:
        logger.error("Error inserting user event: %s", e)
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            db_pool.putconn(conn)

# Function to publish a user event to Kafka
def publish_event_to_kafka(event: UserEvent):
    try:
        event_data = event.dict()
        event_data["timestamp"] = int(time.time())
        message = json.dumps(event_data).encode("utf-8")
        producer.produce(kafka_config["user_behav_topic"], message)
        producer.flush()
        logger.info("Published user event to Kafka for user_id: %s", event.user_id)
    except Exception as e:
        logger.error("Error publishing to Kafka: %s", e)
        raise e

# FastAPI endpoint to track user events
@app.post("/track", status_code=201)
async def track_event(event: UserEvent):
    try:
        # Insert event into PostgreSQL
        insert_user_event(event)
        # Publish event to Kafka for real-time analytics
        publish_event_to_kafka(event)
        return {"message": "User event tracked successfully"}
    except Exception as e:
        logger.error("Error in track_event: %s", e)
        raise HTTPException(status_code=500, detail="Error processing user event")

# Run the service with uvicorn
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
