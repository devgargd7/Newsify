import json
import logging
import time
from datetime import datetime
from typing import Dict, Optional

import pymongo
import uvicorn
import yaml
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UserBehaviorTrackingService")

# Load external configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Set up MongoDB connection
mongo_config = config["mongo"]
try:
    mongo_client = pymongo.MongoClient(
        host=mongo_config["host"],
        port=mongo_config["port"],
        username=mongo_config.get("username"),
        password=mongo_config.get("password"),
        authSource=mongo_config.get("auth_source", "admin")
    )
    db = mongo_client[mongo_config["db"]]
    user_events_collection = db[mongo_config["collections"]["user_interactions"]]
    logger.info("Connected to MongoDB")
except Exception as e:
    logger.error("Error connecting to MongoDB: %s", e)
    raise e

# Create Kafka producer with the provided configuration
kafka_config = config["kafka"]
producer_conf = {
    'bootstrap.servers': kafka_config["bootstrap_servers"]
}
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
    story_id: Optional[str] = None  # Changed from article_id to story_id
    event_time: Optional[float] = Field(default_factory=lambda: time.time())
    metadata: Optional[Dict] = None

# Function to insert a user event into MongoDB
def insert_user_event(event: UserEvent):
    try:
        event_dict = event.model_dump()
        event_dict["event_time"] = datetime.fromtimestamp(event_dict["event_time"])
        user_events_collection.insert_one(event_dict)
        logger.info("User event inserted for user_id: %s", event.user_id)
    except Exception as e:
        logger.error("Error inserting user event: %s", e)
        raise e

# Function to publish a user event to Kafka
def publish_event_to_kafka(event: UserEvent):
    try:
        event_data = event.model_dump()
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
        # Insert event into MongoDB
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