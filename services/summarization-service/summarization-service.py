import logging
import os
import time
from datetime import datetime, timezone

import pymongo
import torch
from transformers import pipeline

# Load configuration from environment variables
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "news")
MONGO_COLLECTION_STORIES = os.getenv("MONGO_COLLECTION_STORIES", "stories")
MONGO_COLLECTION_ARTICLES = os.getenv("MONGO_COLLECTION_ARTICLES", "articles")
MODEL_NAME = os.getenv("SUMMARIZATION_MODEL", "facebook/bart-large-cnn")
TRAINING_INTERVAL = int(os.getenv("TRAINING_INTERVAL", 3600))

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

from torch.quantization import quantize_dynamic

summarizer = pipeline(
    "summarization",
    model=MODEL_NAME,
    tokenizer=MODEL_NAME
)
summarizer.model = quantize_dynamic(summarizer.model, {torch.nn.Linear}, dtype=torch.qint8)


def generate_summary(text):
    with torch.amp.autocast('cpu'):
        return summarizer(
            text,
        )[0]["summary_text"]

def summarize_stories():
    mongo_client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
    db = mongo_client[MONGO_DB]
    stories_collection = db[MONGO_COLLECTION_STORIES]
    articles_collection = db[MONGO_COLLECTION_ARTICLES]
    articles_collection.create_index("link")
    stories_to_summarize = stories_collection.find({
        "$or": [
            {"last_summarized": {"$exists": False}},
            {"$expr": {"$gt": ["$last_updated", "$last_summarized"]}}
        ]
    })

    for story in stories_to_summarize:
        story_id = story["_id"]
        article_links = story["articles"]
        text = " ".join([a["text"] for a in articles_collection.find({"link": {"$in": article_links}})])
        summary = generate_summary(text)
        stories_collection.update_one(
            {"_id": story_id},
            {"$set": {"summary": summary, "last_summarized": datetime.now(timezone.utc).isoformat()}}
        )
        logger.info(f"Summarized story {story_id}")

def main():
    while True:
        start_time = time.time()
        summarize_stories()
        elapsed = time.time() - start_time
        if elapsed < TRAINING_INTERVAL:
            time.sleep(TRAINING_INTERVAL - elapsed)

if __name__ == "__main__":
    main()