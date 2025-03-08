import logging
import time
from datetime import datetime, timezone

import pymongo
import torch
import yaml
from transformers import pipeline

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

summarizer = pipeline(
    "summarization",
    model=config["model"]["summarization"]["model_name"],
    tokenizer=config["model"]["summarization"]["model_name"]
)

def generate_summary(text):
    with torch.amp.autocast('cpu'):
        return summarizer(
            text,
        )[0]["summary_text"]

def summarize_stories():
    mongo_client = pymongo.MongoClient(config["mongo"]["host"], config["mongo"]["port"])
    db = mongo_client[config["mongo"]["db"]]
    stories_collection = db[config["mongo"]["collections"]["stories"]]
    articles_collection = db[config["mongo"]["collections"]["articles"]]
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
        if elapsed < config["training"]["interval"]:
            time.sleep(config["training"]["interval"] - elapsed)

if __name__ == "__main__":
    main()