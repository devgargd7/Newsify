import json
import logging
import os
from datetime import datetime

import feedparser
import pymongo

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("NewsIngestionService")

CONFIG_FILE = "config.json"

class NewsIngestionService:
    def __init__(self, config_file=CONFIG_FILE):
        self.rss_feeds, mongo_uri, db_name = self._load_config(config_file)
        # Initialize MongoDB connection
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db["articles"]
        # Create an index on the 'link' field for deduplication (if not exists)
        self.collection.create_index("link", unique=True)
        logger.info("Connected to MongoDB and ensured index on 'link'.")

    def _load_config(self, config_file):
        # Load the RSS feeds configuration from the external JSON file
        if not os.path.exists(config_file):
            logger.error("Config file %s not found.", config_file)
            return []
        with open(config_file, "r", encoding="utf-8") as f:
            configs = json.load(f)
            feeds, mongo_uri, mongo_db = configs["rss_feeds"], configs['mongo']['uri'], configs['mongo']['db']
        logger.info("Loaded %s RSS feed sources from config.", len(feeds))
        return feeds, mongo_uri, mongo_db

    def fetch_and_store(self):
        for feed in self.rss_feeds:
            logger.info("Fetching feed from %s...", feed['name'])
            parsed_feed = feedparser.parse(feed["url"])
            for entry in parsed_feed.entries:
                article = self._prepare_article(feed["name"], entry)
                self._store_article(article)

    def _prepare_article(self, source, entry):
        # Parse and structure the article data
        published = None
        if "published_parsed" in entry and entry.published_parsed:
            published = datetime(*entry.published_parsed[:6])
        elif "updated_parsed" in entry and entry.updated_parsed:
            published = datetime(*entry.updated_parsed[:6])
        else:
            published = datetime.utcnow()

        article = {
            "source": source,
            "title": entry.get("title", "No Title"),
            "link": entry.get("link"),
            "summary": entry.get("summary", ""),
            "published": published,
            "fetched_at": datetime.utcnow(),
            # Additional fields can be added here later
        }
        return article

    def _store_article(self, article):
        try:
            # Insert the article, skipping duplicates based on the 'link'
            self.collection.insert_one(article)
            logger.info("Inserted article: %s", article['title'])
        except pymongo.errors.DuplicateKeyError:
            logger.info("Duplicate article found (skipping): %s", article['title'])

if __name__ == "__main__":
    service = NewsIngestionService()
    service.fetch_and_store()
