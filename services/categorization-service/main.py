import json
import logging
import sys
import time

import psycopg2
import spacy
from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import execute_values
from pymongo import MongoClient
from rake_nltk import Rake
from transformers import pipeline

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ContentCategorizationService")

CONFIG_FILE = "../config.json"

def load_config(config_file=CONFIG_FILE):
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        logger.info("Configuration loaded from %s", config_file)
        return config
    except Exception as e:
        logger.error("Failed to load configuration: %s", e)
        sys.exit(1)

def get_mongo_db(mongo_config):
    try:
        client = MongoClient(mongo_config["uri"])
        db = client[mongo_config["database"]]
        logger.info("Connected to MongoDB at %s", mongo_config["uri"])
        return db
    except Exception as e:
        logger.error("MongoDB connection error: %s", e)
        sys.exit(1)

def get_postgres_connection(pg_config):
    try:
        conn = psycopg2.connect(
            host=pg_config["host"],
            port=pg_config["port"],
            database=pg_config["database"],
            user=pg_config["user"],
            password=pg_config["password"]
        )
        logger.info("Connected to PostgreSQL at %s", pg_config["host"])
        return conn
    except Exception as e:
        logger.error("PostgreSQL connection error: %s", e)
        sys.exit(1)

def get_es_client(es_config):
    try:
        # If credentials are provided, use them
        if es_config.get("user") and es_config.get("password"):
            es = Elasticsearch(
                [es_config["host"]],
                http_auth=(es_config["user"], es_config["password"])
            )
        else:
            es = Elasticsearch([es_config["host"]])
        logger.info("Connected to Elasticsearch at %s", es_config["host"])
        return es
    except Exception as e:
        logger.error("Elasticsearch connection error: %s", e)
        sys.exit(1)

def fetch_unprocessed_articles(mongo_db, collection_name):
    """
    Fetch articles that have not yet been processed for categorization.
    We assume each article has a flag 'categorization_processed'.
    """
    try:
        collection = mongo_db[collection_name]
        articles = list(collection.find({"categorization_processed": {"$ne": True}}))
        logger.info("Fetched %d unprocessed articles", len(articles))
        return articles
    except Exception as e:
        logger.error("Error fetching articles from MongoDB: %s", e)
        return []

def process_article(article, classifier, candidate_labels, nlp, rake):
    """
    Process a single article:
      - Use zero-shot classification to assign categories.
      - Extract named entities using spaCy.
      - Extract keywords using RAKE.
    """
    # Use 'content' if available; fallback to 'summary'
    text = article.get("content") or article.get("summary")
    if not text:
        logger.warning("Article %s has no text content", article.get("_id"))
        return None
    
    # Zero-shot classification for categories
    classification = classifier(text, candidate_labels)
    categories = classification.get("labels", [])
    
    # Named Entity Recognition with spaCy
    doc = nlp(text)
    entities = list(set([ent.text for ent in doc.ents]))
    
    # Keyword extraction using RAKE
    rake.extract_keywords_from_text(text)
    keywords = rake.get_ranked_phrases()
    
    result = {
        "article_id": str(article.get("_id")),
        "categories": categories,
        "entities": entities,
        "keywords": keywords
    }
    return result

def store_tags_in_postgres(conn, pg_table, tags_data):
    try:
        with conn.cursor() as cursor:
            query = f"""
            INSERT INTO {pg_table} (article_id, categories, entities, keywords)
            VALUES %s
            ON CONFLICT (article_id) DO UPDATE SET
                categories = EXCLUDED.categories,
                entities = EXCLUDED.entities,
                keywords = EXCLUDED.keywords;
            """
            values = [
                (data["article_id"], data["categories"], data["entities"], data["keywords"])
                for data in tags_data
            ]
            execute_values(cursor, query, values)
            conn.commit()
            logger.info("Stored tags for %d articles in PostgreSQL", len(tags_data))
    except Exception as e:
        logger.error("Error storing tags in PostgreSQL: %s", e)
        conn.rollback()

def index_in_elasticsearch(es_client, index_name, tags_data):
    try:
        actions = []
        for data in tags_data:
            action = {
                "_index": index_name,
                "_id": data["article_id"],
                "_source": data
            }
            actions.append(action)
        helpers.bulk(es_client, actions)
        logger.info("Indexed %d articles in Elasticsearch", len(actions))
    except Exception as e:
        logger.error("Error indexing in Elasticsearch: %s", e)

def mark_articles_as_processed(mongo_db, collection_name, article_ids):
    try:
        collection = mongo_db[collection_name]
        result = collection.update_many(
            {"_id": {"$in": article_ids}},
            {"$set": {"categorization_processed": True}}
        )
        logger.info("Marked %d articles as processed", result.modified_count)
    except Exception as e:
        logger.error("Error marking articles as processed in MongoDB: %s", e)

def main():
    # Load configuration from external file
    config = load_config()
    
    # Set up connections
    mongo_db = get_mongo_db(config["mongo"])
    pg_conn = get_postgres_connection(config["postgres"])
    es_client = get_es_client(config["elasticsearch"])
    
    # Candidate labels for zero-shot classification
    candidate_labels = config["categorization"]["candidate_labels"]
    
    # Initialize zero-shot classifier
    classifier = pipeline("zero-shot-classification", model=config["categorization"]["zero_shot_model"])
    
    # Initialize spaCy for NER
    nlp = spacy.load(config["categorization"]["spacy_model"])
    
    # Initialize RAKE for keyword extraction (can be tuned via config if needed)
    rake = Rake()
    
    # Fetch unprocessed articles from MongoDB
    articles = fetch_unprocessed_articles(mongo_db, config["mongo"]["articles_collection"])
    
    tags_results = []
    processed_article_ids = []
    for article in articles:
        result = process_article(article, classifier, candidate_labels, nlp, rake)
        if result:
            tags_results.append(result)
            processed_article_ids.append(article.get("_id"))
    
    if tags_results:
        # Store the categorization results in PostgreSQL
        store_tags_in_postgres(pg_conn, config["postgres"]["tags_table"], tags_results)
        
        # Index results in Elasticsearch
        index_in_elasticsearch(es_client, config["elasticsearch"]["index"], tags_results)
        
        # Mark articles as processed in MongoDB
        mark_articles_as_processed(mongo_db, config["mongo"]["articles_collection"], processed_article_ids)
    else:
        logger.info("No articles processed.")
    
    pg_conn.close()
    logger.info("Content Categorization & Tagging Service completed.")

if __name__ == "__main__":
    main()
