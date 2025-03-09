import json
import logging
import os
from datetime import datetime, timezone

import numpy as np
import pymongo
import spacy
import yake
import yaml
from bson import ObjectId
from confluent_kafka import Consumer, KafkaError
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

# Load configuration from environment variables with defaults
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
NLP_MODEL = os.getenv("NLP_MODEL", "en_core_web_sm")
SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", 0.7))
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "news")
MONGO_COLLECTION_ARTICLES = os.getenv("MONGO_COLLECTION_ARTICLES", "articles")
MONGO_COLLECTION_STORIES = os.getenv("MONGO_COLLECTION_STORIES", "stories")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "articles")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "streaming_pipeline")

# Logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Rest of the code remains unchanged
# ...

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("streaming_pipeline.log")]
)
logger = logging.getLogger(__name__)

def load_spacy_model(model_name):
    try:
        logger.info(f"Loading spaCy model: {model_name}")
        nlp = spacy.load(model_name)
        logger.info(f"Successfully loaded spaCy model: {model_name}")
        return nlp
    except OSError:
        logger.warning(f"spaCy model '{model_name}' not found. Downloading...")
        spacy.cli.download(model_name)
        nlp = spacy.load(model_name)
        return nlp

nlp = load_spacy_model(NLP_MODEL)
embedder = SentenceTransformer('all-MiniLM-L6-v2')
kw_extractor = yake.KeywordExtractor()

def extract_features(article):
    text = article.get("title", "") + " " + article.get("text", "")
    embedding = embedder.encode(text, convert_to_tensor=False)
    doc = nlp(text)
    entities = [ent.text for ent in doc.ents]
    # keywords = [kw[0] for kw in kw_extractor.extract_keywords(text)]
    return {
        'embedding': embedding.tolist(),
        'entities': entities,
        # 'keywords': keywords
    }

def categorize_article(article, stories, embedder):
    embedding = np.array(article['embedding'])
    if stories:
        valid_stories = {sid: s for sid, s in stories.items() if s['centroid'].size > 0}
        if valid_stories:
            story_ids = list(valid_stories.keys())
            centroids = np.array([valid_stories[sid]['centroid'] for sid in story_ids])
            similarities = cosine_similarity([embedding], centroids)[0]
            max_similarity = np.max(similarities)
            if max_similarity > SIMILARITY_THRESHOLD:
                story_id = story_ids[np.argmax(similarities)]
                return story_id, False
    # Create a new story with a persistent ID
    new_story_id = str(ObjectId())  # Changed to use persistent ObjectId
    stories[new_story_id] = {
        "centroid": embedding,
        "articles": [article["link"]]
    }
    return new_story_id, True

def store_article_and_story(db, article, story_id, is_new_story=False):
    try:
        article["processed_at"] = datetime.now(timezone.utc).isoformat()
        article["story_id"] = story_id  # Ensure article references the persistent story ID
        db[MONGO_COLLECTION_ARTICLES].insert_one(article)
        if is_new_story:
            db[MONGO_COLLECTION_STORIES].insert_one({
                "_id": story_id,
                "articles": [article["link"]],
                "centroid": article["embedding"],
                # "keywords": article["keywords"], 
                "entities": article["entities"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "last_updated": datetime.now(timezone.utc).isoformat()
            })
        else:
            db[MONGO_COLLECTION_STORIES].update_one(
                {"_id": story_id},
                {
                    "$push": {"articles": article["link"]},
                    "$set": {"last_updated": datetime.now(timezone.utc).isoformat()}
                }
            )
    except pymongo.errors.PyMongoError as e:
        logger.error(f"Failed to store article/story: {e}")

def initialize_kafka_consumer():
    consumer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest"
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])
    return consumer

def main():
    try:
        mongo_client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        db = mongo_client[MONGO_DB]
        logger.info("Connected to MongoDB")
    except pymongo.errors.ConnectionFailure as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return

    consumer = initialize_kafka_consumer()
    logger.info("Kafka consumer initialized")

    stories = {
        str(story["_id"]): {
            "centroid": np.array(story.get("centroid", [])),
            "articles": story["articles"]
        }
        for story in db[MONGO_COLLECTION_STORIES].find({}, {"_id": 1, "centroid": 1, "articles": 1})
    }
    logger.info(f"Loaded {len(stories)} existing stories from MongoDB")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    break
            try:
                article = json.loads(msg.value().decode("utf-8"))
                features = extract_features(article)
                article.update(features)
                story_id, is_new_story = categorize_article(article, stories, embedder)
                store_article_and_story(db, article, story_id, is_new_story)
                logger.info(f"Processed article {article['link']} into story {story_id}")
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error processing message: {e}")
    finally:
        consumer.close()
        mongo_client.close()
        logger.info("Resources cleaned up")

if __name__ == "__main__":
    main()