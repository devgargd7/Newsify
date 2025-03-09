import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone

import faiss
import numpy as np
import pymongo
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, StringType, StructField, StructType

# Load configuration from environment variables
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "news")
MONGO_COLLECTION_USER_INTERACTIONS = os.getenv("MONGO_COLLECTION_USER_INTERACTIONS", "user_interactions")
MONGO_COLLECTION_STORIES = os.getenv("MONGO_COLLECTION_STORIES", "stories")
MONGO_COLLECTION_RECOMMENDATIONS = os.getenv("MONGO_COLLECTION_RECOMMENDATIONS", "recommendations")
FAISS_INDEX_FILE = os.getenv("FAISS_INDEX_FILE", "faiss_index.bin")
FAISS_MAPPING_FILE = os.getenv("FAISS_MAPPING_FILE", "faiss_mapping.json")
DRIFT_THRESHOLD = float(os.getenv("DRIFT_THRESHOLD", 0.2))
TRAINING_INTERVAL = int(os.getenv("TRAINING_INTERVAL", 86400))

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# ### Helper Functions

def compute_interaction_score(interaction):
    """Compute a score for an interaction based on its event type."""
    scores = {
        "like": 1.0,
        "read": 0.01,
        "share": 0.5,
        "click": 0.1,
        "default": 0.0
    }
    return float(scores.get(interaction.get("event_type", "default")))

def compute_drift_score(db):
    recent_interactions = list(db[MONGO_COLLECTION_USER_INTERACTIONS].find({
        "timestamp": {"$gte": datetime.now(timezone.utc) - timedelta(days=7)}
    }))
    if not recent_interactions:
        return 0.0
    recent_scores = [compute_interaction_score(interaction) for interaction in recent_interactions]
    historical_scores = [compute_interaction_score(interaction) for interaction in db[MONGO_COLLECTION_USER_INTERACTIONS].find()]
    recent_mean = np.mean(recent_scores) if recent_scores else 0
    historical_mean = np.mean(historical_scores) if historical_scores else 0
    drift_score = abs(recent_mean - historical_mean) / historical_mean if historical_mean != 0 else 0
    return drift_score

def compute_user_embedding(user_id, db):
    """Compute user profile embedding as the average of liked story embeddings."""
    interactions = db[MONGO_COLLECTION_USER_INTERACTIONS].find({"user_id": user_id, "event_type": "like"})
    story_ids = [interaction["story_id"] for interaction in interactions]
    if not story_ids:
        return None
    embeddings = []
    for story_id in story_ids:
        story = db[MONGO_COLLECTION_STORIES].find_one({"_id": story_id})
        if story and "centroid" in story:
            embeddings.append(np.array(story["centroid"], dtype="float32"))
    return np.mean(np.stack(embeddings), axis=0) if embeddings else None

def store_user_embedding(db, user_id, embedding):
    db["user_embeddings"].update_one(
        {"user_id": user_id},
        {"$set": {"embedding": embedding.tolist(), "last_updated": datetime.now().isoformat()}},
        upsert=True
    )
def compute_freshness_factor(last_updated, current_time, lambda_=0.1):
    """Calculate freshness using exponential decay based on time difference."""
    time_diff = (current_time - last_updated).total_seconds() / 3600  # in hours
    return math.exp(-lambda_ * time_diff)

def load_faiss_index():
    """Load the FAISS index and mapping from disk."""
    index = faiss.read_index(FAISS_INDEX_FILE)
    with open(FAISS_MAPPING_FILE, "r") as f:
        mapping = json.load(f)  # {str(index): story_id}
    return index, mapping

def get_faiss_recommendations(user_embedding, index, mapping, top_m=50):
    """Retrieve content-based recommendations using FAISS."""
    if user_embedding is None:
        return []
    query_vector = np.expand_dims(user_embedding, axis=0)
    distances, indices = index.search(query_vector, top_m)
    recs = []
    for idx, dist in zip(indices[0], distances[0]):
        story_id = mapping.get(str(idx))
        if story_id:
            recs.append({"story_id": story_id, "faiss_similarity": float(dist)})
    return recs

def get_als_recommendations(user_id, model, user_indexer_model, item_indexer_model, top_k=50):
    """Retrieve collaborative recommendations using the ALS model."""
    spark = SparkSession.builder.getOrCreate()
    user_to_index = {label: float(idx) for idx, label in enumerate(user_indexer_model.labels)}
    index_to_story = {float(idx): label for idx, label in enumerate(item_indexer_model.labels)}

    if user_id not in user_to_index:
        return []

    user_index = user_to_index[user_id]
    user_df = spark.createDataFrame([(user_index,)], ["userIndex"])
    als_recs = model.recommendForUserSubset(user_df, top_k)
    recommendations = als_recs.collect()
    if not recommendations:
        return []

    rec_list = recommendations[0].recommendations
    return [{"story_id": index_to_story[rec.storyIndex], "als_score": float(rec.rating)} 
            for rec in rec_list if rec.storyIndex in index_to_story]

def get_popular_stories(db, top_n=50):
    """Retrieve the top N most interacted-with stories."""
    pipeline = [
        {"$group": {"_id": "$story_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": top_n}
    ]
    popular = list(db[MONGO_COLLECTION_USER_INTERACTIONS].aggregate(pipeline))
    return [story["_id"] for story in popular]

def combine_recommendations(als_recs, faiss_recs, db, w_als=0.5, w_faiss=0.5, top_n=10):
    """Combine ALS and FAISS recommendations, adjusting for freshness."""
    combined = {}
    current_time = datetime.now(timezone.utc)

    # Normalize ALS scores
    als_range = (max(rec["als_score"] for rec in als_recs) - min(rec["als_score"] for rec in als_recs)) if als_recs else 1
    for rec in als_recs:
        story_id = rec["story_id"]
        als_score = rec["als_score"] / als_range if als_range > 0 else 0
        story = db[MONGO_COLLECTION_STORIES].find_one({"_id": story_id})
        if story:
            last_updated = datetime.fromisoformat(story["last_updated"])
            freshness = compute_freshness_factor(last_updated, current_time)
            combined[story_id] = (w_als * als_score, 0, freshness)

    # Normalize FAISS similarities
    faiss_range = (max(rec["faiss_similarity"] for rec in faiss_recs) - min(rec["faiss_similarity"] for rec in faiss_recs)) if faiss_recs else 1
    for rec in faiss_recs:
        story_id = rec["story_id"]
        faiss_similarity = rec["faiss_similarity"] / faiss_range if faiss_range > 0 else 0
        story = db[MONGO_COLLECTION_STORIES].find_one({"_id": story_id})
        if story:
            last_updated = datetime.fromisoformat(story["last_updated"])
            freshness = compute_freshness_factor(last_updated, current_time)
            if story_id in combined:
                als_component, _, freshness = combined[story_id]
                combined[story_id] = (als_component, w_faiss * faiss_similarity, freshness)
            else:
                combined[story_id] = (0, w_faiss * faiss_similarity, freshness)

    # Compute final scores
    final_recs = []
    for story_id, (als_component, faiss_component, freshness) in combined.items():
        score = (als_component + faiss_component) * freshness
        final_recs.append({"story_id": story_id, "score": score})

    final_recs.sort(key=lambda x: x["score"], reverse=True)
    return final_recs[:top_n]

# ### Main Training Function

def train_and_precompute_recommendations(db):
    """Train the ALS model and precompute hybrid recommendations for all users."""
    spark = SparkSession.builder.appName("RecommendationTrainer").master("local[*]").getOrCreate()

    # Connect to MongoDB
    try:
        mongo_client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        db = mongo_client[MONGO_DB]
        interactions_collection = db[MONGO_COLLECTION_USER_INTERACTIONS]
        logger.info("Connected to MongoDB")
    except pymongo.errors.ConnectionFailure as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        spark.stop()
        return

    # Load interactions
    interactions = list(interactions_collection.find())
    if not interactions:
        logger.info("No user interactions found. Skipping training.")
        spark.stop()
        return
    logger.info(f"Retrieved {len(interactions)} interactions.")

    # Process interactions
    processed_interactions = [
        {
            "_id": str(interaction["_id"]),
            "user_id": interaction["user_id"],
            "story_id": interaction["story_id"],
            "score": compute_interaction_score(interaction)
        }
        for interaction in interactions
    ]

    # Define schema and create DataFrame
    schema = StructType([
        StructField("_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("story_id", StringType(), True),
        StructField("score", FloatType(), True)
    ])
    df = spark.createDataFrame(processed_interactions, schema=schema)
    df = df.dropDuplicates(["user_id", "story_id"]).filter(col("score").isNotNull() & (col("score") >= 0))

    # Index users and stories
    user_indexer = StringIndexer(inputCol="user_id", outputCol="userIndex")
    user_indexer_model = user_indexer.fit(df)
    df = user_indexer_model.transform(df)

    item_indexer = StringIndexer(inputCol="story_id", outputCol="storyIndex")
    item_indexer_model = item_indexer.fit(df)
    df = item_indexer_model.transform(df)

    # Train ALS model
    als = ALS(
        rank=5,
        userCol="userIndex",
        itemCol="storyIndex",
        ratingCol="score",
        implicitPrefs=True,
        coldStartStrategy="drop"
    )
    model = als.fit(df)
    model.write().overwrite().save("als_model")
    logger.info("ALS model trained and saved.")

    # Load FAISS index
    index, mapping = load_faiss_index()

    # Precompute recommendations
    recommendations_collection = db[MONGO_COLLECTION_RECOMMENDATIONS]
    users = set(interaction["user_id"] for interaction in processed_interactions)
    for user_id in users:
        user_embedding = compute_user_embedding(user_id, db)
        if user_embedding is not None:
            store_user_embedding(db, user_id, user_embedding)
            als_recs = get_als_recommendations(user_id, model, user_indexer_model, item_indexer_model)
            faiss_recs = get_faiss_recommendations(user_embedding, index, mapping)
            final_recs = combine_recommendations(als_recs, faiss_recs, db)
        else:
            # Fallback for users with no likes
            popular_stories = get_popular_stories(db)
            final_recs = [{"story_id": sid, "score": 1.0} for sid in popular_stories]

        # Store recommendations
        recommendations_collection.update_one(
            {"user_id": user_id},
            {"$set": {"recommendations": final_recs, "last_updated": datetime.now().isoformat()}},
            upsert=True
        )
    logger.info(f"Precomputed recommendations for {len(users)} users.")
    spark.stop()

# ### Main Loop

def main():
    """Run the recommendation training and precomputation periodically."""
    mongo_client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
    db = mongo_client[MONGO_DB]
    while True:
        start_time = time.time()
        drift_score = compute_drift_score(db)
        logger.info(f"Current drift score: {drift_score}")
        if drift_score > DRIFT_THRESHOLD:
            logger.info("Drift detected. Triggering retraining.")
            train_and_precompute_recommendations(db)
        else:
            logger.info("No significant drift. Skipping retraining.")
        elapsed = time.time() - start_time
        if elapsed < TRAINING_INTERVAL:
            time.sleep(TRAINING_INTERVAL - elapsed)

if __name__ == "__main__":
    main()