import logging
import math
import time
from datetime import datetime

import numpy as np
import pymongo
import yaml
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import SparkSession
from sklearn.metrics.pairwise import cosine_similarity

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# MongoDB configuration
MONGO_HOST = config["mongo"]["host"]
MONGO_PORT = config["mongo"]["port"]
MONGO_DB = config["mongo"]["db"]
MONGO_COLLECTION_USER_INTERACTIONS = config["mongo"]["collections"]["user_interactions"]
MONGO_COLLECTION_STORIES = config["mongo"]["collections"]["stories"]
MONGO_COLLECTION_ARTICLES = config["mongo"]["collections"]["articles"]

def generate_recommendations(user_id, db, top_n=10, candidate_k=100, content_m=50):
    '''
    Generate personalized story recommendations for a user.

    Args:
        user_id (str): The ID of the user.
        db (pymongo.database.Database): MongoDB database instance.
        top_n (int): Number of recommendations to return.
        candidate_k (int): Number of ALS candidates to fetch.
        content_m (int): Number of content-based candidates to fetch.

    Returns:
        list: List of recommended story IDs.
    '''
    spark = SparkSession.builder.getOrCreate()
    model = ALSModel.load("als_model")

    # Get ALS recommendations
    user_df = spark.createDataFrame([{"user_id": user_id}])
    als_recs = model.recommendForUserSubset(user_df, candidate_k)
    als_story_ids = [row.story_id for row in als_recs.collect()[0].recommendations]

    # Compute user embedding for content-based filtering
    user_embedding = compute_user_embedding(user_id, db)
    if user_embedding is not None:
        content_recs = get_top_content_based(user_embedding, db, content_m)
    else:
        content_recs = []

    # Combine candidate stories
    candidates = list(set(als_story_ids + content_recs))

    # Compute final scores with freshness
    final_scores = []
    current_time = datetime.now()
    for story_id in candidates:
        story = db[MONGO_COLLECTION_STORIES].find_one({"_id": story_id})
        if story and 'centroid' in story and 'last_updated' in story:
            last_updated = datetime.fromisoformat(story['last_updated'])
            freshness = compute_freshness_factor(last_updated, current_time)
            als_score = get_als_score(model, user_id, story_id) if story_id in als_story_ids else 0
            content_score = (cosine_similarity([user_embedding], [story['centroid']])[0][0]
                            if user_embedding is not None else 0)
            score = (0.7 * als_score + 0.3 * content_score) * freshness
            final_scores.append((story_id, score))

    # Rank and select top N
    final_scores.sort(key=lambda x: x[1], reverse=True)
    spark.stop()
    return [story_id for story_id, _ in final_scores[:top_n]]

def compute_user_embedding(user_id, db):
    '''Compute user embedding by averaging embeddings of liked stories.'''
    interactions = db[MONGO_COLLECTION_USER_INTERACTIONS].find({"user_id": user_id, "like": 1})
    story_ids = [interaction['story_id'] for interaction in interactions]
    if not story_ids:
        return None
    embeddings = []
    for story_id in story_ids:
        story = db[MONGO_COLLECTION_STORIES].find_one({"_id": story_id})
        if story and 'centroid' in story:
            embeddings.append(np.array(story['centroid']))
    return np.mean(embeddings, axis=0) if embeddings else None

def get_top_content_based(user_embedding, db, m):
    '''Get top M stories based on content similarity to user embedding.'''
    stories = list(db[MONGO_COLLECTION_STORIES].find({}, {"_id": 1, "centroid": 1}))
    similarities = []
    for story in stories:
        if 'centroid' in story:
            sim = cosine_similarity([user_embedding], [np.array(story['centroid'])])[0][0]
            similarities.append((story['_id'], sim))
    similarities.sort(key=lambda x: x[1], reverse=True)
    return [story_id for story_id, _ in similarities[:m]]

def compute_freshness_factor(last_updated, current_time, lambda_=0.1):
    '''Compute freshness factor based on time since last update.'''
    time_diff = (current_time - last_updated).total_seconds() / 3600  # Convert to hours
    return math.exp(-lambda_ * time_diff)

def get_als_score(model, user_id, story_id):
    '''Retrieve ALS predicted score for a user-story pair (placeholder).'''
    # In practice, use model.transform() on a DataFrame with (user_id, story_id)
    # This is a simplification; actual implementation depends on model API usage
    return 1.0  # Placeholder
