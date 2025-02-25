import json
import logging
import sys
import time
from typing import Any, Dict, List, Optional

import faiss
import numpy as np
import psycopg2
import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RecommendationEngine")

# -------------------------------
# Utility functions for configuration
# -------------------------------
def load_config(config_file: str) -> dict:
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        logger.info("Loaded configuration from %s", config_file)
        return config
    except Exception as e:
        logger.error("Error loading configuration: %s", e)
        sys.exit(1)

# -------------------------------
# Global Configuration & Connections
# -------------------------------
CONFIG_FILE = "config.json"
config = load_config(CONFIG_FILE)

# PostgreSQL connection function
def get_pg_connection(pg_config: dict):
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
        logger.error("Error connecting to PostgreSQL: %s", e)
        sys.exit(1)

# Connect to Redis
def get_redis_client(redis_config: dict):
    try:
        r = redis.Redis(
            host=redis_config["host"],
            port=redis_config["port"],
            password=redis_config.get("password", None),
            decode_responses=True
        )
        # Simple ping to test connection
        r.ping()
        logger.info("Connected to Redis at %s", redis_config["host"])
        return r
    except Exception as e:
        logger.error("Error connecting to Redis: %s", e)
        sys.exit(1)

pg_conn = get_pg_connection(config["postgres"])
redis_client = get_redis_client(config["redis"])

# -------------------------------
# Load FAISS Index and Mapping
# -------------------------------
try:
    faiss_index = faiss.read_index(config["faiss"]["index_file"])
    # Load the mapping from FAISS index positions to article metadata
    with open(config["faiss"]["mapping_file"], "r") as f:
        faiss_mapping = json.load(f)  # Expected to be a dict: {str(index): { "article_id": ..., "title": ..., ... }, ... }
    logger.info("Loaded FAISS index with %d vectors", faiss_index.ntotal)
except Exception as e:
    logger.error("Error loading FAISS index or mapping: %s", e)
    sys.exit(1)

# -------------------------------
# FastAPI Application Initialization
# -------------------------------
app = FastAPI(title="Recommendation Engine Service")

# Pydantic model for recommendation request
class RecommendationRequest(BaseModel):
    user_id: str
    limit: Optional[int] = 10

# -------------------------------
# Helper Functions for Recommendations
# -------------------------------
def get_user_interactions(user_id: str) -> List[str]:
    """
    Retrieve a list of article IDs that the user has interacted with.
    Assumes user interactions are stored in a table defined in config["postgres"]["user_events_table"].
    """
    try:
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = f"""
                SELECT DISTINCT article_id 
                FROM {config["postgres"]["user_events_table"]}
                WHERE user_id = %s;
            """
            cursor.execute(query, (user_id,))
            results = cursor.fetchall()
            article_ids = [row["article_id"] for row in results if row["article_id"]]
            logger.info("User %s has %d interactions", user_id, len(article_ids))
            return article_ids
    except Exception as e:
        logger.error("Error fetching interactions for user %s: %s", user_id, e)
        return []

def fetch_article_embedding(article_id: str) -> Optional[np.ndarray]:
    """
    Fetch the embedding for a given article ID from PostgreSQL.
    Assumes articles (with embeddings) are stored in the table defined in config["postgres"]["articles_table"],
    and that the embedding is stored as a JSON array.
    """
    try:
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = f"""
                SELECT embedding 
                FROM {config["postgres"]["articles_table"]}
                WHERE article_id = %s;
            """
            cursor.execute(query, (article_id,))
            result = cursor.fetchone()
            if result and result["embedding"]:
                embedding = np.array(json.loads(result["embedding"]), dtype='float32')
                return embedding
            else:
                return None
    except Exception as e:
        logger.error("Error fetching embedding for article_id %s: %s", article_id, e)
        return None

def compute_user_profile_embedding(article_ids: List[str]) -> Optional[np.ndarray]:
    """
    Compute the average embedding vector from the articles the user has interacted with.
    """
    embeddings = []
    for article_id in article_ids:
        emb = fetch_article_embedding(article_id)
        if emb is not None:
            embeddings.append(emb)
    if embeddings:
        avg_embedding = np.mean(np.stack(embeddings), axis=0)
        return avg_embedding
    else:
        return None

def get_collaborative_recommendations(user_id: str, limit: int) -> List[Dict[str, Any]]:
    """
    Placeholder for collaborative filtering.
    In a production system, this might query a feature store (e.g., Feast) or precomputed model.
    Here, we simply return an empty list.
    """
    # For the sake of example, we'll assume no collaborative data is available.
    return []

def get_content_based_recommendations(query_vector: np.ndarray, limit: int) -> List[Dict[str, Any]]:
    """
    Use FAISS to retrieve the nearest neighbors to the query_vector.
    Returns a list of article metadata (as stored in the faiss_mapping).
    """
    query_vector = np.expand_dims(query_vector, axis=0)
    distances, indices = faiss_index.search(query_vector, limit)
    recommendations = []
    for idx in indices[0]:
        metadata = faiss_mapping.get(str(idx))
        if metadata:
            recommendations.append(metadata)
    return recommendations

def combine_recommendations(collab: List[Dict[str, Any]], content: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    """
    Combine collaborative and content-based recommendations.
    For demonstration, if both lists are non-empty, we merge them, remove duplicates, and return the top `limit` items.
    """
    combined = {rec["article_id"]: rec for rec in content}
    for rec in collab:
        combined[rec["article_id"]] = rec
    # Convert to list and take the first `limit` recommendations.
    return list(combined.values())[:limit]

def cache_recommendations(user_id: str, recommendations: List[Dict[str, Any]], expiry: int = 600):
    """
    Cache recommendations in Redis for fast serving.
    """
    key = f"recommendations:{user_id}"
    try:
        redis_client.set(key, json.dumps(recommendations), ex=expiry)
        logger.info("Cached recommendations for user %s in Redis", user_id)
    except Exception as e:
        logger.error("Error caching recommendations for user %s: %s", user_id, e)

def get_cached_recommendations(user_id: str) -> Optional[List[Dict[str, Any]]]:
    """
    Retrieve cached recommendations for a user, if available.
    """
    key = f"recommendations:{user_id}"
    try:
        data = redis_client.get(key)
        if data:
            return json.loads(data)
        else:
            return None
    except Exception as e:
        logger.error("Error retrieving cached recommendations for user %s: %s", user_id, e)
        return None

# -------------------------------
# API Endpoint for Recommendations
# -------------------------------
@app.get("/recommendations", response_model=List[Dict[str, Any]])
def recommendations_endpoint(user_id: str = Query(...), limit: int = Query(10, gt=0)):
    # Check Redis cache first
    cached = get_cached_recommendations(user_id)
    if cached:
        logger.info("Serving recommendations for user %s from cache", user_id)
        return cached

    # Get user interactions
    user_article_ids = get_user_interactions(user_id)
    # Compute user profile embedding if possible
    user_embedding = compute_user_profile_embedding(user_article_ids)
    content_recs = []
    if user_embedding is not None:
        content_recs = get_content_based_recommendations(user_embedding, limit)
    else:
        logger.info("No user embedding available for user %s; falling back to popular content", user_id)
        # In a production system, popular articles would be precomputed.
        content_recs = get_content_based_recommendations(np.zeros(faiss_index.d, dtype='float32'), limit)
    
    # Get collaborative recommendations (placeholder)
    collab_recs = get_collaborative_recommendations(user_id, limit)
    
    # Combine recommendations
    final_recs = combine_recommendations(collab_recs, content_recs, limit)
    
    # Cache recommendations in Redis
    cache_recommendations(user_id, final_recs)
    
    return final_recs

# -------------------------------
# Main Entry Point
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
