import json
import logging
import sys
import time
from typing import Any, Dict, List

import numpy as np
import psycopg2

# For explainability (placeholder)
import shap
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from psycopg2.extras import RealDictCursor, execute_values
from pydantic import BaseModel
from transformers import pipeline

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BiasDetectionService")

# -------------------------------
# Utility: Load external configuration
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

CONFIG_FILE = "config.json"
config = load_config(CONFIG_FILE)

# -------------------------------
# PostgreSQL Connection
# -------------------------------
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

pg_conn = get_pg_connection(config["postgres"])

# -------------------------------
# Initialize Sentiment Analysis Pipeline
# -------------------------------
sentiment_model = config["sentiment"]["model_name"]
sentiment_pipeline = pipeline("sentiment-analysis", model=sentiment_model)

# -------------------------------
# FastAPI Application Initialization
# -------------------------------
app = FastAPI(title="Bias Detection & Fairness Monitoring Service")

# Pydantic model for bias report (for API responses)
class BiasReport(BaseModel):
    log_id: int
    user_id: str
    diversity_score: float
    average_sentiment: float
    bias_flag: bool
    explanation: str
    timestamp: float

# -------------------------------
# Fetch Recommendation Logs from PostgreSQL
# -------------------------------
def fetch_recommendation_logs() -> List[Dict[str, Any]]:
    """
    Fetch recommendation logs that have not yet been processed for bias.
    Assumes a boolean flag 'processed_for_bias' in the logs table.
    """
    try:
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = f"""
                SELECT * FROM {config['postgres']['recommendation_logs_table']}
                WHERE processed_for_bias = FALSE;
            """
            cursor.execute(query)
            logs = cursor.fetchall()
            logger.info("Fetched %d recommendation logs", len(logs))
            return logs
    except Exception as e:
        logger.error("Error fetching recommendation logs: %s", e)
        return []

# -------------------------------
# Mark Log as Processed
# -------------------------------
def mark_log_as_processed(log_id: int):
    try:
        with pg_conn.cursor() as cursor:
            query = f"""
                UPDATE {config['postgres']['recommendation_logs_table']}
                SET processed_for_bias = TRUE
                WHERE log_id = %s;
            """
            cursor.execute(query, (log_id,))
            pg_conn.commit()
            logger.info("Marked log %d as processed", log_id)
    except Exception as e:
        logger.error("Error marking log %d as processed: %s", log_id, e)
        pg_conn.rollback()

# -------------------------------
# Bias Metric Computation
# -------------------------------
def compute_diversity_score(recommendations: List[Dict[str, Any]]) -> float:
    """
    Compute diversity score as the ratio of unique sources to total recommendations.
    """
    sources = [rec.get("source") for rec in recommendations if rec.get("source")]
    if not sources:
        return 0.0
    unique_sources = set(sources)
    return len(unique_sources) / len(sources)

def compute_average_sentiment(recommendations: List[Dict[str, Any]]) -> float:
    """
    Compute the average sentiment score of the recommended articles.
    Uses the sentiment pipeline on either the headline or summary.
    Positive sentiment is scored positively; negative sentiment is negative.
    """
    sentiments = []
    for rec in recommendations:
        text = rec.get("headline") or rec.get("summary")
        if text:
            result = sentiment_pipeline(text)[0]
            score = result["score"] if result["label"] == "POSITIVE" else -result["score"]
            sentiments.append(score)
    if sentiments:
        return float(np.mean(sentiments))
    return 0.0

def explain_bias(recommendations: List[Dict[str, Any]]) -> str:
    """
    Placeholder for generating an explanation using SHAP/LIME.
    In production, this would compute feature contributions from a model.
    """
    return "Explanation: The recommendation set is dominated by a limited set of sources."

# -------------------------------
# Process a Single Recommendation Log to Compute Bias Metrics
# -------------------------------
def process_log(log: Dict[str, Any]) -> Dict[str, Any]:
    log_id = log["log_id"]
    user_id = log["user_id"]
    recommendations = log.get("recommendations", [])
    
    diversity_score = compute_diversity_score(recommendations)
    average_sentiment = compute_average_sentiment(recommendations)
    
    # Retrieve thresholds from configuration
    diversity_threshold = config["bias_detection"].get("diversity_threshold", 0.5)
    sentiment_threshold = config["bias_detection"].get("sentiment_threshold", 0.0)
    
    # Flag bias if diversity is below threshold or sentiment is skewed
    bias_flag = (diversity_score < diversity_threshold) or (average_sentiment < sentiment_threshold)
    explanation = explain_bias(recommendations)
    
    bias_report = {
        "log_id": log_id,
        "user_id": user_id,
        "diversity_score": diversity_score,
        "average_sentiment": average_sentiment,
        "bias_flag": bias_flag,
        "explanation": explanation,
        "timestamp": log.get("timestamp", time.time())
    }
    return bias_report

# -------------------------------
# Store Bias Report in PostgreSQL
# -------------------------------
def store_bias_report(report: Dict[str, Any]):
    try:
        with pg_conn.cursor() as cursor:
            query = f"""
                INSERT INTO {config['postgres']['bias_reports_table']}
                (log_id, user_id, diversity_score, average_sentiment, bias_flag, explanation, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s))
                ON CONFLICT (log_id) DO UPDATE SET
                    diversity_score = EXCLUDED.diversity_score,
                    average_sentiment = EXCLUDED.average_sentiment,
                    bias_flag = EXCLUDED.bias_flag,
                    explanation = EXCLUDED.explanation,
                    timestamp = EXCLUDED.timestamp;
            """
            cursor.execute(query, (
                report["log_id"],
                report["user_id"],
                report["diversity_score"],
                report["average_sentiment"],
                report["bias_flag"],
                report["explanation"],
                report["timestamp"]
            ))
            pg_conn.commit()
            logger.info("Stored bias report for log_id %d", report["log_id"])
    except Exception as e:
        logger.error("Error storing bias report for log_id %d: %s", report["log_id"], e)
        pg_conn.rollback()

# -------------------------------
# API Endpoint: Trigger Bias Processing
# -------------------------------
@app.post("/process_bias")
def process_bias():
    logs = fetch_recommendation_logs()
    if not logs:
        return {"message": "No new recommendation logs to process."}
    
    reports = []
    for log in logs:
        report = process_log(log)
        store_bias_report(report)
        mark_log_as_processed(log["log_id"])
        reports.append(report)
    return {"message": "Processed bias reports.", "reports": reports}

# -------------------------------
# API Endpoint: Retrieve Bias Reports
# -------------------------------
@app.get("/bias_reports", response_model=List[BiasReport])
def get_bias_reports(limit: int = Query(10, gt=0)):
    try:
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = f"""
                SELECT * FROM {config['postgres']['bias_reports_table']}
                ORDER BY timestamp DESC
                LIMIT %s;
            """
            cursor.execute(query, (limit,))
            reports = cursor.fetchall()
            return reports
    except Exception as e:
        logger.error("Error retrieving bias reports: %s", e)
        raise HTTPException(status_code=500, detail="Error retrieving bias reports.")

# -------------------------------
# Main Entry Point
# -------------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
