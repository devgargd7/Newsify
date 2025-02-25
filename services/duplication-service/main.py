import json
import logging
import os

import faiss
import numpy as np
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, first, udf
from pyspark.sql.types import ArrayType, FloatType
from sentence_transformers import SentenceTransformer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeduplicationService")

# Load external configuration
CONFIG_FILE = "config.json"
if not os.path.exists(CONFIG_FILE):
    logger.error(f"Config file {CONFIG_FILE} not found.")
    exit(1)
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

# Spark configuration using the external config
spark = SparkSession.builder \
    .appName(config["spark"]["app_name"]) \
    .config("spark.mongodb.input.uri", f"{config['mongo']['uri']}{config['mongo']['db']}.{config['mongo']['collection']}") \
    .getOrCreate()

logger.info("Spark session initialized with MongoDB connector configuration.")

# -------------------------------
# 1. Load Raw Articles from MongoDB
# -------------------------------
# Use the MongoDB Spark Connector to load raw articles.
raw_articles_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
logger.info("Loaded raw articles from MongoDB.")

# -------------------------------
# 2. Compute Embeddings with SentenceTransformer
# -------------------------------
embedding_model_name = config["embedding"]["model_name"]
embedding_dim = config["embedding"]["embedding_dim"]

model = SentenceTransformer(embedding_model_name)

def compute_embedding(text):
    if text is None:
        return [0.0] * embedding_dim
    emb = model.encode(text)
    return emb.tolist()

embedding_udf = udf(lambda x: compute_embedding(x), ArrayType(FloatType()))
articles_df = raw_articles_df.withColumn("embedding", embedding_udf(col("summary")))
logger.info("Computed embeddings for articles.")

# -------------------------------
# 3. Prepare Features for Clustering
# -------------------------------
def array_to_vector(arr):
    if arr is None:
        return Vectors.dense([])
    return Vectors.dense(arr)

vector_udf = udf(array_to_vector, VectorUDT())
articles_df = articles_df.withColumn("features", vector_udf(col("embedding")))

# -------------------------------
# 4. Cluster Articles using KMeans
# -------------------------------
k = config["clustering"]["k"]
kmeans = KMeans(featuresCol="features", predictionCol="cluster_id", k=k, seed=42)
kmeans_model = kmeans.fit(articles_df)
clustered_df = kmeans_model.transform(articles_df)
logger.info("Clustering complete; sample clusters:")
clustered_df.select("title", "cluster_id").show(5, truncate=False)

# -------------------------------
# 5. Aggregate Clustered Articles into News Stories
# -------------------------------
stories_df = clustered_df.groupBy("cluster_id").agg(
    collect_list("link").alias("articles_links"),
    first("published").alias("story_published"),
    first("source").alias("primary_source")
)

# -------------------------------
# 6. Store Clustered Stories in PostgreSQL
# -------------------------------
jdbc_url = config["postgres"]["jdbc_url"]
jdbc_properties = {
    "user": config["postgres"]["user"],
    "password": config["postgres"]["password"],
    "driver": "org.postgresql.Driver"
}

stories_df.write.jdbc(url=jdbc_url, table=config["postgres"]["table_stories"], mode="overwrite", properties=jdbc_properties)
logger.info("Clustered news stories stored in PostgreSQL.")

# -------------------------------
# 7. Build FAISS Index for Similarity Search
# -------------------------------
articles_embeddings = clustered_df.select("link", "embedding").rdd.map(lambda row: (row.link, row.embedding)).collect()
links = [item[0] for item in articles_embeddings]
embeddings = [item[1] for item in articles_embeddings]

embeddings_array = np.array(embeddings).astype('float32')
d = embeddings_array.shape[1]  # dimension of embeddings

index = faiss.IndexFlatL2(d)
index.add(embeddings_array)
logger.info(f"FAISS index built with {index.ntotal} vectors.")

faiss_index_file = config["faiss"]["index_file"]
faiss.write_index(index, faiss_index_file)
logger.info(f"FAISS index saved to disk at {faiss_index_file}.")

# -------------------------------
# 8. Stop Spark Session
# -------------------------------
spark.stop()
logger.info("Deduplication & Clustering Service completed.")
