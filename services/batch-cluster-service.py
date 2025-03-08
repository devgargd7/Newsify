import json
import logging
from datetime import datetime, timezone

import faiss
import hdbscan
import numpy as np
import pymongo
import umap.umap_ as umap
import yaml
from bson import ObjectId

# Load configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

MONGO_HOST = config["mongo"]["host"]
MONGO_PORT = config["mongo"]["port"]
MONGO_DB = config["mongo"]["db"]
MONGO_COLLECTION_ARTICLES = config["mongo"]["collections"]["articles"]
MONGO_COLLECTION_STORIES = config["mongo"]["collections"]["stories"]
FAISS_INDEX_FILE = config.get("faiss", {}).get("index_file", "faiss_index.bin")
FAISS_MAPPING_FILE = config.get("faiss", {}).get("mapping_file", "faiss_mapping.json")

UMAP_N_COMPONENTS = 5
UMAP_N_NEIGHBORS = 15
UMAP_MIN_DISTANCE = 0.0
HDBSCAN_MIN_CLUSTER_SIZE = 5
HDBSCAN_MIN_SAMPLE = 1

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_articles(db):
    """Fetch articles from MongoDB with necessary fields."""
    return list(db[MONGO_COLLECTION_ARTICLES].find({}, {"_id": 1, "embedding": 1, "story_id": 1, "link": 1}))

def reduce_embeddings(embeddings):
    """Reduce dimensionality of embeddings using UMAP."""
    reducer = umap.UMAP(n_components=UMAP_N_COMPONENTS, n_neighbors=UMAP_N_NEIGHBORS, min_dist=UMAP_MIN_DISTANCE, metric='cosine')
    return reducer.fit_transform(embeddings)

def cluster_articles(reduced_embeddings):
    """Cluster reduced embeddings using HDBSCAN."""
    clusterer = hdbscan.HDBSCAN(min_cluster_size=HDBSCAN_MIN_CLUSTER_SIZE, min_samples=HDBSCAN_MIN_SAMPLE, cluster_selection_method='leaf')
    return clusterer.fit_predict(reduced_embeddings)

def map_clusters_to_stories(new_clusters, existing_stories, threshold=0.7):
    """Map new cluster labels to existing story IDs based on overlap, tracking used stories."""
    story_mapping = {}
    used_story_ids = set()
    for label, article_ids in new_clusters.items():
        article_set = set(article_ids)
        max_overlap = 0
        best_story_id = None
        for story_id, existing_articles in existing_stories.items():
            if story_id in used_story_ids:
                continue
            intersection = len(article_set & existing_articles)
            union = len(article_set | existing_articles)
            jaccard = intersection / union if union > 0 else 0
            if jaccard > max_overlap:
                max_overlap = jaccard
                best_story_id = story_id
        if max_overlap > threshold:
            story_mapping[label] = best_story_id
            used_story_ids.add(best_story_id)
        else:
            new_story_id = str(ObjectId())
            while new_story_id in existing_stories or new_story_id in story_mapping.values():
                new_story_id = str(ObjectId())
            story_mapping[label] = new_story_id
    return story_mapping, used_story_ids

def load_faiss_index():
    """Load existing FAISS index or create a new one if it doesn't exist."""
    try:
        index = faiss.read_index(FAISS_INDEX_FILE)
        with open(FAISS_MAPPING_FILE, "r") as f:
            mapping = json.load(f)  # {str(index_position): story_id}
        logger.info(f"Loaded FAISS index with {index.ntotal} vectors")
    except Exception as e:
        logger.warning(f"Failed to load FAISS index: {e}. Creating a new index.")
        index = faiss.IndexFlatL2(384)  # Assuming 384 dimensions for embeddings
        mapping = {}
    return index, mapping

def update_faiss_index(index, mapping, story_id, centroid, position=None):
    """Add or update a story in the FAISS index."""
    centroid_array = np.array([centroid], dtype="float32")
    if position is None:
        position = index.ntotal
        index.add(centroid_array)
    else:
        index.remove_ids(np.array([position]))
        index.add_with_ids(centroid_array, np.array([position]))
    mapping[str(position)] = story_id
    logger.info(f"Updated FAISS index for story {story_id} at position {position}")

def save_faiss_index(index, mapping):
    """Save the FAISS index and mapping to disk."""
    faiss.write_index(index, FAISS_INDEX_FILE)
    with open(FAISS_MAPPING_FILE, "w") as f:
        json.dump(mapping, f)
    logger.info(f"Saved FAISS index with {index.ntotal} vectors")

def update_stories(db, new_clusters, story_mapping, used_story_ids):
    """Update story documents and FAISS index, deleting unused stories."""
    index, mapping = load_faiss_index()

    # Track positions to remove from FAISS
    positions_to_remove = []

    for label, article_ids in new_clusters.items():
        story_id = story_mapping[label]
        
        # Fetch articles to get their URLs and other fields
        articles = list(db[MONGO_COLLECTION_ARTICLES].find(
            {"_id": {"$in": article_ids}},
            {"link": 1,
              "embedding": 1,
                # "keywords": 1,
                  "entities": 1}
        ))
        if not articles:
            continue

        # Use article URLs instead of ObjectIds
        article_links = [article["link"] for article in articles]

        # Recalculate centroid from embeddings
        embeddings = [np.array(article["embedding"]) for article in articles if "embedding" in article]
        centroid = np.mean(embeddings, axis=0).tolist() if embeddings else []

        # Aggregate keywords and entities
        # keywords = set()
        entities = set()
        for article in articles:
            # keywords.update(article.get("keywords", []))
            entities.update(article.get("entities", []))
        # keywords = list(keywords)
        entities = list(entities)

        # Prepare update fields
        existing_story = db[MONGO_COLLECTION_STORIES].find_one({"_id": story_id})
        update_fields = {
            "articles": article_links,
            "centroid": centroid,
            # "keywords": keywords,
            "entities": entities,
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        if not existing_story:
            update_fields["created_at"] = datetime.now(timezone.utc).isoformat()

        # Update the story document
        db[MONGO_COLLECTION_STORIES].update_one(
            {"_id": story_id},
            {"$set": update_fields},
            upsert=True
        )

        # Update FAISS index
        if str(story_id) in mapping.values():
            for pos, mapped_id in list(mapping.items()):
                if mapped_id == story_id:
                    positions_to_remove.append(int(pos))
                    del mapping[pos]
                    break
        update_faiss_index(index, mapping, story_id, centroid)

        # Update articles with story_id
        db[MONGO_COLLECTION_ARTICLES].update_many(
            {"_id": {"$in": article_ids}},
            {"$set": {"story_id": story_id}}
        )

    # Delete unused stories from MongoDB and FAISS
    all_existing_stories = set(existing_stories.keys())
    unused_stories = all_existing_stories - used_story_ids
    for story_id in unused_stories:
        db[MONGO_COLLECTION_STORIES].delete_one({"_id": story_id})
        logger.info(f"Deleted unused story {story_id} from MongoDB")
        for pos, mapped_id in list(mapping.items()):
            if mapped_id == story_id:
                positions_to_remove.append(int(pos))
                del mapping[pos]
                break

    # Remove positions from FAISS index
    if positions_to_remove:
        positions_array = np.array(positions_to_remove, dtype=np.int64)
        index.remove_ids(positions_array)
        logger.info(f"Removed {len(positions_to_remove)} stories from FAISS index")

    save_faiss_index(index, mapping)

def main():
    """Main function to run the batch cluster refinement and FAISS index update."""
    try:
        mongo_client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        db = mongo_client[MONGO_DB]
        logger.info("Connected to MongoDB")
    except pymongo.errors.ConnectionFailure as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return

    global existing_stories  # Define as global to access in update_stories
    existing_stories = {
        str(story["_id"]): set(story["articles"])
        for story in db[MONGO_COLLECTION_STORIES].find({}, {"_id": 1, "articles": 1})
    }
    articles = fetch_articles(db)
    if not articles:
        logger.info("No articles to cluster.")
        return

    from sklearn.preprocessing import normalize

    embeddings = np.array([article["embedding"] for article in articles])
    normalized_embeddings = normalize(embeddings, norm='l2')
    reduced_embeddings = reduce_embeddings(normalized_embeddings)
    labels = cluster_articles(reduced_embeddings)

    new_clusters = {}
    for article, label in zip(articles, labels):
        if label == -1:  # Skip noise points
            continue
        if label not in new_clusters:
            new_clusters[label] = []
        new_clusters[label].append(article["_id"])

    story_mapping, used_story_ids = map_clusters_to_stories(new_clusters, existing_stories, threshold=0.5)
    update_stories(db, new_clusters, story_mapping, used_story_ids)
    logger.info("Cluster refinement and FAISS index update completed.")

if __name__ == "__main__":
    main()