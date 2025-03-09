import json
import logging
import os
import time
from datetime import datetime, timezone

import feedparser
import redis
import yaml
from confluent_kafka import Producer
from newspaper import Article

# Load configuration from environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "articles")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
DEDUPLICATION_EXPIRY = int(os.getenv("DEDUPLICATION_EXPIRY", 86400))
RSS_FEEDS = json.loads(os.getenv("RSS_FEEDS", '{"source1": "http://example.com/rss"}'))
INGESTION_INTERVAL = int(os.getenv("INGESTION_INTERVAL", 300))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_rss_feed(url):
    """Fetch and parse an RSS feed from the given URL."""
    try:
        feed = feedparser.parse(url)
        if feed.bozo:
            logger.error(f"Error parsing feed {url}: {feed.bozo_exception}")
            return []
        return feed.entries
    except Exception as e:
        logger.error(f"Error fetching feed {url}: {e}")
        return []

def scrape_article_text(url):
    """Scrape the full text of an article from its URL."""
    try:
        article = Article(url)
        article.download()
        article.parse()
        return article.text
    except Exception as e:
        logger.error(f"Error scraping article from {url}: {e}")
        return ""

def extract_article_data(entry, source):
    """Extract relevant fields from an RSS entry and scrape full text."""
    link = entry.get('link')
    if not link:
        logger.warning(f"Article without link from source {source}")
        return None
    guid = entry.get('id', link)
    title = entry.get('title', '')
    description = entry.get('description', '')
    pub_date = entry.get('published_parsed', entry.get('updated_parsed'))
    if pub_date:
        pub_date = datetime(*pub_date[:6]).isoformat()
    text = scrape_article_text(link) 
    return {
        'guid': guid,
        'title': title,
        'description': description,
        'pub_date': pub_date,
        'link': link,
        'source': source,
        'summary': entry.get('summary', ''),
        'text': text,  
        'ingestion_time': datetime.now(timezone.utc).isoformat()
    }

def is_duplicate(redis_conn, link):
    """Check if an article is a duplicate using Redis."""
    key = f"article:{link}"
    return not redis_conn.set(key, 1, ex=DEDUPLICATION_EXPIRY, nx=True)

def delivery_report(err, msg):
    """Callback for Kafka message delivery."""
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def publish_to_kafka(producer, topic, article):
    """Publish an article to the Kafka topic."""
    try:
        key = article['link'].encode('utf-8')
        value = json.dumps(article).encode('utf-8')
        producer.produce(topic, key=key, value=value, callback=delivery_report)
        producer.poll(0)
    except Exception as e:
        logger.error(f"Error publishing article {article['link']} to Kafka: {e}")

def main():
    """Main function to run the ingestion loop."""
    redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    logger.info("Kafka producer initialized")

    while True:
        start_time = time.time()
        logger.info("Starting ingestion cycle")
        new_articles = 0
        for source, rss_url in RSS_FEEDS.items():
            entries = fetch_rss_feed(rss_url)
            for entry in entries:
                article = extract_article_data(entry, source)
                if article and article['text'] and not is_duplicate(redis_conn, article['link']):
                    publish_to_kafka(producer, KAFKA_TOPIC, article)
                    new_articles += 1
                else:
                    logger.info(f"Duplicate article {article['link']} from {source}" if article else "Invalid article skipped")
        producer.flush()
        logger.info(f"Ingestion cycle completed. Published {new_articles} new articles")
        elapsed = time.time() - start_time
        if elapsed < INGESTION_INTERVAL:
            time.sleep(INGESTION_INTERVAL - elapsed)

if __name__ == "__main__":
    main()