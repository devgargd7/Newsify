import json
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional

import httpx
import jwt
import redis.asyncio as aioredis
import strawberry
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.security import OAuth2AuthorizationBearer
from jwt import PyJWTError
from strawberry.asgi import GraphQL

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("APIGateway")

# -------------------------------
# Load External Configuration
# -------------------------------
CONFIG_FILE = "config.json"
if not os.path.exists(CONFIG_FILE):
    logger.error("Configuration file %s not found", CONFIG_FILE)
    exit(1)

with open(CONFIG_FILE, "r") as f:
    config = json.load(f)
logger.info("Loaded configuration from %s", CONFIG_FILE)

# -------------------------------
# JWT Authentication Dependency
# -------------------------------
oauth2_scheme = OAuth2AuthorizationBearer(auto_error=False)

def get_current_user(token: Optional[str] = Depends(oauth2_scheme)) -> Dict[str, Any]:
    jwt_secret = config.get("jwt_secret")
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
        )
    try:
        payload = jwt.decode(token, jwt_secret, algorithms=["HS256"])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication token",
            )
        return {"user_id": user_id, "scopes": payload.get("scopes", [])}
    except PyJWTError as e:
        logger.error("JWT decoding error: %s", e)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate token",
        )

# -------------------------------
# Setup Redis Client for Caching & Rate Limiting
# -------------------------------
redis_config = config.get("redis", {})
redis_client = aioredis.from_url(
    f"redis://{redis_config.get('host', 'localhost')}:{redis_config.get('port', 6379)}",
    password=redis_config.get("password"),
    decode_responses=True,
)
logger.info("Initialized Redis client.")

# -------------------------------
# Utility: Select a Service URL Randomly (simulate load balancing)
# -------------------------------
def select_service_url(service_key: str) -> str:
    urls = config.get(service_key, [])
    if not urls:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service {service_key} is unavailable",
        )
    return random.choice(urls)

# -------------------------------
# Initialize FastAPI Application
# -------------------------------
app = FastAPI(title="API Gateway")

# -------------------------------
# REST Endpoints
# -------------------------------

@app.get("/api/news")
async def get_news(current_user: Dict[str, Any] = Depends(get_current_user)):
    news_service_url = select_service_url("news_service_urls")
    url = f"{news_service_url}/news"  # Assume underlying service exposes /news
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        # Optionally cache the response
        return response.json()
    except Exception as e:
        logger.error("Error fetching news: %s", e)
        raise HTTPException(status_code=500, detail="Error fetching news")

@app.get("/api/recommendations")
async def get_recommendations(
    user_id: Optional[str] = None,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    # Use the user from token if not provided
    user_id = user_id or current_user.get("user_id")
    recommendation_service_url = select_service_url("recommendation_service_urls")
    url = f"{recommendation_service_url}/recommendations?user_id={user_id}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error("Error fetching recommendations: %s", e)
        raise HTTPException(status_code=500, detail="Error fetching recommendations")

@app.get("/api/user")
async def get_user_data(current_user: Dict[str, Any] = Depends(get_current_user)):
    user_service_url = select_service_url("user_service_urls")
    url = f"{user_service_url}/user?user_id={current_user.get('user_id')}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error("Error fetching user data: %s", e)
        raise HTTPException(status_code=500, detail="Error fetching user data")

# -------------------------------
# GraphQL Schema and Endpoint using Strawberry
# -------------------------------
@strawberry.type
class News:
    id: str
    title: str
    summary: str

@strawberry.type
class Recommendation:
    article_id: str
    title: str
    summary: str

@strawberry.type
class UserData:
    user_id: str
    name: str
    email: str

@strawberry.type
class Query:
    @strawberry.field
    async def news(self) -> List[News]:
        # Reuse the /api/news endpoint logic
        news_service_url = select_service_url("news_service_urls")
        url = f"{news_service_url}/news"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        news_items = response.json()
        return [
            News(id=item.get("id"), title=item.get("title"), summary=item.get("summary"))
            for item in news_items
        ]

    @strawberry.field
    async def recommendations(self, user_id: str) -> List[Recommendation]:
        recommendation_service_url = select_service_url("recommendation_service_urls")
        url = f"{recommendation_service_url}/recommendations?user_id={user_id}"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        rec_items = response.json()
        return [
            Recommendation(
                article_id=item.get("article_id"),
                title=item.get("title"),
                summary=item.get("summary", ""),
            )
            for item in rec_items
        ]

    @strawberry.field
    async def user(self, user_id: str) -> UserData:
        user_service_url = select_service_url("user_service_urls")
        url = f"{user_service_url}/user?user_id={user_id}"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
        response.raise_for_status()
        user_item = response.json()
        return UserData(
            user_id=user_item.get("user_id"),
            name=user_item.get("name"),
            email=user_item.get("email"),
        )

schema = strawberry.Schema(query=Query)
graphql_app = GraphQL(schema)

# Mount the GraphQL endpoint at /graphql
app.mount("/graphql", graphql_app)

# -------------------------------
# Middleware: Rate Limiting (Placeholder Implementation)
# -------------------------------
# In production, you might use packages such as slowapi or fastapi-limiter.
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Example: Limit 100 requests per minute per client IP (placeholder)
    client_ip = request.client.host
    key = f"rate_limit:{client_ip}"
    try:
        count = await redis_client.incr(key)
        if count == 1:
            # Set expiry of 60 seconds on first increment
            await redis_client.expire(key, 60)
        if count > 100:
            raise HTTPException(
                status_code=429, detail="Too Many Requests. Please try again later."
            )
    except Exception as e:
        logger.error("Rate limiting error: %s", e)
    response = await call_next(request)
    return response

# -------------------------------
# Main Entry Point
# -------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
