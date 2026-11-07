"""Shared connection settings for all portfolio scripts (local Docker lab only)."""
import os

PG_DSN = os.getenv(
    "NF_PG_DSN",
    "host=localhost port=5433 dbname=nairaflow user=nairaflow password=nairaflow_dev",
)
MONGO_URI = os.getenv("NF_MONGO_URI", "mongodb://localhost:27018/?directConnection=true")
REDIS_URL = os.getenv("NF_REDIS_URL", "redis://localhost:6380/0")
