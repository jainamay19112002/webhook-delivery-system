import os
from pydantic import BaseModel

class Settings(BaseModel):
    PROJECT_NAME: str = "Reliable Webhook Delivery Engine"
    API_V1_STR: str = "/api/v1"
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./webhook_system.db")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    USE_FAKEREDIS_IF_OFFLINE: bool = True
    
    # Reliability Settings
    MAX_DELIVERY_RETRIES: int = 5
    INITIAL_BACKOFF_SECONDS: float = 1.0
    BACKOFF_MULTIPLIER: float = 2.0
    MAX_BACKOFF_SECONDS: float = 60.0
    
    # Circuit Breaker Settings
    CIRCUIT_FAILURE_THRESHOLD: int = 5
    CIRCUIT_RECOVERY_TIME_SECONDS: float = 30.0
    
    # Worker Settings
    WORKER_CONCURRENCY: int = 10
    STREAM_KEY: str = "webhook:events"
    CONSUMER_GROUP: str = "delivery_workers"
    DLQ_STREAM_KEY: str = "webhook:dlq"

settings = Settings()
