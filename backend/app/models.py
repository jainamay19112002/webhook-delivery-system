import uuid
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from sqlmodel import SQLModel, Field, Column, JSON

def generate_uuid() -> str:
    return str(uuid.uuid4())

class Subscriber(SQLModel, table=True):
    __tablename__ = "subscribers"
    
    id: str = Field(default_factory=generate_uuid, primary_key=True)
    name: str = Field(index=True)
    target_url: str
    secret: str  # Used for HMAC-SHA256 payload signatures
    event_types: str = Field(default="*")  # Comma-separated or "*" for all
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Optional endpoint behavior override for testing circuit breakers / failures
    simulate_failure: bool = Field(default=False)
    simulate_delay_sec: float = Field(default=0.0)
    simulate_status_code: int = Field(default=500)

class SubscriberCreate(SQLModel):
    name: str
    target_url: str
    event_types: Optional[List[str]] = ["*"]

class SubscriberRead(SQLModel):
    id: str
    name: str
    target_url: str
    secret: str
    event_types: str
    is_active: bool
    created_at: datetime
    simulate_failure: bool
    simulate_delay_sec: float
    simulate_status_code: int

class Event(SQLModel, table=True):
    __tablename__ = "events"
    
    id: str = Field(default_factory=generate_uuid, primary_key=True)  # Serves as Idempotency Key
    event_type: str = Field(index=True)
    payload_json: str = Field(default="{}")
    status: str = Field(default="PENDING", index=True)  # PENDING, DELIVERED, FAILED, RETRYING, DEAD_LETTERED
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def payload(self) -> Dict[str, Any]:
        try:
            return json.loads(self.payload_json)
        except Exception:
            return {}

class EventCreate(SQLModel):
    event_type: str
    payload: Dict[str, Any]
    idempotency_key: Optional[str] = None

class DeliveryAttempt(SQLModel, table=True):
    __tablename__ = "delivery_attempts"
    
    id: str = Field(default_factory=generate_uuid, primary_key=True)
    event_id: str = Field(index=True, foreign_key="events.id")
    subscriber_id: str = Field(index=True, foreign_key="subscribers.id")
    attempt_number: int
    response_status_code: Optional[int] = None
    response_body: Optional[str] = None
    execution_time_ms: float = 0.0
    status: str = Field(index=True)  # SUCCESS, FAILED, CIRCUIT_OPEN
    error_message: Optional[str] = None
    signature_used: Optional[str] = None
    attempted_at: datetime = Field(default_factory=datetime.utcnow)

class DeliveryAttemptRead(SQLModel):
    id: str
    event_id: str
    subscriber_id: str
    attempt_number: int
    response_status_code: Optional[int]
    response_body: Optional[str]
    execution_time_ms: float
    status: str
    error_message: Optional[str]
    attempted_at: datetime

class CircuitBreakerStatus(SQLModel, table=True):
    __tablename__ = "circuit_breaker_status"
    
    subscriber_id: str = Field(primary_key=True)
    state: str = Field(default="CLOSED")  # CLOSED, OPEN, HALF_OPEN
    consecutive_failures: int = Field(default=0)
    last_failure_at: Optional[datetime] = None
    last_state_change: datetime = Field(default_factory=datetime.utcnow)
