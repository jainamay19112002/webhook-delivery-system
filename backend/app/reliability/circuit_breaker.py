from datetime import datetime, timedelta
from typing import Dict
from sqlmodel import Session, select
from app.models import CircuitBreakerStatus
from app.config import settings

class CircuitBreakerManager:
    """
    Per-subscriber Circuit Breaker pattern state machine.
    States: CLOSED -> OPEN -> HALF_OPEN -> CLOSED (or back to OPEN)
    """
    def __init__(
        self,
        failure_threshold: int = settings.CIRCUIT_FAILURE_THRESHOLD,
        recovery_time_sec: float = settings.CIRCUIT_RECOVERY_TIME_SECONDS
    ):
        self.failure_threshold = failure_threshold
        self.recovery_time_sec = recovery_time_sec

    def get_state(self, subscriber_id: str, session: Session) -> str:
        cb = session.get(CircuitBreakerStatus, subscriber_id)
        if not cb:
            cb = CircuitBreakerStatus(subscriber_id=subscriber_id, state="CLOSED")
            session.add(cb)
            session.commit()
            session.refresh(cb)
            return "CLOSED"

        now = datetime.utcnow()
        if cb.state == "OPEN":
            # Check if recovery cooldown period has elapsed
            if cb.last_failure_at and (now - cb.last_failure_at) > timedelta(seconds=self.recovery_time_sec):
                cb.state = "HALF_OPEN"
                cb.last_state_change = now
                session.add(cb)
                session.commit()
                return "HALF_OPEN"

        return cb.state

    def record_success(self, subscriber_id: str, session: Session):
        cb = session.get(CircuitBreakerStatus, subscriber_id)
        if not cb:
            cb = CircuitBreakerStatus(subscriber_id=subscriber_id)
        
        cb.state = "CLOSED"
        cb.consecutive_failures = 0
        cb.last_state_change = datetime.utcnow()
        session.add(cb)
        session.commit()

    def record_failure(self, subscriber_id: str, session: Session) -> str:
        cb = session.get(CircuitBreakerStatus, subscriber_id)
        if not cb:
            cb = CircuitBreakerStatus(subscriber_id=subscriber_id)
        
        cb.consecutive_failures += 1
        cb.last_failure_at = datetime.utcnow()

        if cb.state == "CLOSED" and cb.consecutive_failures >= self.failure_threshold:
            cb.state = "OPEN"
            cb.last_state_change = datetime.utcnow()
        elif cb.state == "HALF_OPEN":
            cb.state = "OPEN"
            cb.last_state_change = datetime.utcnow()

        session.add(cb)
        session.commit()
        return cb.state
