import pytest
import time
from app.reliability.hmac_signer import HMACSigner
from app.reliability.backoff import ExponentialBackoffWithJitter
from app.reliability.circuit_breaker import CircuitBreakerManager
from app.database import engine, init_db
from sqlmodel import Session

def test_hmac_signer():
    secret = "whsec_test_secret_12345"
    payload = b'{"event": "payment.succeeded", "amount": 9900}'
    
    header_val, sig = HMACSigner.generate_signature(secret, payload)
    assert header_val.startswith("t=")
    assert "v1=" in header_val
    
    # Valid verification
    is_valid = HMACSigner.verify_signature(secret, payload, header_val)
    assert is_valid is True

    # Tampered payload verification
    tampered_payload = b'{"event": "payment.succeeded", "amount": 100}'
    is_invalid = HMACSigner.verify_signature(secret, tampered_payload, header_val)
    assert is_invalid is False

def test_exponential_backoff():
    backoff = ExponentialBackoffWithJitter(initial_backoff=1.0, multiplier=2.0, max_backoff=10.0)
    
    assert backoff.calculate_delay(1) == 0.0
    
    # Attempt 2: max 1.0s (full jitter means 0 <= delay <= 1.0)
    delay2 = backoff.calculate_delay(2)
    assert 0.0 <= delay2 <= 1.0

    # Attempt 3: max 2.0s
    delay3 = backoff.calculate_delay(3)
    assert 0.0 <= delay3 <= 2.0

def test_circuit_breaker():
    init_db()
    cb_manager = CircuitBreakerManager(failure_threshold=3, recovery_time_sec=1.0)
    sub_id = "test_subscriber_cb_1"

    with Session(engine) as session:
        # Initial state should be CLOSED
        assert cb_manager.get_state(sub_id, session) == "CLOSED"

        # Record 2 failures -> should still be CLOSED
        cb_manager.record_failure(sub_id, session)
        cb_manager.record_failure(sub_id, session)
        assert cb_manager.get_state(sub_id, session) == "CLOSED"

        # Record 3rd failure -> should flip to OPEN
        cb_manager.record_failure(sub_id, session)
        assert cb_manager.get_state(sub_id, session) == "OPEN"

    # Wait 1.1s for recovery cooldown
    time.sleep(1.1)

    with Session(engine) as session:
        # Cooldown passed -> state should transition to HALF_OPEN
        assert cb_manager.get_state(sub_id, session) == "HALF_OPEN"

        # Success in HALF_OPEN resets to CLOSED
        cb_manager.record_success(sub_id, session)
        assert cb_manager.get_state(sub_id, session) == "CLOSED"
