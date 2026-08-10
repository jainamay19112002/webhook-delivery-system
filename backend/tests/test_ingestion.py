import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session
from app.main import app
from app.database import engine, init_db
from app.models import Subscriber, Event

@pytest.fixture(name="client")
def client_fixture():
    init_db()
    with TestClient(app) as client:
        yield client

def test_subscriber_creation(client: TestClient):
    response = client.post(
        "/api/v1/subscribers",
        json={
            "name": "Acme Payment Analytics",
            "target_url": "http://localhost:8000/api/v1/mock/webhook-receiver",
            "event_types": ["order.created", "payment.succeeded"]
        }
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "Acme Payment Analytics"
    assert data["secret"].startswith("whsec_")
    assert "id" in data

def test_event_ingestion_and_idempotency(client: TestClient):
    idem_key = "test-idem-key-uuid-9999"

    # First Ingestion
    res1 = client.post(
        "/api/v1/events",
        json={
            "event_type": "payment.succeeded",
            "payload": {"amount": 4999, "currency": "USD"},
            "idempotency_key": idem_key
        }
    )
    assert res1.status_code == 202
    data1 = res1.json()
    assert data1["event_id"] == idem_key
    assert data1.get("is_duplicate") is None or data1.get("is_duplicate") is False

    # Second Ingestion with duplicate idempotency key -> returns existing without creating duplicate
    res2 = client.post(
        "/api/v1/events",
        json={
            "event_type": "payment.succeeded",
            "payload": {"amount": 4999, "currency": "USD"},
            "idempotency_key": idem_key
        }
    )
    assert res2.status_code == 202
    data2 = res2.json()
    assert data2["event_id"] == idem_key
    assert data2["is_duplicate"] is True
