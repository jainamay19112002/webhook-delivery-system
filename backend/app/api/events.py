import json
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Header, status
from sqlmodel import Session, select
from app.database import get_session
from app.models import Event, EventCreate, Subscriber, DeliveryAttempt
from app.queue.redis_queue import queue_client

router = APIRouter(prefix="/events", tags=["Events"])

@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(
    payload: EventCreate,
    x_idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
    session: Session = Depends(get_session)
):
    """
    Ingests a new webhook event.
    Idempotency: If X-Idempotency-Key or payload.idempotency_key is provided and exists,
    returns existing event to avoid duplicate event generation.
    """
    idem_key = payload.idempotency_key or x_idempotency_key

    if idem_key:
        existing = session.get(Event, idem_key)
        if existing:
            return {
                "message": "Event already ingested (idempotent request)",
                "event_id": existing.id,
                "status": existing.status,
                "is_duplicate": True
            }

    # Find matching subscribers for event_type
    all_subscribers = session.exec(select(Subscriber).where(Subscriber.is_active == True)).all()
    matching_subscribers = [
        sub for sub in all_subscribers
        if sub.event_types == "*" or payload.event_type in sub.event_types.split(",")
    ]

    event_id = idem_key if idem_key else None
    event_obj = Event(
        id=event_id if event_id else undefined, # SQLModel will auto-gen if None
        event_type=payload.event_type,
        payload_json=json.dumps(payload.payload),
        status="PENDING"
    ) if event_id else Event(
        event_type=payload.event_type,
        payload_json=json.dumps(payload.payload),
        status="PENDING"
    )

    session.add(event_obj)
    session.commit()
    session.refresh(event_obj)

    subscriber_ids = [sub.id for sub in matching_subscribers]

    # Publish event to Redis Stream Queue
    await queue_client.publish_event(
        event_id=event_obj.id,
        event_type=event_obj.event_type,
        subscriber_ids=subscriber_ids,
        payload=payload.payload
    )

    return {
        "message": "Event accepted for delivery",
        "event_id": event_obj.id,
        "matched_subscribers_count": len(matching_subscribers),
        "status": "PENDING"
    }

@router.get("")
def list_events(limit: int = 50, session: Session = Depends(get_session)):
    events = session.exec(
        select(Event).order_by(Event.created_at.desc()).limit(limit)
    ).all()
    
    results = []
    for evt in events:
        attempts = session.exec(
            select(DeliveryAttempt).where(DeliveryAttempt.event_id == evt.id)
        ).all()
        results.append({
            "id": evt.id,
            "event_type": evt.event_type,
            "payload": evt.payload,
            "status": evt.status,
            "created_at": evt.created_at,
            "updated_at": evt.updated_at,
            "delivery_attempts_count": len(attempts),
            "attempts": attempts
        })

    return results

@router.get("/{event_id}")
def get_event(event_id: str, session: Session = Depends(get_session)):
    event = session.get(Event, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    
    attempts = session.exec(
        select(DeliveryAttempt).where(DeliveryAttempt.event_id == event.id)
    ).all()

    return {
        "id": event.id,
        "event_type": event.event_type,
        "payload": event.payload,
        "status": event.status,
        "created_at": event.created_at,
        "updated_at": event.updated_at,
        "attempts": attempts
    }

@router.post("/dlq/replay/{event_id}")
async def replay_dlq_event(event_id: str, session: Session = Depends(get_session)):
    event = session.get(Event, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    
    all_subscribers = session.exec(select(Subscriber).where(Subscriber.is_active == True)).all()
    matching_subscribers = [
        sub for sub in all_subscribers
        if sub.event_types == "*" or event.event_type in sub.event_types.split(",")
    ]
    
    event.status = "PENDING"
    event.updated_at = datetime.utcnow()
    session.add(event)
    session.commit()

    subscriber_ids = [sub.id for sub in matching_subscribers]
    await queue_client.publish_event(
        event_id=event.id,
        event_type=event.event_type,
        subscriber_ids=subscriber_ids,
        payload=event.payload
    )

    return {"message": "Event requeued for redelivery from DLQ", "event_id": event.id}
