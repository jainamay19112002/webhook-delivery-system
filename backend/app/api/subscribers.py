import secrets
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session, select
from app.database import get_session
from app.models import Subscriber, SubscriberCreate, SubscriberRead

router = APIRouter(prefix="/subscribers", tags=["Subscribers"])

@router.post("", response_model=SubscriberRead, status_code=status.HTTP_201_CREATED)
def create_subscriber(payload: SubscriberCreate, session: Session = Depends(get_session)):
    secret = f"whsec_{secrets.token_hex(24)}"
    event_types_str = ",".join(payload.event_types) if payload.event_types else "*"
    
    subscriber = Subscriber(
        name=payload.name,
        target_url=payload.target_url,
        secret=secret,
        event_types=event_types_str
    )
    session.add(subscriber)
    session.commit()
    session.refresh(subscriber)
    return subscriber

@router.get("", response_model=List[SubscriberRead])
def list_subscribers(session: Session = Depends(get_session)):
    subscribers = session.exec(select(Subscriber)).all()
    return subscribers

@router.get("/{subscriber_id}", response_model=SubscriberRead)
def get_subscriber(subscriber_id: str, session: Session = Depends(get_session)):
    subscriber = session.get(Subscriber, subscriber_id)
    if not subscriber:
        raise HTTPException(status_code=404, detail="Subscriber not found")
    return subscriber

@router.post("/{subscriber_id}/simulate-behavior")
def update_subscriber_simulation(
    subscriber_id: str,
    simulate_failure: bool,
    simulate_delay_sec: float = 0.0,
    simulate_status_code: int = 500,
    session: Session = Depends(get_session)
):
    subscriber = session.get(Subscriber, subscriber_id)
    if not subscriber:
        raise HTTPException(status_code=404, detail="Subscriber not found")
    
    subscriber.simulate_failure = simulate_failure
    subscriber.simulate_delay_sec = simulate_delay_sec
    subscriber.simulate_status_code = simulate_status_code
    session.add(subscriber)
    session.commit()
    session.refresh(subscriber)
    return {"message": "Simulation behavior updated", "subscriber": subscriber}
