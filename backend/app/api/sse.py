import asyncio
import json
from fastapi import APIRouter, Depends
from sse_starlette.sse import EventSourceResponse
from sqlmodel import Session, select, func
from app.database import engine
from app.models import Event, DeliveryAttempt, Subscriber, CircuitBreakerStatus

router = APIRouter(prefix="/events", tags=["SSE"])

async def event_generator():
    """
    Pushes live metric snapshots & delivery logs to SSE client every 1 second.
    """
    while True:
        try:
            with Session(engine) as session:
                total_events = session.exec(select(func.count(Event.id))).one()
                total_delivered = session.exec(select(func.count(Event.id)).where(Event.status == "DELIVERED")).one()
                total_retrying = session.exec(select(func.count(Event.id)).where(Event.status == "RETRYING")).one()
                total_dlq = session.exec(select(func.count(Event.id)).where(Event.status == "DEAD_LETTERED")).one()

                total_attempts = session.exec(select(func.count(DeliveryAttempt.id))).one()
                successful_attempts = session.exec(
                    select(func.count(DeliveryAttempt.id)).where(DeliveryAttempt.status == "SUCCESS")
                ).one()

                avg_latency = session.exec(select(func.avg(DeliveryAttempt.execution_time_ms))).one() or 0.0

                success_rate = round((successful_attempts / total_attempts * 100), 1) if total_attempts > 0 else 100.0

                recent_attempts = session.exec(
                    select(DeliveryAttempt).order_by(DeliveryAttempt.attempted_at.desc()).limit(15)
                ).all()

                circuit_breakers = session.exec(select(CircuitBreakerStatus)).all()

                subscribers = session.exec(select(Subscriber)).all()

                payload = {
                    "metrics": {
                        "total_events": total_events,
                        "total_delivered": total_delivered,
                        "total_retrying": total_retrying,
                        "total_dlq": total_dlq,
                        "success_rate": success_rate,
                        "avg_latency_ms": round(avg_latency, 1),
                        "total_subscribers": len(subscribers)
                    },
                    "circuit_breakers": [
                        {
                            "subscriber_id": cb.subscriber_id,
                            "state": cb.state,
                            "consecutive_failures": cb.consecutive_failures
                        }
                        for cb in circuit_breakers
                    ],
                    "recent_attempts": [
                        {
                            "id": att.id,
                            "event_id": att.event_id,
                            "subscriber_id": att.subscriber_id,
                            "attempt_number": att.attempt_number,
                            "status": att.status,
                            "response_status_code": att.response_status_code,
                            "execution_time_ms": att.execution_time_ms,
                            "error_message": att.error_message,
                            "attempted_at": att.attempted_at.isoformat()
                        }
                        for att in recent_attempts
                    ]
                }

                yield {
                    "event": "update",
                    "data": json.dumps(payload)
                }

        except Exception as e:
            yield {
                "event": "error",
                "data": json.dumps({"error": str(e)})
            }

        await asyncio.sleep(1.0)

@router.get("/stream")
async def sse_stream():
    return EventSourceResponse(event_generator())
