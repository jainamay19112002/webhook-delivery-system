import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, List
import httpx
from sqlmodel import Session, select

from app.config import settings
from app.database import engine
from app.models import Event, Subscriber, DeliveryAttempt
from app.reliability.hmac_signer import HMACSigner
from app.reliability.backoff import ExponentialBackoffWithJitter
from app.reliability.circuit_breaker import CircuitBreakerManager
from app.queue.redis_queue import queue_client

logger = logging.getLogger("delivery_worker")

class WebhookDeliveryEngine:
    def __init__(self):
        self.backoff_calc = ExponentialBackoffWithJitter()
        self.circuit_breaker = CircuitBreakerManager()
        self.is_running = False

    async def deliver_to_subscriber(
        self,
        event: Event,
        subscriber: Subscriber,
        http_client: httpx.AsyncClient
    ) -> bool:
        """
        Delivers single webhook event to target subscriber with retries, HMAC, circuit breaker checks.
        """
        payload_bytes = event.payload_json.encode("utf-8")
        
        # 1. Circuit Breaker Check
        with Session(engine) as session:
            cb_state = self.circuit_breaker.get_state(subscriber.id, session)
            
        if cb_state == "OPEN":
            logger.warning(f"Circuit OPEN for subscriber {subscriber.name} ({subscriber.id}). Skipping delivery.")
            with Session(engine) as session:
                attempt = DeliveryAttempt(
                    event_id=event.id,
                    subscriber_id=subscriber.id,
                    attempt_number=1,
                    status="CIRCUIT_OPEN",
                    error_message="Circuit breaker is OPEN (subscriber endpoint unhealthy)"
                )
                session.add(attempt)
                session.commit()
            return False

        # 2. Prepare HMAC signature & headers
        header_sig, raw_sig = HMACSigner.generate_signature(subscriber.secret, payload_bytes)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "WebhookDeliverySystem/1.0",
            "X-Webhook-Idem-Key": event.id,
            "X-Webhook-Event": event.event_type,
            "X-Webhook-Signature": header_sig,
        }

        # 3. Retry loop with Exponential Backoff + Jitter
        max_attempts = settings.MAX_DELIVERY_RETRIES
        delivered_successfully = False

        for attempt_num in range(1, max_attempts + 1):
            if attempt_num > 1:
                delay = self.backoff_calc.calculate_delay(attempt_num)
                logger.info(f"Retrying event {event.id} to {subscriber.name} (Attempt {attempt_num}/{max_attempts}) after {delay:.2f}s backoff...")
                await asyncio.sleep(delay)

            start_time = time.time()
            response_code = None
            response_body = None
            error_msg = None
            success = False

            # Check if endpoint is mock internal test or external URL
            target_url = subscriber.target_url

            try:
                # Handle test mock failure simulations
                if subscriber.simulate_delay_sec > 0:
                    await asyncio.sleep(subscriber.simulate_delay_sec)
                
                if subscriber.simulate_failure:
                    response_code = subscriber.simulate_status_code or 500
                    response_body = json.dumps({"error": "Simulated endpoint failure"})
                    error_msg = f"HTTP {response_code} Simulated failure"
                else:
                    response = await http_client.post(
                        target_url,
                        content=payload_bytes,
                        headers=headers,
                        timeout=5.0
                    )
                    response_code = response.status_code
                    response_body = response.text[:1000]  # Cap snippet size
                    
                    if 200 <= response_code < 300:
                        success = True
                    else:
                        error_msg = f"HTTP {response_code}"

            except httpx.RequestError as exc:
                error_msg = f"Request failure: {str(exc)}"
            except Exception as exc:
                error_msg = f"Unexpected error: {str(exc)}"

            execution_time_ms = round((time.time() - start_time) * 1000, 2)

            # Log attempt to DB
            with Session(engine) as session:
                delivery_record = DeliveryAttempt(
                    event_id=event.id,
                    subscriber_id=subscriber.id,
                    attempt_number=attempt_num,
                    response_status_code=response_code,
                    response_body=response_body,
                    execution_time_ms=execution_time_ms,
                    status="SUCCESS" if success else "FAILED",
                    error_message=error_msg,
                    signature_used=raw_sig
                )
                session.add(delivery_record)
                
                if success:
                    self.circuit_breaker.record_success(subscriber.id, session)
                    delivered_successfully = True
                    break
                else:
                    self.circuit_breaker.record_failure(subscriber.id, session)

        return delivered_successfully

    async def process_single_stream_entry(self, entry_id: str, entry_data: Dict[str, Any], http_client: httpx.AsyncClient):
        event_id = entry_data.get("event_id")
        subscriber_ids = json.loads(entry_data.get("subscriber_ids", "[]"))

        with Session(engine) as session:
            db_event = session.get(Event, event_id)
            if not db_event:
                await queue_client.acknowledge_event(entry_id)
                return

            subscribers = session.exec(
                select(Subscriber).where(Subscriber.id.in_(subscriber_ids), Subscriber.is_active == True)
            ).all()

        if not subscribers:
            db_event.status = "DELIVERED"
            db_event.updated_at = datetime.utcnow()
            with Session(engine) as session:
                session.add(db_event)
                session.commit()
            await queue_client.acknowledge_event(entry_id)
            return

        db_event.status = "RETRYING"
        with Session(engine) as session:
            session.add(db_event)
            session.commit()

        # Concurrent delivery worker pool for subscribers
        tasks = [
            self.deliver_to_subscriber(db_event, sub, http_client)
            for sub in subscribers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_success = all(res is True for res in results if not isinstance(res, Exception))

        with Session(engine) as session:
            evt = session.get(Event, event_id)
            if evt:
                if all_success:
                    evt.status = "DELIVERED"
                else:
                    evt.status = "DEAD_LETTERED"
                    # Push to DLQ
                    asyncio.create_task(
                        queue_client.push_to_dlq(
                            event_id=evt.id,
                            subscriber_id="multiple",
                            reason="Max retries exhausted for one or more subscribers",
                            attempts=settings.MAX_DELIVERY_RETRIES
                        )
                    )
                evt.updated_at = datetime.utcnow()
                session.add(evt)
                session.commit()

        await queue_client.acknowledge_event(entry_id)

    async def start_worker_loop(self):
        self.is_running = True
        logger.info("Starting Webhook Delivery Worker loop...")
        
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)
        async with httpx.AsyncClient(limits=limits, timeout=10.0) as http_client:
            while self.is_running:
                try:
                    entries = await queue_client.read_events(
                        consumer_name="worker_1",
                        count=settings.WORKER_CONCURRENCY,
                        block_ms=1000
                    )
                    
                    if entries:
                        tasks = [
                            self.process_single_stream_entry(entry_id, entry_data, http_client)
                            for entry_id, entry_data in entries
                        ]
                        await asyncio.gather(*tasks, return_exceptions=True)
                    else:
                        await asyncio.sleep(0.1)
                except Exception as exc:
                    logger.error(f"Error in worker loop: {exc}")
                    await asyncio.sleep(1.0)

    def stop(self):
        self.is_running = False

delivery_engine = WebhookDeliveryEngine()
