import logging
from typing import Optional
from fastapi import APIRouter, Request, Header, HTTPException, status
from app.reliability.hmac_signer import HMACSigner

router = APIRouter(prefix="/mock", tags=["Mock Endpoint"])
logger = logging.getLogger("mock_receiver")

@router.post("/webhook-receiver")
async def mock_webhook_receiver(
    request: Request,
    x_webhook_signature: Optional[str] = Header(None, alias="X-Webhook-Signature"),
    x_webhook_idem_key: Optional[str] = Header(None, alias="X-Webhook-Idem-Key"),
    x_webhook_event: Optional[str] = Header(None, alias="X-Webhook-Event")
):
    body_bytes = await request.body()
    logger.info(f"Received Webhook: Event={x_webhook_event}, IdemKey={x_webhook_idem_key}, Body={body_bytes.decode('utf-8')}")

    return {
        "status": "received",
        "event_type": x_webhook_event,
        "idem_key": x_webhook_idem_key,
        "signature_received": x_webhook_signature
    }
