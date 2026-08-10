import hmac
import hashlib
import time

class HMACSigner:
    """
    Computes HMAC-SHA256 signatures for webhook security.
    Header format: t=<timestamp>,v1=<signature_hex>
    This allows subscribers to verify that webhooks were authored by our system.
    """
    @staticmethod
    def generate_signature(secret: str, payload_bytes: bytes, timestamp: int = None) -> tuple[str, str]:
        if timestamp is None:
            timestamp = int(time.time())
        
        # Prepare signed string: {timestamp}.{payload}
        signed_payload = f"{timestamp}.".encode("utf-8") + payload_bytes
        
        signature = hmac.new(
            secret.encode("utf-8"),
            signed_payload,
            hashlib.sha256
        ).hexdigest()
        
        header_value = f"t={timestamp},v1={signature}"
        return header_value, signature

    @staticmethod
    def verify_signature(secret: str, payload_bytes: bytes, header_value: str, tolerance_sec: int = 300) -> bool:
        try:
            parts = dict(item.split("=", 1) for item in header_value.split(","))
            timestamp = int(parts.get("t", 0))
            signature = parts.get("v1", "")

            # Check timestamp tolerance (prevent replay attacks)
            current_time = int(time.time())
            if abs(current_time - timestamp) > tolerance_sec:
                return False

            # Recalculate signature
            signed_payload = f"{timestamp}.".encode("utf-8") + payload_bytes
            expected_signature = hmac.new(
                secret.encode("utf-8"),
                signed_payload,
                hashlib.sha256
            ).hexdigest()

            return hmac.compare_digest(signature, expected_signature)
        except Exception:
            return False
