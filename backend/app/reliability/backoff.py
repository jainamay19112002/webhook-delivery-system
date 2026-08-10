import random
import math
from app.config import settings

class ExponentialBackoffWithJitter:
    """
    Implements Full Jitter Exponential Backoff.
    prevents thundering herd problem when subscribers recover from downtime.
    """
    def __init__(
        self,
        initial_backoff: float = settings.INITIAL_BACKOFF_SECONDS,
        multiplier: float = settings.BACKOFF_MULTIPLIER,
        max_backoff: float = settings.MAX_BACKOFF_SECONDS
    ):
        self.initial_backoff = initial_backoff
        self.multiplier = multiplier
        self.max_backoff = max_backoff

    def calculate_delay(self, attempt: int) -> float:
        """
        attempt: 1-indexed attempt number (1, 2, 3...)
        Returns delay in seconds.
        """
        if attempt <= 1:
            return 0.0
            
        calculated_backoff = self.initial_backoff * math.pow(self.multiplier, attempt - 2)
        capped_backoff = min(self.max_backoff, calculated_backoff)
        
        # Full jitter: random float between 0 and capped_backoff
        jittered_delay = random.uniform(0, capped_backoff)
        return round(jittered_delay, 3)
