from dataclasses import dataclass
from datetime import datetime


@dataclass
class Delivery:
    """Delivery domain entity."""

    delivery_id: int
    order_id: int
    status: str
    delivered_at: datetime | None = None
    tracking_number: str | None = None
