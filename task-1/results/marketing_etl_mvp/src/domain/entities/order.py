from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class Order:
    """Order domain entity."""

    order_id: int
    customer_id: int
    total_amount: Decimal
    status: str
    created_at: datetime
    updated_at: datetime
