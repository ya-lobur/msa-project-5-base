from dataclasses import dataclass
from datetime import datetime


@dataclass
class Customer:
    """Customer domain entity."""

    customer_id: int
    email: str
    name: str
    created_at: datetime
    is_active: bool = True
