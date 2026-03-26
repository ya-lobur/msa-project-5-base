"""Domain entities for the ETL system."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional


@dataclass
class Shipment:
    """Shipment entity representing a cargo transportation order."""

    id: int
    client_id: int
    driver_id: int
    vehicle_id: int
    origin: str
    destination: str
    cargo_type: str
    cargo_weight: Decimal
    price: Decimal
    status: str
    created_at: datetime
    updated_at: datetime
    pickup_date: Optional[datetime] = None
    delivery_date: Optional[datetime] = None
    notes: Optional[str] = None
