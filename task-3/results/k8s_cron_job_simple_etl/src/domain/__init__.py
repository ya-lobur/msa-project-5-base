"""Domain layer - business entities and repository interfaces."""

from src.domain.entities import Shipment
from src.domain.repositories import ShipmentRepository

__all__ = ["Shipment", "ShipmentRepository"]
