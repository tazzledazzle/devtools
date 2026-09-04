from decimal import Decimal
from dataclasses import dataclass
from enum import Enum


class Inventory:
    """"""
    def __init__(self, sku: str, quantity: int) -> None:
        self.sku = sku
        self._quantity = 0
        self.quantity = quantity


    @property
    def quantity(self) -> int:
        return self._quantity

    @quantity.setter
    def quantity(self, value: int) -> None:
        if value < 0:
            raise ValueError(f"quantity cannot be negative, got {value}")
        self._quantity = value

    @property
    def is_out_of_stock(self) -> bool:
        """Computed, read-only"""
        return self._quantity == 0
@dataclass
class Employee:
    name: str
    department: str
    salary: Decimal
    bonus_pct: Decimal = Decimal("0")
    reports: list[str] = field(default_factory=list)

class OrderStatus(Enum):
    PLACED = "placed"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


@dataclass
class Order:
    order_id: str
    customer: str
    total: Decimal
    status: OrderStatus = OrderStatus.PLACED

    @classmethod
    def from_csv_row(cls, row: str) -> "Order":
        """"""
        order_id, customer, total = row.strip().split(",")
        return cls(order_id=order_id, customer=customer, total=Decimal(total))

class PricingRules:
    @staticmethod
    def is_bulk_order(quantity: int, threshold: int = 100) -> bool:
        return quantity >= threshold

# Custom exceptions for domain errors - callers catch them instead of bare `except Exception`
class InsufficientInventoryError(Exception):
    def __init__(self,sku: str, requested: int, available: int) -> None:
        self.sku, self.requested, self.available = sku, requested, available
        super().__init__(
            f"cannot fulfill {requested} of {sku}; only {available} available"
        )


def fulfill_order(inventory: Inventory, request_qty: int) -> None:
    if request_qty > inventory.quantity:
        raise InsufficientInventoryError(inventory.sku, request_qty, inventory.quantity)
    inventory.quantity -= request_qty


# given two strings str1 and str2 where str1 contains one char more than str2 find 
# indx of chars in str1 that get removed to make str2 and str1 
# equal. return array of indices in increasing order
# if not possible return array[-1]

getRemoveableIndices