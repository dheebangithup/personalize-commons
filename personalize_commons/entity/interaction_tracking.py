from dataclasses import dataclass
from typing import Dict

@dataclass
class InteractionTracking:
    tenant_id: str
    month: str  # format YYYY-MM
    interactions: Dict[str, int]  # e.g., {"purchase": 15, "add_to_cart": 7}
    active_users: int
