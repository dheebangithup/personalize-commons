from typing import Dict

from pydantic import BaseModel, Field

from personalize_commons.utils.datetime_utils import ist_now


class InteractionTrackingEntity(BaseModel):
    tenant_id: str = Field(..., description="Tenant/User ID")
    month: str = Field(default_factory=lambda: ist_now().strftime("%Y-%m"),
                       description="Month in YYYY-MM format (IST)")
    interactions: Dict[str, int] = Field(default_factory=dict,
                                         description="Map of event type to count")

    unique_users: int = Field(
        default=0,
        description="Number of distinct end-users who had at least one interaction in this month"
    )
