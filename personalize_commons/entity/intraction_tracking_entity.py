from typing import Dict

from pydantic import BaseModel, Field

from personalize_commons.utils.datetime_utils import ist_now


class IntractionTrackingEntity(BaseModel):
    tenant_id: str = Field(..., description="Tenant/User ID")
    month: str = Field(default_factory=lambda: ist_now().strftime("%Y-%m"),
                       description="Month in YYYY-MM format")
    interactions: Dict[str, int] = Field(default_factory=dict,
                                         description="Map of event type to count")
