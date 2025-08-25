from pydantic import BaseModel, Field
from personalize_commons.utils.datetime_utils import ist_now
from typing import Optional


class IntractionUserTrackerEntity(BaseModel):
    """
    Represents a single record in the intraction_user_tracker table.
    Each row marks one unique end-user for a given tenant and month.
    """
    tenant_id: str = Field(..., description="Tenant ID")
    user_id: str = Field(..., description="End-user ID inside tenant")
    month: str = Field(
        default_factory=lambda: ist_now().strftime("%Y-%m"),
        description="Month in YYYY-MM format (IST)"
    )
    expire_at: Optional[int] = Field(
        None, description="Epoch timestamp for TTL (auto-expire in DynamoDB)"
    )

    @property
    def tenant_month(self) -> str:
        """
        Composite PK for DynamoDB (tenant_id + month).
        """
        return f"{self.tenant_id}#{self.month}"
