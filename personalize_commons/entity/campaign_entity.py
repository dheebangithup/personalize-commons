from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.constants.db_constants import  DBConstants
from personalize_commons.utils.datetime_utils import to_ist_iso

'''
PK :tenant_id
SK : campaign_id
GSI: tenant_updated_at_index : PK:tenant_id, SK: updated_at
'''

class RecommendationLogic(str, Enum):
    ITEMS_TO_USER=AppConstants.ITEMS_TO_USER
    ITEMS_TO_ITEM=AppConstants.ITEMS_TO_ITEM
    USERS_TO_ITEM=AppConstants.USERS_TO_ITEM

class CampaignStatus(str, Enum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    STOPPED = "STOPPED"
    PAUSED = "PAUSED"


class CampaignEntity(BaseModel):


    # Core fields
    campaign_id: str = Field(default_factory=lambda: f"campaign_{uuid4()}")
    campaign_name: str = Field(..., max_length=255)
    industry_type: str = Field(..., max_length=100)
    target_segment: Dict[str, Any] = Field(..., description="Serialized QueryRequest")
    message_template: dict[str, Any]
    tenant_id: str
    item_id: Optional[str] = None

    # Status and metadata
    status: CampaignStatus = CampaignStatus.DRAFT
    description: Optional[str] = None
    recommendation_logic: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None

    # Timestamps
    start_date: Optional[str] = None  # ISO format string
    end_date: Optional[str] = None  # ISO format string
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    # Additional metadata
    created_by: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "PK": "TENANT#tenant123",
                "SK": "CAMPAIGN#campaign_abc123",
                "campaign_id": "campaign_abc123",
                "campaign_name": "Summer Sale",
                "industry_type": "E-commerce",
                "target_segment": {"conditions": {"age": {"operator": ">", "value": 18}}},
                "message_template": "Hello {name}, check out our sale!",
                "tenant_id": "tenant123",
                "status": "DRAFT",
                "created_at": "2023-01-01T00:00:00.000Z",
                "updated_at": "2023-01-01T00:00:00.000Z"
            }
        }

    def to_dynamodb_item(self) -> Dict[str, Any]:
        """Convert to DynamoDB item format."""
        item = self.model_dump()

        # Convert enums to strings
        if 'status' in item and item['status']:
            item['status'] = item['status'].value
        return item

    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> 'CampaignEntity':
        """Create from DynamoDB item."""
        # Convert DynamoDB format to our model
        if 'status' in item and item['status']:
            item['status'] = CampaignStatus(item['status'])
        if DBConstants.CREATED_AT in item and item[DBConstants.CREATED_AT]:
            item[DBConstants.CREATED_AT] = to_ist_iso( item[DBConstants.CREATED_AT])

        if DBConstants.UPDATED_AT in item and item[DBConstants.UPDATED_AT]:
            item[DBConstants.UPDATED_AT] = to_ist_iso(item[DBConstants.UPDATED_AT])

        return cls(**item)