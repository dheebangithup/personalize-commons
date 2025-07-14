import base64
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any
from uuid import uuid4

from pydantic import BaseModel, Field



'''
PK :tenant_id
SK : recommendation_id
LSI: StatusIndex,CreatedAtIndex

'''
class RecommendationStatus(str, Enum):
    """Status of the recommendation job."""
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RECOM_DONE="RECOMMENDATION_DONE"


class RecommendationMetrics(BaseModel):
    ai_recommended_items: int = Field(..., description="Number of items recommended",alias="ai_recommended_items")
    ai_recommended_users: int = Field(..., description="Number of items recommended",alias="ai_recommended_users")
    recommended_users: int = Field(..., description="Number of users recommended",alias="recommended_users")
    recommended_items: int = Field(..., description="Number of items recommended",alias="recommended_items")
    segment_matched_users: int = Field(..., description="Number of users matched to the target segment",alias="segment_matched_users")
    default_users: int = Field(..., description="if target segment is not provided, to fetch the default users from DB",alias="default_users")
    failed_recommendations: int = Field(..., description="Number of users failed in ai",alias="failed_recommendations")
    message_success_count: int = Field(..., description="Number of successfully processed messages",alias="message_success_count")
    message_failed_count: int = Field(..., description="Number of failed messages",alias="message_failed_count")

class RecommendationEntity(BaseModel):
    """
    Entity representing a recommendation job in the system.
    Uses tenant_id as partition key and recommendation_id as sort key in DynamoDB.
    """

    # Required fields
    tenant_id: str = Field(..., description="Tenant identifier (partition key)")
    recommendation_id: str = Field(..., description="Unique identifier for the recommendation job (sort key)")
    campaign_id: str = Field(..., description="ID of the campaign this recommendation is for")
    status: RecommendationStatus = Field(default=RecommendationStatus.RUNNING, description="Current status of the recommendation job")

    # Recommendation results
    recom_file_key: Optional[str] = Field(None, description="s3 key of the recommendation file")

    # Metrics
    metrics: RecommendationMetrics = Field(None, description="Recommendation metrics")

    # Error handling
    error_message: Optional[str] = Field(None, description="Error message if the job failed")
    recombee_errors: Optional[str] = Field(None, description="recombee api errors")

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow, description="When the recommendation job was created")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="When the recommendation job was last updated")
    completed_at: Optional[datetime] = Field(None, description="When the recommendation job was completed")

    # Additional metadata
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata for the recommendation job")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    def to_dynamodb_item(self) -> Dict[str, Any]:
        """Convert the entity to a DynamoDB item."""
        item = self.model_dump(exclude_none=True)

        # Convert enums to strings
        if 'status' in item:
            item['status'] = item['status'].value

        # Convert datetime objects to ISO format strings
        for field in ['created_at', 'updated_at', 'completed_at']:
            if field in item and item[field] is not None:
                if not isinstance(item[field], str):
                    item[field] = item[field].isoformat()

        return item

    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> 'RecommendationEntity':
        """Create an entity from a DynamoDB item."""
        # Convert string status back to enum
        if 'status' in item and isinstance(item['status'], str):
            item['status'] = RecommendationStatus(item['status'])


        if 'recom_file_key' in item:
            item['recom_file_key'] =  base64.urlsafe_b64encode(str(item['recom_file_key']).encode())
        # Convert ISO format strings back to datetime objects
        for field in ['created_at', 'updated_at', 'completed_at']:
            if field in item and item[field] is not None and isinstance(item[field], str):
                item[field] = datetime.fromisoformat(item[field])

        return cls(**item)

    @staticmethod
    def of(campaign: 'CampaignEntity') -> 'RecommendationEntity':
        """
        Create a RecommendationEntity object from a CampaignEntity object.
        """
        return RecommendationEntity(
            tenant_id=campaign.tenant_id,
            recommendation_id=f"recommendation_{uuid4()}",
            campaign_id=campaign.campaign_id,
            status=RecommendationStatus.RUNNING,
            metadata=campaign.model_dump(),
            created_at=datetime.utcnow()
        )
