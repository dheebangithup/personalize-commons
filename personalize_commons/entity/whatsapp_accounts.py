from pydantic import BaseModel, Field, validator, field_validator
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import uuid

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.utils.datetime_utils import ist_now


class WhatsAppAccount(BaseModel):
    account_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str = Field(..., description="ID of the tenant that owns this account")
    account_name: str = Field(..., min_length=1, max_length=255, description="Friendly name for the account")
    phone_number_id: str = Field(..., description="WhatsApp Phone Number ID from Meta")
    business_account_id: str = Field(..., description="WhatsApp Business Account ID from Meta")
    access_token: str = Field(..., description="AWS Secrets Manager ARN for access token")
    is_active: bool = Field(default=True, description="Whether the account is active")
    app_id: Optional[str] = Field(None, description="Meta App ID for token refresh")
    app_secret: Optional[str] = Field(None, description="AWS Secrets Manager ARN for app secret")
    token_expires_at: Optional[datetime] = Field(None, description="Access token expiration date")
    token_last_refreshed: Optional[datetime] = Field(None, description="When token was last refreshed")
    created_at: datetime = Field(default_factory=ist_now)
    updated_at: datetime = Field(default_factory=ist_now)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    @field_validator('account_id', 'tenant_id', 'phone_number_id', 'business_account_id', 'access_token')
    def validate_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Field cannot be empty')
        return v.strip()

    @field_validator('account_name')
    def validate_account_name(cls, v):
        if len(v) > 255:
            raise ValueError('Account name cannot exceed 255 characters')
        return v

    @classmethod
    def from_dynamo_item(cls, item: Dict[str, Any]) -> 'WhatsAppAccount':
        """Create a WhatsAppAccount instance from a DynamoDB item"""
        if not item:
            return None
        return cls(**item)

    @staticmethod
    def _parse_datetime(dt_str: Optional[str]) -> datetime:
        """Parse datetime from ISO format string"""
        if not dt_str:
            return ist_now()
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return ist_now()

    def activate(self) -> None:
        """Activate the account"""
        self.is_active = True
        self.updated_at = ist_now()

    def deactivate(self) -> None:
        """Deactivate the account"""
        self.is_active = False
        self.updated_at = ist_now()

    def update_account_name(self, new_name: str) -> None:
        """Update account name with validation"""
        if not new_name or not new_name.strip():
            raise ValueError("Account name cannot be empty")
        if len(new_name) > 255:
            raise ValueError("Account name cannot exceed 255 characters")

        self.account_name = new_name.strip()
        self.updated_at = ist_now()

    def is_token_expired(self) -> bool:
        """Check if token is expired or about to expire"""
        if not self.token_expires_at:
            return True

        # Consider token expired if it expires within 1 hours
        from datetime import datetime
        seven_days_from_now = datetime.now() + timedelta(hours=1)
        return self.token_expires_at <= seven_days_from_now

    def update_token_info(self, expires_in_seconds: int):
        """Update token expiration information"""
        from datetime import datetime, timedelta
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in_seconds)
        self.token_last_refreshed = datetime.now()
        self.updated_at = datetime.now()


# Helper functions for batch operations
class WhatsAppAccountBatch:
    @staticmethod
    def from_dynamo_items(items: List[Dict[str, Any]]) -> List[WhatsAppAccount]:
        """Convert multiple DynamoDB items to WhatsAppAccount instances"""
        return [WhatsAppAccount.from_dynamo_item(item) for item in items if item]


