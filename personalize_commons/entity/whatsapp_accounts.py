from pydantic import BaseModel, Field, validator, field_validator
from typing import Optional, Dict, Any, List
from datetime import datetime
import uuid

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.utils.datetime_utils import ist_now


class WhatsAppAccount(BaseModel):
    account_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str = Field(..., description="ID of the tenant that owns this account")
    account_name: str = Field(..., min_length=1, max_length=255, description="Friendly name for the account")
    phone_number_id: str = Field(..., description="WhatsApp Phone Number ID from Meta")
    business_account_id: str = Field(..., description="WhatsApp Business Account ID from Meta")
    access_token_secret_arn: str = Field(..., description="AWS Secrets Manager ARN for access token")
    is_active: bool = Field(default=True, description="Whether the account is active")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    @field_validator('account_id', 'tenant_id', 'phone_number_id', 'business_account_id', 'access_token_secret_arn')
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

        # Convert DynamoDB item to model fields
        return cls(
            account_id=item.get(AppConstants.ACCOUNT_ID),
            tenant_id=item.get(AppConstants.TENANT_ID),
            account_name=item.get('account_name'),
            phone_number_id=item.get('phone_number_id'),
            business_account_id=item.get('business_account_id'),
            access_token_secret_arn=item.get('accessTokenSecretArn'),
            is_active=item.get('is_active', True),
            created_at=cls._parse_datetime(item.get('created_at')),
            updated_at=cls._parse_datetime(item.get('updated_at'))
        )

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

    def to_response_dict(self) -> Dict[str, Any]:
        """Convert to API response format (excludes sensitive fields)"""
        return {
            AppConstants.ACCOUNT_ID: self.account_id,
            AppConstants.TENANT_ID: self.tenant_id,
            'account_name': self.account_name,
            'phone_number_id': self.phone_number_id,
            'business_account_id': self.business_account_id,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }


# Helper functions for batch operations
class WhatsAppAccountBatch:
    @staticmethod
    def from_dynamo_items(items: List[Dict[str, Any]]) -> List[WhatsAppAccount]:
        """Convert multiple DynamoDB items to WhatsAppAccount instances"""
        return [WhatsAppAccount.from_dynamo_item(item) for item in items if item]

    @staticmethod
    def to_dynamo_items(accounts: List[WhatsAppAccount]) -> List[Dict[str, Any]]:
        """Convert multiple WhatsAppAccount instances to DynamoDB items"""
        return [account.to_dynamo_item() for account in accounts]

    @staticmethod
    def to_response_dicts(accounts: List[WhatsAppAccount]) -> List[Dict[str, Any]]:
        """Convert multiple WhatsAppAccount instances to response format"""
        return [account.to_response_dict() for account in accounts]