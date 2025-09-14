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
    access_token_secret_arn: str = Field(..., description="AWS Secrets Manager ARN for access token")
    is_active: bool = Field(default=True, description="Whether the account is active")
    app_id: Optional[str] = Field(None, description="Meta App ID for token refresh")
    app_secret_arn: Optional[str] = Field(None, description="AWS Secrets Manager ARN for app secret")
    token_expires_at: Optional[datetime] = Field(None, description="Access token expiration date")
    token_last_refreshed: Optional[datetime] = Field(None, description="When token was last refreshed")
    created_at: datetime = Field(default_factory=ist_now)
    updated_at: datetime = Field(default_factory=ist_now)

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

        # Map DynamoDB field names to model field names
        field_mapping = {
            'accountId': 'account_id',
            'tenantId': 'tenant_id',
            'accountName': 'account_name',
            'phoneNumberId': 'phone_number_id',
            'businessAccountId': 'business_account_id',
            'accessTokenSecretArn': 'access_token_secret_arn',
            'appId': 'app_id',
            'appSecretArn': 'app_secret_arn',
            'tokenExpiresAt': 'token_expires_at',
            'tokenLastRefreshed': 'token_last_refreshed',
            'isActive': 'is_active',
            'createdAt': 'created_at',
            'updatedAt': 'updated_at'
        }
        
        # Create a new dict with model field names
        model_data = {}
        for db_field, model_field in field_mapping.items():
            if db_field in item:
                model_data[model_field] = item[db_field]
        
        # Handle datetime fields
        datetime_fields = ['token_expires_at', 'token_last_refreshed', 'created_at', 'updated_at']
        for field in datetime_fields:
            if field in model_data:
                model_data[field] = cls._parse_datetime(model_data[field])
        
        # Handle boolean field
        if 'is_active' in model_data and not isinstance(model_data['is_active'], bool):
            model_data['is_active'] = str(model_data['is_active']).lower() == 'true'

        return cls(**model_data)

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
        response = {
            'account_id': self.account_id,
            'tenant_id': self.tenant_id,
            'account_name': self.account_name,
            'phone_number_id': self.phone_number_id,
            'business_account_id': self.business_account_id,
            'access_token_secret_arn': self.access_token_secret_arn,
            'app_id': self.app_id,
            'app_secret_arn': self.app_secret_arn,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

        # Add token management fields if they exist
        if self.token_expires_at:
            response['token_expires_at'] = self.token_expires_at.isoformat()
        if self.token_last_refreshed:
            response['token_last_refreshed'] = self.token_last_refreshed.isoformat()

        return response

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

    @staticmethod
    def to_dynamo_items(accounts: List[WhatsAppAccount]) -> List[Dict[str, Any]]:
        """Convert multiple WhatsAppAccount instances to DynamoDB items"""
        return [account.to_dynamo_item() for account in accounts]

    @staticmethod
    def to_response_dicts(accounts: List[WhatsAppAccount]) -> List[Dict[str, Any]]:
        """Convert multiple WhatsAppAccount instances to response format"""
        return [account.to_response_dict() for account in accounts]