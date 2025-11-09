"""
MediaFileEntity for storing file metadata in DynamoDB
PK: tenant_id
SK: file_key (full R2 key including folder path)
LSI: FolderIndex (folder), TypeIndex (type), TagIndex (tag), IsTempIndex (is_temp)

AWS CLI Command (Alternative):
------------------------------
aws dynamodb create-table \
    --table-name media_library \
    --attribute-definitions \
        AttributeName=tenant_id,AttributeType=S \
        AttributeName=file_key,AttributeType=S \
        AttributeName=folder,AttributeType=S \
        AttributeName=type,AttributeType=S \
        AttributeName=tag,AttributeType=S \
        AttributeName=is_temp,AttributeType=N \
    --key-schema \
        AttributeName=tenant_id,KeyType=HASH \
        AttributeName=file_key,KeyType=RANGE \
    --local-secondary-indexes \
        'IndexName=FolderIndex,KeySchema=[{AttributeName=tenant_id,KeyType=HASH},{AttributeName=folder,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
        'IndexName=TypeIndex,KeySchema=[{AttributeName=tenant_id,KeyType=HASH},{AttributeName=type,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
        'IndexName=TagIndex,KeySchema=[{AttributeName=tenant_id,KeyType=HASH},{AttributeName=tag,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
        'IndexName=IsTempIndex,KeySchema=[{AttributeName=tenant_id,KeyType=HASH},{AttributeName=is_temp,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    --billing-mode PAY_PER_REQUEST
"""
from datetime import datetime
from typing import Optional, Dict, Any
from uuid import uuid4

from pydantic import BaseModel, Field

from personalize_commons.utils.datetime_utils import ist_now


class MediaFileEntity(BaseModel):
    """
    Entity representing a media file with metadata.
    Uses tenant_id as partition key and file_key as sort key in DynamoDB.
    """

    # Required fields
    tenant_id: str = Field(..., description="Tenant identifier (partition key)")
    file_key: str = Field(..., description="Full R2 key including folder path (sort key)")
    
    # File information
    file_name: str = Field(..., description="Original file name")
    file_size: int = Field(..., description="File size in bytes")
    content_type: Optional[str] = Field(None, description="MIME type of the file")
    public_url: str = Field(..., description="Public URL for the file")
    
    # Metadata
    folder: Optional[str] = Field(None, description="Folder/path within tenant (e.g., 'Festival_Offers')")
    tag: Optional[str] = Field(None, description="Custom tag for categorization (free-form text)")
    type: Optional[str] = Field(None, description="File type: image, video, document")
    is_temp: bool = Field(default=False, description="Whether file is temporary")
    uploaded_by: Optional[str] = Field(None, description="User ID who uploaded the file")
    
    # Timestamps
    created_at: datetime = Field(default_factory=ist_now, description="When the file was uploaded")
    updated_at: datetime = Field(default_factory=ist_now, description="When the file was last updated")
    
    # Additional metadata
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    def to_dynamodb_item(self) -> Dict[str, Any]:
        """Convert the entity to a DynamoDB item."""
        item = self.model_dump(exclude_none=True, mode='python')
        
        # Convert datetime objects to ISO format strings
        for field in ['created_at', 'updated_at']:
            if field in item and item[field] is not None:
                if not isinstance(item[field], str):
                    item[field] = item[field].isoformat()
        
        # Convert bool is_temp to number (0 or 1) for DynamoDB index compatibility
        # IsTempIndex expects Number (N) type
        if 'is_temp' in item and isinstance(item['is_temp'], bool):
            item['is_temp'] = 1 if item['is_temp'] else 0
        
        return item

    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> 'MediaFileEntity':
        """Create an entity from a DynamoDB item."""
        # Convert ISO format strings back to datetime objects
        for field in ['created_at', 'updated_at']:
            if field in item and item[field] is not None and isinstance(item[field], str):
                item[field] = datetime.fromisoformat(item[field])
        
        # Convert number is_temp back to bool (handles both number 0/1 and bool values)
        # IsTempIndex stores as Number (N) in DynamoDB
        if 'is_temp' in item:
            if isinstance(item['is_temp'], (int, float)):
                item['is_temp'] = bool(item['is_temp'])
            elif isinstance(item['is_temp'], str):
                # Handle string "true"/"false" or "0"/"1" for backward compatibility
                item['is_temp'] = item['is_temp'].lower() == 'true' or item['is_temp'] == '1'
        
        return cls(**item)

    @staticmethod
    def generate_file_key(tenant_id: str, folder: Optional[str], file_name: str) -> str:
        """
        Generate R2 file key with folder support.
        Format: {tenant_id}/{folder}/{file_name} or {tenant_id}/{file_name}
        """
        if folder:
            # Sanitize folder name (remove special chars, spaces become underscores)
            sanitized_folder = folder.strip().replace(' ', '_').replace('/', '_')
            return f"{tenant_id}/{sanitized_folder}/{file_name}"
        return f"{tenant_id}/{file_name}"

