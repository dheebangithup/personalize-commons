# src/entity/whats_app_job.py
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any

from personalize_commons.utils.datetime_utils import ist_now
from pydantic import BaseModel, Field

from personalize_commons.utils.security_util import SecurityUtil


class JobStatus(str, Enum):
    PARTIALLY_COMPLETED = 'PARTIALLY_COMPLETED'
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

"""
DynamoDB Table: WhatsAppJob
===========================

Primary Key:
------------
- tenant_id (PK) : string   → isolates jobs per tenant
- job_id   (SK) : string   → unique ID for each job within a tenant

Attributes:
-----------
- tenant_id        : str  (partition key)
- job_id           : str  (sort key, UUID)
- campaign_id      : str
- recommendation_id: str
- status           : str   (PENDING | PROCESSING | COMPLETED | FAILED)
- total_recipients : int
- processed_count  : int
- success_count    : int
- failed_count     : int
- report_s3_key    : str
- created_at       : ISO datetime
- started_at       : ISO datetime (optional)
- completed_at     : ISO datetime (optional)
- error_message    : str (optional)

Global Secondary Indexes (GSIs):
--------------------------------
1. campaign_index
   - PK: tenant_id
   - SK: campaign_id
   → Query all jobs for a given campaign (per tenant).

2. recommendation_index
   - PK: tenant_id
   - SK: recommendation_id
   → Query all jobs for a given recommendation (per tenant).

3. status_index
   - PK: tenant_id
   - SK: status
   → Query jobs by status (e.g., PENDING, PROCESSING) per tenant.

Notes:
------
- `created_at` can be used for sorting results within queries.
- Typical access patterns:
    - Get job by tenant_id + job_id
    - List jobs for a campaign
    - List jobs for a recommendation
    - List jobs by status (e.g., pending jobs for processing workers)
"""

class WhatsAppJob(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str                                      # NEW field
    campaign_id: str
    recommendation_id: str
    status: JobStatus = JobStatus.PENDING
    total_recipients: int = 0
    processed_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    report_s3_key: str = ""
    created_at: datetime = Field(default_factory=ist_now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None

    def to_dynamodb_item(self) -> Dict[str, Any]:
        """Convert to DynamoDB item format."""
        item = self.model_dump()

        # Convert enums to strings
        if 'status' in item and item['status']:
            item['status'] = item['status'].value
            
        # Convert datetime objects to ISO format strings
        datetime_fields = ['created_at', 'started_at', 'completed_at']
        for field in datetime_fields:
            if field in item and item[field] is not None:
                if isinstance(item[field], datetime):
                    item[field] = item[field].isoformat()
                    
        return item

    @classmethod
    def from_dynamodb_item(cls, item: Dict[str, Any]) -> 'WhatsAppJob':
        """Create from DynamoDB item."""
        # Convert DynamoDB format to our model
        if 'status' in item and item['status']:
            item['status'] = JobStatus(item['status'])
        if 'report_s3_key' in item and item['report_s3_key'] is not None:
            item['report_s3_key'] = SecurityUtil.encode_b64(str(item['report_s3_key']))

        return cls(**item)
