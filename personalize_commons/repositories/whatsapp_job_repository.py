import logging as logger
import os
from typing import List, Optional, Dict, Any

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from personalize_commons.entity.whats_app_job import WhatsAppJob


class WhatsAppJobRepository:
    def __init__(self, resource):
        self.dynamodb = resource
        self.table = self.dynamodb.Table(os.getenv("DYNAMO_TABLE_WHATSAPP_JOB"))

    def create(self, job: WhatsAppJob) -> WhatsAppJob:
        """Create a new WhatsApp job"""
        try:
            dynamo_item = job.to_dynamodb_item()
            self.table.put_item(Item=dynamo_item)
            logger.info(f"Created job {job.job_id} for tenant {job.tenant_id}")
            return job
        except ClientError as e:
            logger.error(f"Failed to create job: {str(e)}")
            raise Exception(f"Failed to create job: {str(e)}")

    def get_by_id(self, tenant_id: str, job_id: str) -> Optional[WhatsAppJob]:
        """Get a WhatsApp job by tenant_id and job_id"""
        try:
            response = self.table.get_item(
                Key={
                    "tenant_id": tenant_id,
                    "job_id": job_id
                }
            )
            item = response.get("Item")
            if not item:
                logger.debug(f"Job {job_id} not found for tenant {tenant_id}")
                return None
            return WhatsAppJob.from_dynamodb_item(item)
        except ClientError as e:
            logger.error(f"Failed to get job {job_id}: {str(e)}")
            raise Exception(f"Failed to get job: {str(e)}")

    def update_job(self, tenant_id: str, job_id: str, updates: Dict[str, Any]) -> Optional[WhatsAppJob]:
        """
        Update a WhatsApp job with given attributes.
        Example: {"status": "PROCESSING", "processed_count": 50}
        """
        # Prevent updating key attributes
        key_attributes = {'tenant_id', 'job_id'}
        if any(key in updates for key in key_attributes):
            # remove PKs
            updates.pop("tenant_id", None)
            updates.pop("job_id", None)

        if not updates:
            raise ValueError("No attributes to update")

        try:
            # Filter out any key attributes that might still be in updates
            updates = {k: v for k, v in updates.items() if k not in key_attributes}

            update_expr = "SET " + ", ".join(f"#{k} = :{k}" for k in updates.keys())
            expr_attr_names = {f"#{k}": k for k in updates.keys()}
            expr_attr_values = {f":{k}": v for k, v in updates.items()}

            response = self.table.update_item(
                Key={"tenant_id": tenant_id, "job_id": job_id},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values,
                ReturnValues="ALL_NEW"
            )

            item = response.get("Attributes")
            return WhatsAppJob.from_dynamodb_item(item) if item else None

        except ClientError as e:
            logger.error(f"Failed to update job {job_id}: {str(e)}")
            if e.response['Error']['Code'] == 'ValidationException':
                raise ValueError(f"Invalid update operation: {str(e)}") from e
            raise Exception(f"Failed to update job: {str(e)}") from e

    # ----------------------------
    # GSI Queries
    # ----------------------------

    def get_by_campaign(self, tenant_id: str, campaign_id: str) -> List[WhatsAppJob]:
        """Query jobs by campaign_id using GSI1"""
        try:
            response = self.table.query(
                IndexName="campaign_index",
                KeyConditionExpression=Key("tenant_id").eq(tenant_id) & Key("campaign_id").eq(campaign_id)
            )
            return [WhatsAppJob.from_dynamodb_item(item) for item in response.get("Items", [])]
        except ClientError as e:
            logger.error(f"Failed to query jobs for campaign {campaign_id}: {str(e)}")
            raise Exception(f"Failed to query jobs: {str(e)}")

    def get_by_recommendation(self, tenant_id: str, recommendation_id: str) -> List[WhatsAppJob]:
        """Query jobs by recommendation_id using GSI2"""
        try:
            response = self.table.query(
                IndexName="recommendation_index",
                KeyConditionExpression=Key("tenant_id").eq(tenant_id) & Key("recommendation_id").eq(recommendation_id)
            )
            return [WhatsAppJob.from_dynamodb_item(item) for item in response.get("Items", [])]
        except ClientError as e:
            logger.error(f"Failed to query jobs for recommendation {recommendation_id}: {str(e)}")
            raise Exception(f"Failed to query jobs: {str(e)}")

    def get_by_status(self, tenant_id: str, status: str) -> List[WhatsAppJob]:
        """Query jobs by status using GSI3"""
        try:
            response = self.table.query(
                IndexName="status_index",
                KeyConditionExpression=Key("tenant_id").eq(tenant_id) & Key("status").eq(status)
            )
            return [WhatsAppJob.from_dynamodb_item(item) for item in response.get("Items", [])]
        except ClientError as e:
            logger.error(f"Failed to query jobs for status {status}: {str(e)}")
            raise Exception(f"Failed to query jobs: {str(e)}")
