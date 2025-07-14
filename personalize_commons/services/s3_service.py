import json
import os
from datetime import datetime
from typing import List, Dict, Any

import boto3
from botocore.exceptions import ClientError


class S3Service:
    """Service for handling S3 operations for JSONL files."""

    def __init__(self,client ):
        """
        Initialize the S3 service.

        Args:
            bucket_name: Name of the S3 bucket. If not provided, will use S3_BUCKET from environment.
        """
        self.bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
        if not self.bucket_name:
            raise ValueError("S3_BUCKET environment variable must be set")

        self.s3_client = client

    def _get_s3_key(self, tenant_id: str, recommendation_id: str, campaign_id: str) -> str:

        """
        Generate an S3 key for a recommendation JSONL file.

        s3 path like: recommendations/
                          /tenant_id/
                          /campaign_id/
                          /recommendation_id/
                          /filename.jsonl
        Args:
            tenant_id: Tenant identifier
            recommendation_id: Recommendation identifier
            campaign_id: Campaign identifier

        Returns:
            str: S3 key for the JSONL file
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{recommendation_id}_{timestamp}.jsonl"
        return f"recommendations/{tenant_id}/{campaign_id}/{filename}"

    def upload_jsonl(
            self,
            data: List[Dict[str, Any]],
            tenant_id: str,
            recommendation_id: str,
            campaign_id: str,
    ) -> str:
        """
        Upload data as JSONL to S3.

        Args:
            data: List of dictionaries to be saved as JSONL
            tenant_id: Tenant identifier
            recommendation_id: Recommendation identifier
            filename: Optional custom filename (without extension)

        Returns:
            str: S3 URL of the uploaded file

        Raises:
            ClientError: If upload to S3 fails
            :param data:
            :param tenant_id:
            :param recommendation_id:
            :param campaign_id:
        """
        try:
            # Convert data to JSONL format
            jsonl_content = "\n".join(json.dumps(item) for item in data)

            # Generate S3 key
            s3_key = self._get_s3_key(tenant_id, recommendation_id, campaign_id)

            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=jsonl_content,
                ContentType='application/jsonl'
            )

            # Generate and return the S3 URL
            return s3_key

        except ClientError as e:
            raise Exception(f"Failed to upload to S3: {str(e)}")

    def download_dict(
            self,
            s3key: str,
    ) -> List[Dict[str, Any]]:

        try:
            # Get object from S3
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3key
            )
            # Read and parse JSONL content
            content = response['Body'].read().decode('utf-8')
            return [json.loads(line) for line in content.splitlines() if line.strip()]

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"File not found: {s3key}")
            raise Exception(f"Failed to download from S3: {str(e)}")

    def download_jsonl(
            self,
            s3key: str,
    ):

        try:
            # Get object from S3
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3key
            )
            # Read and parse JSONL content
            content = response['Body'].read().decode('utf-8')
            return content

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"File not found: {s3key}")
            raise Exception(f"Failed to download from S3: {str(e)}")