import json
import os
import gzip
import io
from datetime import datetime
from typing import List, Dict, Any, Union, Optional
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from personalize_commons.exception.s3_upload_exception import S3UploadException
from decimal import Decimal
from datetime import datetime, date
import uuid
import base64
import math

from personalize_commons.utils.datetime_utils import ist_now

# Optional pandas import
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None


def safe_json_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(obj).decode('utf-8')
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return str(obj)

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
        timestamp = ist_now().strftime("%Y%m%d_%H%M%S")
        filename = f"{recommendation_id}_{timestamp}.jsonl"
        return f"recommendations/{tenant_id}/{campaign_id}/{filename}"

    def get_s3_job_key(self, tenant_id: str, campaign_id: str, recommendation_id: str,job_id) -> str:

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
            job_id: Job identifier

        Returns:
            str: S3 key for the JSONL file
        """
        timestamp = ist_now().strftime("%Y%m%d_%H%M%S")
        filename = f"{job_id}_{timestamp}.jsonl"
        return f"recommendations/{tenant_id}/{campaign_id}/{recommendation_id}/{filename}"

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
            jsonl_content = "\n".join(json.dumps(item,default=safe_json_serializer) for item in data)

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
            raise S3UploadException(f"Failed to upload to S3: {str(e)}")

    def upload_jsonl_with_key(
            self,
            data: List[Dict[str, Any]],
            s3_key: str,
    ) -> str:
        """
        Upload data as JSONL to S3.

        Args:
            data: List of dictionaries to be saved as JSONL
            s3_key: S3 key

        Returns:
            str: S3 URL of the uploaded file

        Raises:
            ClientError: If upload to S3 fails
            :param data:
            :param s3_key
        """
        try:
            # Convert data to JSONL format
            jsonl_content = "\n".join(json.dumps(item,default=safe_json_serializer) for item in data)

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
            raise S3UploadException(f"Failed to upload to S3: {str(e)}")

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

    def upload_file(
            self,
            data: Union[List[Dict[str, Any]], Dict[str, Any], 'pd.DataFrame', str, bytes, Path],
            s3_path: str,
            bucket_name: Optional[str] = None,
            enable_gzip: bool = False,
            content_type: Optional[str] = None,
            metadata: Optional[Dict[str, str]] = None,
            **kwargs
    ) -> str:
        """
        Flexible method to upload various data types to S3.

        Supports:
        - Pandas DataFrame (converted to JSONL, CSV, or Parquet)
        - Dict or List of Dicts (converted to JSON or JSONL)
        - File path (str or Path object) - reads and uploads file
        - Bytes/Bytearray - uploads directly

        Args:
            data: Data to upload. Can be:
                - pandas.DataFrame: DataFrame to upload
                - Dict[str, Any]: Single dictionary (converted to JSON)
                - List[Dict[str, Any]]: List of dictionaries (converted to JSONL)
                - str/Path: File path to read and upload
                - bytes/bytearray: Raw bytes to upload
            s3_path: S3 key/path where file will be stored (e.g., 'folder/subfolder/file.json')
            bucket_name: S3 bucket name. If None, uses default bucket from environment.
            enable_gzip: If True, compresses the data using gzip before uploading.
            content_type: MIME type of the content (e.g., 'application/json', 'text/csv').
                         If None, will be inferred from data type and s3_path extension.
            metadata: Optional metadata dictionary to attach to the S3 object.
            **kwargs: Additional arguments for pandas DataFrame conversion:
                - format: 'jsonl', 'csv', 'parquet' (default: 'jsonl' for DataFrame)
                - index: Include index in CSV/JSONL (default: False)
                - orient: JSON orientation for DataFrame.to_json (default: 'records')

        Returns:
            str: S3 key/path of the uploaded file

        Raises:
            S3UploadException: If upload fails
            ValueError: If data type is not supported or pandas is not available for DataFrame

        Examples:
            # Upload pandas DataFrame as JSONL
            s3_service.upload_file(df, 'data/recommendations.jsonl', enable_gzip=True)

            # Upload list of dicts as JSONL
            s3_service.upload_file([{'id': 1, 'name': 'test'}], 'data/test.jsonl')

            # Upload single dict as JSON
            s3_service.upload_file({'id': 1, 'name': 'test'}, 'data/test.json')

            # Upload file from disk
            s3_service.upload_file('/path/to/file.txt', 'data/file.txt', enable_gzip=True)

            # Upload DataFrame as CSV
            s3_service.upload_file(df, 'data/recommendations.csv', format='csv', enable_gzip=True)
        """
        try:
            bucket = bucket_name or self.bucket_name
            if not bucket:
                raise ValueError("bucket_name must be provided or AWS_S3_BUCKET_NAME environment variable must be set")

            # Determine content type and prepare body
            body, final_content_type, final_s3_path = self._prepare_upload_data(
                data, s3_path, enable_gzip, content_type, **kwargs
            )

            # Prepare upload parameters
            upload_params = {
                'Bucket': bucket,
                'Key': final_s3_path,
                'Body': body,
            }

            # Set content type
            if final_content_type:
                upload_params['ContentType'] = final_content_type

            # Add gzip encoding if enabled
            if enable_gzip:
                upload_params['ContentEncoding'] = 'gzip'

            # Add metadata if provided
            if metadata:
                upload_params['Metadata'] = metadata

            # Upload to S3
            self.s3_client.put_object(**upload_params)

            return final_s3_path

        except ClientError as e:
            raise S3UploadException(f"Failed to upload to S3: {str(e)}")
        except Exception as e:
            raise S3UploadException(f"Failed to prepare or upload data: {str(e)}")

    def _prepare_upload_data(
            self,
            data: Union[List[Dict[str, Any]], Dict[str, Any], 'pd.DataFrame', str, bytes, Path],
            s3_path: str,
            enable_gzip: bool,
            content_type: Optional[str],
            **kwargs
    ) -> tuple[bytes, Optional[str], str]:
        """
        Prepare data for upload by converting to bytes and determining content type.

        Returns:
            tuple: (body_bytes, content_type, final_s3_path)
        """
        # Handle file path input
        if isinstance(data, (str, Path)):
            path = Path(data)
            if path.is_file():
                with open(path, 'rb') as f:
                    body_bytes = f.read()
                # Infer content type from file extension if not provided
                if not content_type:
                    content_type = self._infer_content_type_from_path(path)
                final_s3_path = s3_path
                # Apply gzip compression if enabled
                if enable_gzip:
                    body_bytes = self._compress_gzip(body_bytes)
            else:
                raise FileNotFoundError(f"File not found: {data}")
            return body_bytes, content_type, final_s3_path

        # Handle bytes/bytearray
        if isinstance(data, (bytes, bytearray)):
            body_bytes = bytes(data)
            if not content_type:
                content_type = self._infer_content_type_from_path(s3_path) or 'application/octet-stream'
            final_s3_path = s3_path
            if enable_gzip:
                body_bytes = self._compress_gzip(body_bytes)
            return body_bytes, content_type, final_s3_path

        # Handle pandas DataFrame
        if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
            format_type = kwargs.get('format', 'jsonl').lower()
            include_index = kwargs.get('index', False)
            orient = kwargs.get('orient', 'records')

            if format_type == 'jsonl':
                # Convert DataFrame to JSONL
                jsonl_lines = []
                for _, row in data.iterrows():
                    row_dict = row.to_dict()
                    jsonl_lines.append(json.dumps(row_dict, default=safe_json_serializer))
                body_str = "\n".join(jsonl_lines)
                body_bytes = body_str.encode('utf-8')
                final_content_type = content_type or 'application/jsonl'
                final_s3_path = s3_path if s3_path.endswith('.jsonl') else f"{s3_path}.jsonl"

            elif format_type == 'csv':
                # Convert DataFrame to CSV
                csv_buffer = io.StringIO()
                data.to_csv(csv_buffer, index=include_index)
                body_bytes = csv_buffer.getvalue().encode('utf-8')
                final_content_type = content_type or 'text/csv'
                final_s3_path = s3_path if s3_path.endswith('.csv') else f"{s3_path}.csv"

            elif format_type == 'parquet':
                # Convert DataFrame to Parquet (requires pyarrow or fastparquet)
                parquet_buffer = io.BytesIO()
                data.to_parquet(parquet_buffer, index=include_index, engine=kwargs.get('engine', 'pyarrow'))
                body_bytes = parquet_buffer.getvalue()
                final_content_type = content_type or 'application/parquet'
                final_s3_path = s3_path if s3_path.endswith('.parquet') else f"{s3_path}.parquet"

            elif format_type == 'json':
                # Convert DataFrame to JSON
                json_str = data.to_json(orient=orient, date_format='iso', default_handler=safe_json_serializer)
                body_bytes = json_str.encode('utf-8')
                final_content_type = content_type or 'application/json'
                final_s3_path = s3_path if s3_path.endswith('.json') else f"{s3_path}.json"

            else:
                raise ValueError(f"Unsupported format for DataFrame: {format_type}. Use 'jsonl', 'csv', 'parquet', or 'json'")

            if enable_gzip:
                body_bytes = self._compress_gzip(body_bytes)

            return body_bytes, final_content_type, final_s3_path

        # Handle dict or list of dicts
        if isinstance(data, dict):
            # Single dict - convert to JSON
            json_str = json.dumps(data, default=safe_json_serializer)
            body_bytes = json_str.encode('utf-8')
            final_content_type = content_type or 'application/json'
            final_s3_path = s3_path if s3_path.endswith('.json') else f"{s3_path}.json"

        elif isinstance(data, list):
            # List - check if it's a list of dicts
            if len(data) == 0:
                # Empty list - create empty JSONL
                body_bytes = b''
                final_content_type = content_type or 'application/jsonl'
                final_s3_path = s3_path if s3_path.endswith('.jsonl') else f"{s3_path}.jsonl"
            elif isinstance(data[0], dict):
                # List of dicts - convert to JSONL
                jsonl_lines = [json.dumps(item, default=safe_json_serializer) for item in data]
                body_str = "\n".join(jsonl_lines)
                body_bytes = body_str.encode('utf-8')
                final_content_type = content_type or 'application/jsonl'
                final_s3_path = s3_path if s3_path.endswith('.jsonl') else f"{s3_path}.jsonl"
            else:
                raise ValueError(
                    f"List must contain dictionaries. Got list of {type(data[0])}"
                )

        else:
            raise ValueError(
                f"Unsupported data type: {type(data)}. "
                f"Supported types: pandas.DataFrame, dict, list[dict], str/Path (file), bytes"
            )

        if enable_gzip:
            body_bytes = self._compress_gzip(body_bytes)

        return body_bytes, final_content_type, final_s3_path

    def _compress_gzip(self, data: bytes) -> bytes:
        """
        Compress data using gzip.

        Args:
            data: Bytes to compress

        Returns:
            bytes: Compressed data
        """
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as gz_file:
            gz_file.write(data)
        return buffer.getvalue()

    def _infer_content_type_from_path(self, path: Union[str, Path]) -> Optional[str]:
        """
        Infer content type from file extension.

        Args:
            path: File path or S3 key

        Returns:
            str: MIME type or None if cannot be inferred
        """
        path_str = str(path)
        extension = Path(path_str).suffix.lower()

        content_type_map = {
            '.json': 'application/json',
            '.jsonl': 'application/jsonl',
            '.csv': 'text/csv',
            '.parquet': 'application/parquet',
            '.txt': 'text/plain',
            '.gz': 'application/gzip',
            '.zip': 'application/zip',
            '.pdf': 'application/pdf',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.xml': 'application/xml',
            '.html': 'text/html',
        }

        return content_type_map.get(extension)