"""
MediaFileRepository for managing media file metadata in DynamoDB
"""
import logging
import os
from typing import Optional, Dict, Any, List
from datetime import datetime

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.entity.media_file_entity import MediaFileEntity

logger = logging.getLogger(__name__)


class MediaFileRepository:
    """Repository class for handling MediaFileEntity CRUD operations with DynamoDB."""

    def __init__(self, resource):
        """Initialize the repository with DynamoDB connection."""
        self.resource = resource
        self.table_name = os.getenv('DYNAMO_TABLE_MEDIA_LIBRARY')
        self.table = self.resource.Table(self.table_name)

    def create_media_file(self, media_file: MediaFileEntity) -> MediaFileEntity:
        """
        Create a new media file record in DynamoDB.

        Args:
            media_file: The media file entity to create

        Returns:
            The created media file entity

        Raises:
            ValueError: If required fields are missing
            Exception: For DynamoDB errors
        """
        try:
            if not media_file.tenant_id or not media_file.file_key:
                raise ValueError("tenant_id and file_key are required")

            # Convert to DynamoDB item and save
            item = media_file.to_dynamodb_item()
            self.table.put_item(Item=item)
            logger.info(f"Created media file {media_file.file_key} for tenant {media_file.tenant_id}")
            return media_file

        except ClientError as e:
            error_msg = f"DynamoDB error creating media file: {e.response['Error']['Message']}"
            logger.error(error_msg, exc_info=True)
            raise Exception(error_msg) from e
        except Exception as e:
            logger.error(f"Unexpected error creating media file: {str(e)}", exc_info=True)
            raise

    def get_media_file(self, tenant_id: str, file_key: str) -> Optional[MediaFileEntity]:
        """
        Get a media file by tenant_id and file_key.

        Args:
            tenant_id: The tenant identifier
            file_key: The file key (sort key)

        Returns:
            The media file entity if found, None otherwise
        """
        try:
            response = self.table.get_item(
                Key={
                    'tenant_id': tenant_id,
                    'file_key': file_key
                }
            )
            item = response.get('Item')
            if item:
                return MediaFileEntity.from_dynamodb_item(item)
            return None
        except ClientError as e:
            logger.error(f"Error getting media file: {e}", exc_info=True)
            raise

    def update_media_file(self, tenant_id: str, file_key: str, update_data: Dict[str, Any]) -> Optional[MediaFileEntity]:
        """
        Update a media file with the provided data.

        Args:
            tenant_id: The tenant identifier
            file_key: The file key
            update_data: Dictionary of fields to update

        Returns:
            The updated MediaFileEntity if successful, None if not found
        """
        try:
            existing = self.get_media_file(tenant_id, file_key)
            if not existing:
                return None

            # Update with new values
            update_dict = existing.model_dump()
            update_dict.update(update_data)
            update_dict['updated_at'] = datetime.now()

            # Convert back to entity and save
            updated = MediaFileEntity(**update_dict)
            item = updated.to_dynamodb_item()
            self.table.put_item(Item=item)

            logger.info(f"Updated media file {file_key} for tenant {tenant_id}")
            return updated

        except ClientError as e:
            logger.error(f"Error updating media file: {e}", exc_info=True)
            raise

    def delete_media_file(self, tenant_id: str, file_key: str) -> bool:
        """
        Delete a media file record.

        Args:
            tenant_id: The tenant identifier
            file_key: The file key

        Returns:
            True if deleted, False if not found
        """
        try:
            response = self.table.delete_item(
                Key={
                    'tenant_id': tenant_id,
                    'file_key': file_key
                },
                ReturnValues='ALL_OLD'
            )
            deleted = response.get('Attributes') is not None
            if deleted:
                logger.info(f"Deleted media file {file_key} for tenant {tenant_id}")
            return deleted
        except ClientError as e:
            logger.error(f"Error deleting media file: {e}", exc_info=True)
            raise

    def list_media_files(
        self,
        tenant_id: str,
        folder: Optional[str] = None,
        tag: Optional[str] = None,
        file_type: Optional[str] = None,
        is_temp: Optional[bool] = None,
        search_query: Optional[str] = None,
        limit: int = 20,
        last_evaluated_key: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        List media files with filtering, search, and pagination.

        Args:
            tenant_id: The tenant identifier
            folder: Optional folder filter
            tag: Optional tag filter
            file_type: Optional type filter
            is_temp: Optional temp filter
            search_query: Optional search query (searches in file_name)
            limit: Maximum number of items to return
            last_evaluated_key: Pagination token from previous request

        Returns:
            Dict with 'items', 'last_evaluated_key', and 'has_more'
        """
        try:
            # Build filter expression
            filter_expression = None
            conditions = []

            if folder:
                conditions.append(Attr('folder').eq(folder))
            if tag:
                conditions.append(Attr('tag').eq(tag))
            if file_type:
                conditions.append(Attr('type').eq(file_type))
            if is_temp is not None:
                # Convert bool to number for DynamoDB query (is_temp is stored as Number in index)
                is_temp_value = 1 if is_temp else 0
                conditions.append(Attr('is_temp').eq(is_temp_value))
            if search_query:
                # Search in file_name (case-insensitive)
                conditions.append(Attr('file_name').contains(search_query.lower()))

            if conditions:
                filter_expression = conditions[0]
                for condition in conditions[1:]:
                    filter_expression = filter_expression & condition

            # Query by tenant_id (partition key)
            query_params = {
                'KeyConditionExpression': Key('tenant_id').eq(tenant_id),
                'Limit': limit,
            }

            if filter_expression:
                query_params['FilterExpression'] = filter_expression

            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key

            response = self.table.query(**query_params)

            items = [
                MediaFileEntity.from_dynamodb_item(item)
                for item in response.get('Items', [])
            ]

            last_key = response.get('LastEvaluatedKey')
            has_more = last_key is not None

            return {
                'items': items,
                'last_evaluated_key': last_key,
                'has_more': has_more
            }

        except ClientError as e:
            logger.error(f"Error listing media files: {e}", exc_info=True)
            raise

    def get_storage_usage(self, tenant_id: str) -> Dict[str, Any]:
        """
        Calculate total storage usage for a tenant.

        Args:
            tenant_id: The tenant identifier

        Returns:
            Dict with 'total_size' (bytes), 'file_count', 'temp_size', 'temp_count'
        """
        try:
            # Query all files for tenant
            response = self.table.query(
                KeyConditionExpression=Key('tenant_id').eq(tenant_id),
                ProjectionExpression='file_size, is_temp'
            )

            total_size = 0
            file_count = 0
            temp_size = 0
            temp_count = 0

            for item in response.get('Items', []):
                size = item.get('file_size', 0)
                is_temp = item.get('is_temp', 0)  # Stored as number (0 or 1) in DynamoDB
                
                total_size += size
                file_count += 1
                
                if is_temp == 1 or is_temp is True:  # Handle both number and bool
                    temp_size += size
                    temp_count += 1

            # Handle pagination if needed
            last_key = response.get('LastEvaluatedKey')
            while last_key:
                response = self.table.query(
                    KeyConditionExpression=Key('tenant_id').eq(tenant_id),
                    ProjectionExpression='file_size, is_temp',
                    ExclusiveStartKey=last_key
                )
                
                for item in response.get('Items', []):
                    size = item.get('file_size', 0)
                    is_temp = item.get('is_temp', 0)  # Stored as number (0 or 1) in DynamoDB
                    
                    total_size += size
                    file_count += 1
                    
                    if is_temp == 1 or is_temp is True:  # Handle both number and bool
                        temp_size += size
                        temp_count += 1
                
                last_key = response.get('LastEvaluatedKey')

            return {
                'total_size': total_size,
                'file_count': file_count,
                'temp_size': temp_size,
                'temp_count': temp_count
            }

        except ClientError as e:
            logger.error(f"Error calculating storage usage: {e}", exc_info=True)
            raise

    def delete_temp_files(self, tenant_id: str) -> Dict[str, Any]:
        """
        Delete all temporary files for a tenant.

        Args:
            tenant_id: The tenant identifier

        Returns:
            Dict with 'deleted_count' and 'deleted_keys'
        """
        try:
            deleted_count = 0
            deleted_keys = []

            # Query all temp files for tenant
            # Note: is_temp is stored as Number (1 for true, 0 for false) in DynamoDB index
            response = self.table.query(
                KeyConditionExpression=Key('tenant_id').eq(tenant_id),
                FilterExpression=Attr('is_temp').eq(1),
                ProjectionExpression='file_key'
            )

            # Delete in batches using batch_writer
            with self.table.batch_writer() as batch:
                for item in response.get('Items', []):
                    file_key = item['file_key']
                    batch.delete_item(Key={
                        'tenant_id': tenant_id,
                        'file_key': file_key
                    })
                    deleted_keys.append(file_key)
                    deleted_count += 1

            # Handle pagination
            last_key = response.get('LastEvaluatedKey')
            while last_key:
                response = self.table.query(
                    KeyConditionExpression=Key('tenant_id').eq(tenant_id),
                    FilterExpression=Attr('is_temp').eq(1),  # Number 1 for true
                    ProjectionExpression='file_key',
                    ExclusiveStartKey=last_key
                )

                with self.table.batch_writer() as batch:
                    for item in response.get('Items', []):
                        file_key = item['file_key']
                        batch.delete_item(Key={
                            'tenant_id': tenant_id,
                            'file_key': file_key
                        })
                        deleted_keys.append(file_key)
                        deleted_count += 1

                last_key = response.get('LastEvaluatedKey')

            logger.info(f"Deleted {deleted_count} temp files for tenant {tenant_id}")
            return {
                'deleted_count': deleted_count,
                'deleted_keys': deleted_keys
            }

        except ClientError as e:
            logger.error(f"Error deleting temp files: {e}", exc_info=True)
            raise

    def get_folders(self, tenant_id: str) -> List[str]:
        """
        Get list of unique folders for a tenant.

        Args:
            tenant_id: The tenant identifier

        Returns:
            List of unique folder names
        """
        try:
            response = self.table.query(
                KeyConditionExpression=Key('tenant_id').eq(tenant_id),
                ProjectionExpression='folder'
            )

            folders = set()
            for item in response.get('Items', []):
                folder = item.get('folder')
                if folder:
                    folders.add(folder)

            # Handle pagination
            last_key = response.get('LastEvaluatedKey')
            while last_key:
                response = self.table.query(
                    KeyConditionExpression=Key('tenant_id').eq(tenant_id),
                    ProjectionExpression='folder',
                    ExclusiveStartKey=last_key
                )
                
                for item in response.get('Items', []):
                    folder = item.get('folder')
                    if folder:
                        folders.add(folder)
                
                last_key = response.get('LastEvaluatedKey')

            return sorted(list(folders))

        except ClientError as e:
            logger.error(f"Error getting folders: {e}", exc_info=True)
            raise

