import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from botocore.exceptions import ClientError

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.constants.db_constants import DBConstants
from personalize_commons.entity.recommendation_entity import RecommendationEntity
from personalize_commons.utils.datetime_utils import ist_now_iso

logger = logging.getLogger(__name__)


class RecommendationRepository:
    """Repository class for handling RecommendationEntity CRUD operations with DynamoDB."""

    def __init__(self, resource):
        """Initialize the repository with DynamoDB connection."""
        self.resource = resource
        self.table_name = os.getenv('DYNAMODB_TABLE_RECOMMENDATIONS', 'recommendations')
        self.table = self.resource.Table(self.table_name)

    def create_recommendation(self, recommendation: RecommendationEntity) -> RecommendationEntity:
        """
        Create a new recommendation record in DynamoDB.

        Args:
            recommendation: The recommendation entity to create

        Returns:
            The created recommendation entity

        Raises:
            ValueError: If required fields are missing
            Exception: For DynamoDB errors
        """
        try:
            if not recommendation.tenant_id or not recommendation.recommendation_id:
                raise ValueError("tenant_id and recommendation_id are required")

            # Ensure we're not overwriting existing record
            existing = self.get_recommendation(
                tenant_id=recommendation.tenant_id,
                recommendation_id=recommendation.recommendation_id
            )
            if existing:
                raise ValueError(f"Recommendation with ID {recommendation.recommendation_id} already exists")

            # Convert to DynamoDB item and save
            item = recommendation.to_dynamodb_item()
            self.table.put_item(Item=item)
            logger.info(
                f"Created recommendation {recommendation.recommendation_id} for tenant {recommendation.tenant_id}")
            return recommendation

        except ClientError as e:
            error_msg = f"DynamoDB error creating recommendation: {e.response['Error']['Message']}"
            logger.error(error_msg, exc_info=True)
            raise Exception(error_msg) from e
        except Exception as e:
            logger.error(f"Unexpected error creating recommendation: {str(e)}", exc_info=True)
            raise

    def update_recommendation(self, recommendation_id: str, tenant_id: str, update_data: Dict[str, Any]) -> Optional[
        RecommendationEntity]:
        """
        Update a recommendation with the provided data.
        
        Args:
            recommendation_id: The ID of the recommendation to update
            update_data: Dictionary of fields to update
            
        Returns:
            The updated RecommendationEntity if successful, None if not found
            
        Raises:
            ValueError: If required fields are missing or invalid
            Exception: For DynamoDB errors
            :param tenant_id:
        """
        try:
            # First get the existing recommendation to merge with updates
            existing = self.get_recommendation(
                tenant_id=tenant_id,
                recommendation_id=recommendation_id
            )
            if not existing:
                return None

            # Convert entity to dict, update with new values, and back to entity
            update_dict = existing.model_dump()

            # Remove updated_at from update_data if it exists to prevent duplicate
            update_data.pop('updated_at', None)

            # Update with new values
            update_dict.update({
                k: v for k, v in update_data.items()
                if v is not None and k in update_dict
            })

            # Ensure tenant_id remains unchanged
            if AppConstants.TENANT_ID in update_dict and update_dict[AppConstants.TENANT_ID] != tenant_id:
                raise ValueError("Cannot change tenant_id of a recommendation")

            # Always set updated_at to current time
            update_dict['updated_at'] = ist_now_iso()

            # Convert back to entity to validate
            updated_entity = RecommendationEntity(**update_dict)

            # Prepare update expression
            update_expression = 'SET ' + ', '.join(
                f'#{k} = :{k}' for k in update_dict.keys() if k != 'tenant_id' and k != 'recommendation_id')
            expression_attribute_names = {f'#{k}': k for k in update_dict.keys() if
                                          k != 'tenant_id' and k != 'recommendation_id'}

            # Convert any datetime objects to ISO format strings
            expression_attribute_values = {}
            for k, v in update_dict.items():
                if k == 'tenant_id' or k == 'recommendation_id':
                    continue
                if isinstance(v, datetime):
                    expression_attribute_values[f':{k}'] = v.isoformat()
                else:
                    expression_attribute_values[f':{k}'] = v

            # Execute update
            self.table.update_item(
                Key={
                    'tenant_id': tenant_id,
                    'recommendation_id': recommendation_id
                },
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues='ALL_NEW'
            )

            return updated_entity

        except ClientError as e:
            error_msg = f"DynamoDB error updating recommendation: {e.response['Error']['Message']}"
            logger.error(error_msg, exc_info=True)
            raise Exception(error_msg) from e
        except Exception as e:
            logger.error(f"Unexpected error updating recommendation: {str(e)}", exc_info=True)
            raise

    def get_recommendation(self, tenant_id: str, recommendation_id: str) -> Optional[RecommendationEntity]:
        """
        Get a recommendation by tenant_id and recommendation_id.

        Args:
            tenant_id: The tenant ID
            recommendation_id: The recommendation ID

        Returns:
            The recommendation entity if found, None otherwise

        Raises:
            Exception: For DynamoDB errors
        """
        try:
            if not tenant_id or not recommendation_id:
                raise ValueError("Both tenant_id and recommendation_id are required")

            response = self.table.get_item(
                Key={
                    'tenant_id': tenant_id,
                    'recommendation_id': recommendation_id
                }
            )

            item = response.get('Item')
            if not item:
                return None

            return RecommendationEntity.from_dynamodb_item(item)

        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return None
            error_msg = f"DynamoDB error getting recommendation: {e.response['Error']['Message']}"
            logger.error(error_msg, exc_info=True)
            raise Exception(error_msg) from e
        except Exception as e:
            logger.error(f"Unexpected error getting recommendation: {str(e)}", exc_info=True)
            raise

    def get_recommendations(
            self,
            tenant_id: str,
            status: str = None,
            start_date: datetime = None,
            end_date: datetime = None,
            page_size: int = 10,
            last_evaluated_key: dict = None,
            sort_order: str = "desc"
    ) -> Dict[str, Any]:
        """
        Get recommendations by status and/or date range using appropriate LSI.
        Uses StatusIndex when filtering by status, otherwise uses CreatedAtIndex.
        Ensures each page has up to `page_size` filtered items.
        """
        try:
            # Base key condition
            key_condition = 'tenant_id = :tenant_id'
            expr_attr_values = {':tenant_id': tenant_id}
            expr_attr_names = {}
            filter_expression = []

            index_name = None
            if status and not (start_date or end_date):
                index_name = DBConstants.STAUS_AT_INDEX
                key_condition += ' AND #status = :status'
                expr_attr_names['#status'] = 'status'
                expr_attr_values[':status'] = status
            else:
                index_name = DBConstants.CREATED_AT_INDEX
                if start_date and end_date:
                    end_date = end_date + timedelta(days=1)
                    key_condition += ' AND #created_at BETWEEN :start_date AND :end_date'
                    expr_attr_names['#created_at'] = 'created_at'
                    expr_attr_values[':start_date'] = start_date.isoformat()
                    expr_attr_values[':end_date'] = end_date.isoformat()
                if status:
                    filter_expression.append('#status = :status')
                    expr_attr_names['#status'] = 'status'
                    expr_attr_values[':status'] = status

            query_params = {
                'IndexName': index_name,
                'KeyConditionExpression': key_condition,
                'ExpressionAttributeValues': expr_attr_values,
                'ScanIndexForward': sort_order.lower() == 'asc',
                'Limit': page_size * 2  # over-fetch a little to reduce round trips
            }
            if expr_attr_names:
                query_params['ExpressionAttributeNames'] = expr_attr_names
            if filter_expression:
                query_params['FilterExpression'] = ' AND '.join(filter_expression)
            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key

            items = []
            response = {}
            while len(items) < page_size:
                response = self.table.query(**query_params)
                new_items = [RecommendationEntity.from_dynamodb_item(item) for item in response.get('Items', [])]
                items.extend(new_items)

                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key or len(items) >= page_size:
                    break

                # Continue scanning with next key if page not full
                query_params['ExclusiveStartKey'] = last_evaluated_key

            return {
                'items': items[:page_size],
                'last_evaluated_key': last_evaluated_key,
                'has_more': bool(last_evaluated_key)
            }

        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}", exc_info=True)
            raise

    def get_recommendations_by_date_range(
            self,
            tenant_id: str,
            start_date: datetime,
            end_date: datetime,
            status: str = None
    ) -> List[RecommendationEntity]:
        """
        Get all recommendations within a date range, optionally filtered by status.

        Args:
            tenant_id: ID of the tenant
            start_date: Start date for filtering (inclusive)
            end_date: End date for filtering (inclusive)
            status: Optional status to filter recommendations

        Returns:
            List of RecommendationEntity objects within the date range

        Raises:
            Exception: For DynamoDB errors
        """
        try:
            # Convert datetime objects to ISO format strings
            end_date = end_date + timedelta(days=1)
            start_iso = start_date.isoformat()
            end_iso = end_date.isoformat()

            expr_attr_names = {
                '#created_at': 'created_at'
            }

            expr_attr_values = {
                ':tenant_id': tenant_id,
                ':start_date': start_iso,
                ':end_date': end_iso
            }

            query_params = {
                'TableName': self.table.name,
                'IndexName': DBConstants.CREATED_AT_INDEX,
                'KeyConditionExpression': 'tenant_id = :tenant_id AND #created_at BETWEEN :start_date AND :end_date',
                'ExpressionAttributeNames': expr_attr_names,
                'ExpressionAttributeValues': expr_attr_values,
                'ScanIndexForward': False
            }

            # Add status filter if provided
            if status:
                query_params['FilterExpression'] = '#status = :status'
                query_params['ExpressionAttributeNames']['#status'] = 'status'
                query_params['ExpressionAttributeValues'][':status'] = status

            items = []
            last_evaluated_key = None

            while True:
                if last_evaluated_key:
                    query_params['ExclusiveStartKey'] = last_evaluated_key

                response = self.table.query(**query_params)
                items.extend(response.get('Items', []))

                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break

            return [RecommendationEntity.from_dynamodb_item(item) for item in items]

        except Exception as e:
            logging.error(f"Error getting recommendations by date range: {str(e)}", exc_info=True)
            raise
