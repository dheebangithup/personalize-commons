import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from botocore.exceptions import ClientError

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.constants.db_constants import DBConstants
from personalize_commons.entity.campaign_entity import CampaignEntity
from personalize_commons.utils.datetime_utils import ist_now_iso

logger = logging.getLogger(__name__)


class CampaignRepository:
    def __init__(self, resource):
        self.resource = resource
        self.campaign_table = self.resource.Table(os.getenv('DYNAMODB_TABLE_CAMPAIGNS', 'campaigns'))

    def create_campaign(self, campaign: CampaignEntity) -> CampaignEntity:
        try:
            campaign.created_at = ist_now_iso()
            campaign.updated_at = ist_now_iso()
            item = campaign.to_dynamodb_item()
            self.campaign_table.put_item(Item=item)
            return CampaignEntity.from_dynamodb_item(item)
        except ClientError as e:
            print("Full error:", e)
            error_msg = f"Error creating campaign: {e.response['Error']['Message']}"
            print(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            print(e)
            raise Exception(e)

    def get_campaign(self, campaign_id: str, tenant_id: str) -> Optional[CampaignEntity]:
        try:
            response = self.campaign_table.get_item(
                Key={
                    AppConstants.TENANT_ID: tenant_id,
                    AppConstants.CAMPAIGN_ID: campaign_id
                }
            )
            item = response.get('Item')
            if item is None:
                return None
            return CampaignEntity.from_dynamodb_item(item)
        except ClientError as e:
            error_msg = f"Error getting campaign: {e.response['Error']['Message']}"
            print("Full error:", e.response)
            raise Exception(error_msg)

    def update_campaign(self, campaign_id: str, update_data: Dict[str, Any], tenant_id: str) -> Optional[
        CampaignEntity]:
        """
        Update a campaign by ID with the provided data.
        Returns the updated campaign if successful, None otherwise.
        """
        try:
            # First get the existing campaign to merge with updates
            existing = self.get_campaign(campaign_id, tenant_id)
            if not existing:
                return None

            # Convert entity to dict, update with new values, and back to entity
            update_dict = existing.model_dump()
            update_dict.update({
                k: v for k, v in update_data.items()
                if v is not None and k in update_dict
            })

            # Ensure tenant_id remains unchanged
            if AppConstants.TENANT_ID in update_dict and update_dict['tenant_id'] != tenant_id:
                raise ValueError("Cannot change tenant_id of a campaign")

            # Convert back to entity to validate
            updated_entity = CampaignEntity(**update_dict)

            # Prepare update expression
            update_expression = 'SET ' + ', '.join(f'#{k} = :{k}' for k in update_data.keys())
            expression_attribute_names = {f'#{k}': k for k in update_data.keys()}
            expression_attribute_values = {f':{k}': v for k, v in update_data.items()}

            # Add updated_at timestamp
            update_expression += ', #updated_at = :updated_at'
            expression_attribute_names['#updated_at'] = 'updated_at'
            expression_attribute_values[':updated_at'] = ist_now_iso()

            # Execute update
            self.campaign_table.update_item(
                Key={
                    'tenant_id': str(tenant_id),
                    'campaign_id': campaign_id
                },
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues='ALL_NEW'
            )

            return updated_entity

        except ClientError as e:
            logger.error(f"Error updating campaign: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating campaign: {str(e)}")
            raise

    def delete_campaign(self, campaign_id: str, tenant_id: str) -> bool:
        """
        Delete a campaign by ID.
        Returns True if the campaign was deleted, False otherwise.
        """
        try:
            response = self.campaign_table.delete_item(
                Key={
                    AppConstants.TENANT_ID: str(tenant_id),
                    AppConstants.CAMPAIGN_ID: campaign_id
                },
                ReturnValues='ALL_OLD'
            )
            # If Attributes is in the response, the item existed and was deleted
            return 'Attributes' in response
        except ClientError as e:
            logger.error(f"Error deleting campaign: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting campaign: {str(e)}")
            raise

    def get_campaigns_by_updated_at(
            self,
            tenant_id: str,
            start_date: datetime = None,
            end_date: datetime = None,
            status: str = None,
            page_size: int = 10,
            last_evaluated_key: dict = None,
            sort_order: str = "desc"
    ) -> dict:
        """
        Get campaigns by update date range and status using appropriate LSI.
        Uses StatusIndex when filtering by status, otherwise uses UpdatedAtIndex.

        Args:
            tenant_id: ID of the tenant
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            status: Optional status to filter campaigns
            page_size: Number of items per page
            last_evaluated_key: Pagination token from previous query
            sort_order: Sort order ('asc' or 'desc')

        Returns:
            Dictionary containing items and pagination info
        """
        try:
            # Base key condition
            key_condition = 'tenant_id = :tenant_id'
            expr_attr_values = {':tenant_id': tenant_id}
            expr_attr_names = {}
            filter_expression = []

            # Determine which index to use based on parameters
            index_name = None
            # This will exclude all records from July 15, unless they were created exactly at midnight (00:00:00) — which is very unlikely.
            if end_date is not None:
                end_date = end_date + timedelta(days=1)
            # If status is provided but no date range, use StatusIndex
            if status and not (start_date or end_date):
                index_name = DBConstants.STAUS_AT_INDEX
                key_condition += ' AND #status = :status'
                expr_attr_names['#status'] = 'status'
                expr_attr_values[':status'] = status



            # Otherwise, use UpdatedAtIndex (default)
            else:
                index_name = DBConstants.UPDATED_AT_INDEX
                # Add date range to key condition if provided
                if start_date and end_date:
                    key_condition += ' AND #updated_at BETWEEN :start_date AND :end_date'
                    expr_attr_names['#updated_at'] = 'updated_at'
                    expr_attr_values[':start_date'] = start_date.isoformat()
                    expr_attr_values[':end_date'] = end_date.isoformat()

                # Add status as filter if provided
                if status:
                    filter_expression.append('#status = :status')
                    expr_attr_names['#status'] = 'status'
                    expr_attr_values[':status'] = status

            # Build query parameters
            query_params = {
                'IndexName': index_name,
                'KeyConditionExpression': key_condition,
                'ExpressionAttributeValues': expr_attr_values,
                'Limit': page_size,
                'ScanIndexForward': sort_order.lower() == 'asc'
            }

            if expr_attr_names:
                query_params['ExpressionAttributeNames'] = expr_attr_names

            # Add filter expression if we have any filters
            if filter_expression:
                query_params['FilterExpression'] = ' AND '.join(filter_expression)

            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key

            # Execute query
            response = self.campaign_table.query(**query_params)

            # Convert items to entities
            items = [CampaignEntity.from_dynamodb_item(item) for item in response.get('Items', [])]

            return {
                AppConstants.ITEMS: items,
                AppConstants.LAST_EVAL_KEY: response.get('LastEvaluatedKey'),
                AppConstants.HAS_MORE: 'LastEvaluatedKey' in response
            }


        except Exception as e:
            logging.error(f"Error getting campaigns: {str(e)}")
            raise
