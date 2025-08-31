import logging
import os
from typing import Optional
from botocore.exceptions import ClientError
import boto3

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.constants.db_constants import DBConstants
from personalize_commons.entity.interaction_tracking import InteractionTracking


class InteractionTrackingRepository:
    def __init__(self, dynamodb_client):
        self.table_name = os.getenv('DYNAMODB_TABLE_INTERACTION_TRACKING')
        self.client = dynamodb_client

    def get(self, tenant_id: str, month: str) -> Optional[InteractionTracking]:
        """
        Fetch interaction stats for a given tenant & month.
        Returns None if not found.
        """
        try:
            resp = self.client.get_item(
                TableName=self.table_name,
                Key={
                    AppConstants.TENANT_ID: {"S": tenant_id},
                    DBConstants.MONTH: {"S": month}
                }
            )

            if "Item" not in resp:
                logging.info(f"No interaction record for {tenant_id} - {month}")
                return None

            item = resp["Item"]
            interactions = {
                k: int(v["N"]) for k, v in item.get("interactions", {}).get("K",{}).items()
            } if "interactions" in item else {}

            return InteractionTracking(
                tenant_id=item["tenant_id"]["S"],
                month=item["month"]["S"],
                interactions=interactions,
                active_users=int(item.get("active_users", {"N": "0"})["N"])
            )

        except ClientError as e:
            logging.error(f"Error fetching interaction tracking: {e}")
            raise

    def save(self, entity: InteractionTracking) -> None:
        """
        Save (insert or overwrite) an InteractionTracking record.
        """
        try:
            # Convert interaction counts into DynamoDB number map
            interaction_map = {
                k: {"N": str(v)} for k, v in entity.interactions.items()
            }

            item = {
                "tenant_id": {"S": entity.tenant_id},
                "month": {"S": entity.month},
                "interactions": {"M": interaction_map},
                "active_users": {"N": str(entity.active_users)}
            }

            self.client.put_item(
                TableName=self.table_name,
                Item=item
            )

            logging.info(f"Saved interaction tracking for {entity.tenant_id} - {entity.month}")

        except ClientError as e:
            logging.error(f"Error saving interaction tracking: {e}")
            raise
