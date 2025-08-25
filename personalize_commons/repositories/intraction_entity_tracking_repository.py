import logging
import os
from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.constants.db_constants import DBConstants
from personalize_commons.entity.intraction_tracking_entity import InteractionTrackingEntity
from personalize_commons.utils.datetime_utils import ist_now

class InteractionTrackingRepository:
    """
    Main aggregates table:
      - PK: tenant_id (S)
      - SK: month     (S, 'YYYY-MM', IST)
      Attributes:
        - interactions (M: str -> N)
        - unique_users (N)
    """
    def __init__(self, client):
        self.dynamodb = client
        self.table_name = os.getenv('INTERACTION_TRACKING_TABLE', 'interaction_tracking')

    def update_interactions(self, tenant_id: str, event_increments: dict, month: str = None):
        """
        Increments one or more interaction counters atomically.
        Example event_increments: {"purchase": 5, "add_to_cart": 2}
        Creates the row if not present.
        """
        if month is None:
            month = ist_now().strftime("%Y-%m")

        if not event_increments:
            return {"ok": True}

        update_parts = []
        expr_attr_names = {}
        expr_attr_values = {":zero": {"N": "0"}}

        for i, (event_type, value) in enumerate(event_increments.items()):
            placeholder_name = f"#e{i}"
            placeholder_value = f":v{i}"

            update_parts.append(
                f"interactions.{placeholder_name} = if_not_exists(interactions.{placeholder_name}, :zero) + {placeholder_value}"
            )
            expr_attr_names[placeholder_name] = event_type
            expr_attr_values[placeholder_value] = {"N": str(value)}

        update_expr = "SET " + ", ".join(update_parts)

        try:
            response = self.dynamodb.update_item(
                TableName=self.table_name,
                Key={
                    AppConstants.TENANT_ID: {"S": tenant_id},
                    DBConstants.MONTH: {"S": month}
                },
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values,
                ReturnValues="UPDATED_NEW"
            )
            return response.get("Attributes", {})
        except Exception as e:
            logging.error(f"Failed to update interactions: {e}")
            return {"ok": False, "error": str(e)}

    def increment_unique_users(self, tenant_id: str, month: str = None, by: int = 1):
        """
        Increments the unique_users counter. Safe to call after confirming first-time user.
        Creates the row if not present.
        """
        if month is None:
            month = ist_now().strftime("%Y-%m")

        response = self.dynamodb.update_item(
            TableName=self.table_name,
            Key={
                AppConstants.TENANT_ID: {"S": tenant_id},
                DBConstants.MONTH: {"S": month}
            },
            UpdateExpression="ADD unique_users :inc",
            ExpressionAttributeValues={":inc": {"N": str(by)}},
            ReturnValues="UPDATED_NEW"
        )
        return response.get("Attributes", {})

    def get_interactions(self, tenant_id: str, month: str = None) -> InteractionTrackingEntity:
        """
        Returns a Pydantic entity (no enum conversion; keys remain strings).
        """
        if month is None:
            month = ist_now().strftime("%Y-%m")

        response = self.dynamodb.get_item(
            TableName=self.table_name,
            Key={
                AppConstants.TENANT_ID: {"S": tenant_id},
                DBConstants.MONTH: {"S": month}
            }
        )
        item = response.get("Item")
        if not item:
            return InteractionTrackingEntity(tenant_id=tenant_id, month=month)

        interactions = {}
        if "interactions" in item:
            interactions = {k: int(v["N"]) for k, v in item["interactions"]["M"].items()}

        unique_users = int(item.get("unique_users", {}).get("N", "0"))

        return InteractionTrackingEntity(
            tenant_id=tenant_id,
            month=month,
            interactions=interactions,
            unique_users=unique_users
        )
