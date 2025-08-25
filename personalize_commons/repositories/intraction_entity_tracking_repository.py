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
        '''
        usage
        repo = InteractionRepository(table_name="Interactions")

        # First-time insert or increment
        repo.update_interactions("tenant123", {"purchase": 10})

        # Increment multiple events
        repo.update_interactions("tenant123", {"purchase": 5, "add_to_cart": 3})

        # Fetch record
        record = repo.get_interactions("tenant123", "2025-08")
        '''
        if month is None:
            month = ist_now().strftime("%Y-%m")  # use IST timezone

        # Build UpdateExpression dynamically
        update_expr = "ADD "
        expr_attr_names = {}
        expr_attr_values = {}
        updates = []

        for i, (event_type, value) in enumerate(event_increments.items()):
            placeholder_name = f"#e{i}"
            placeholder_value = f":v{i}"
            updates.append(f"interactions.{placeholder_name} {placeholder_value}")
            expr_attr_names[placeholder_name] = event_type
            expr_attr_values[placeholder_value] = {"N": str(value)}

        update_expr += ", ".join(updates)

        response = self.dynamodb.update_item(
            TableName=self.table_name,
            Key={AppConstants.TENANT_ID: {"S": tenant_id}, f"{DBConstants.MONTH}": {"S": month}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
            ReturnValues="UPDATED_NEW"
        )

        return response["Attributes"]

    def get_interactions(self, tenant_id: str, month: str = None) -> InteractionTrackingEntity:
        """
        Retrieve interaction record as a Pydantic entity.
        """
        if month is None:
            month = ist_now().strftime("%Y-%m")

        response = self.dynamodb.get_item(
            TableName=self.table_name,
            Key={
                "tenant_id": {"S": tenant_id},
                "month": {"S": month}
            }
        )
        item = response.get("Item")
        if not item or "interactions" not in item:
            return InteractionTrackingEntity(tenant_id=tenant_id, month=month)

        # Convert DynamoDB map to simple dict
        interactions = {k: int(v["N"]) for k, v in item["interactions"]["M"].items()}

        return InteractionTrackingEntity(
            tenant_id=tenant_id,
            month=month,
            interactions=interactions
        )
