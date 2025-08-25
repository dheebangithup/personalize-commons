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
        update_expr = "SET "
        expr_attr_names = {}
        expr_attr_values = {}
        updates = []

        # Initialize interactions map if it doesn't exist
        if not event_increments:
            return {"ok": True}

        # Prepare the update expression for each event type
        for i, (event_type, value) in enumerate(event_increments.items()):
            event_alias = f"#e{i}"
            value_alias = f":v{i}"
            
            expr_attr_names[event_alias] = event_type
            expr_attr_values[value_alias] = {"N": str(value)}
            
            # Use if_not_exists to handle new attributes
            updates.append(f"interactions.{event_alias} = if_not_exists(interactions.{event_alias}, :zero) + {value_alias}")

        # Add zero value for if_not_exists
        expr_attr_values[":zero"] = {"N": "0"}
        
        # If no updates, return early
        if not updates:
            return {"ok": True}
            
        update_expr += ", ".join(updates)

        try:
            response = self.dynamodb.update_item(
                TableName=self.table_name,
                Key={
                    "tenant_id": {"S": str(tenant_id)},
                    "month": {"S": str(month)}
                },
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values,
                ReturnValues="UPDATED_NEW"
            )
            return response.get("Attributes", {})
            
        except self.dynamodb.exceptions.ConditionalCheckFailedException:
            # Item doesn't exist, create it with initial values
            try:
                initial_interactions = {k: {"N": str(v)} for k, v in event_increments.items()}
                
                response = self.dynamodb.put_item(
                    TableName=self.table_name,
                    Item={
                        "tenant_id": {"S": str(tenant_id)},
                        "month": {"S": str(month)},
                        "interactions": {"M": initial_interactions}
                    },
                    ReturnValues="NONE"
                )
                return {"ok": True, "created": True}
                
            except Exception as e:
                logging.error(f"Failed to create new item: {e}")
                return {"ok": False, "error": str(e)}
                
        except Exception as e:
            logging.error(f"Failed to update interactions: {e}")
            return {"ok": False, "error": str(e)}

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
