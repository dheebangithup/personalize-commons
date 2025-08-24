import os

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.constants.db_constants import DBConstants
from personalize_commons.utils.datetime_utils import ist_now


class InteractionTrackingRepository:
    def __init__(self,  client):
        import boto3
        self.dynamodb = client
        self.table_name=os.getenv('INTRACTION_TRACKING_REPOSITORY_TABLE','intraction_tracking')


    def update_interactions(self, tenant_id: str, event_increments: dict, month: str = None):
        from datetime import datetime
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
