import os
from typing import Dict, Optional

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from personalize_commons.constants.app_constants import AppConstants


class ItemRepository:
    def __init__(self, resource):
        self.resource = resource
        self.table_name = os.getenv('DYNAMODB_TABLE_ITEMS', 'Items')
        self.table = self.resource.Table(self.table_name)

    def get_item(self, tenant_id: str, product_id: str) -> Optional[Dict]:
        """
        Retrieve an item by tenant_id and product_id

        Args:
            tenant_id (str): The tenant identifier (partition key)
            product_id (str): The product identifier (sort key)

        Returns:
            Optional[Dict]: The item if found, None otherwise

        Raises:
            DynamoDBError: If there's an error accessing DynamoDB
        """
        try:
            response = self.table.get_item(
                Key={
                    AppConstants.TENANT_ID: tenant_id,
                    AppConstants.ITEM_ID: product_id
                }
            )
            return response.get('Item')
        except ClientError as e:
            raise e

    def add_item(self, item: Dict) -> Dict:
        """
        Add a new item to the database

        Args:
            item (Dict): The item to add, must contain 'tenant_id' and 'product_id'

        Returns:
            Dict: The added item

        Raises:
            ValueError: If required fields are missing
            DynamoDBError: If there's an error accessing DynamoDB
        """
        if not all(k in item for k in ('tenant_id', 'product_id')):
            raise ValueError("Item must contain 'tenant_id' and 'product_id'")

        try:
            self.table.put_item(Item=item)
            return item
        except ClientError as e:
            raise e

    def batch_add_items(self, items: list) -> None:
        """
        Add multiple items in a batch

        Args:
            items (list): List of items to add

        Raises:
            DynamoDBError: If there's an error accessing DynamoDB
        """
        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    if not all(k in item for k in (AppConstants.TENANT_ID, AppConstants.ITEM_ID)):
                        continue  # Skip invalid items
                    batch.put_item(Item=item)
        except ClientError as e:
            raise e

    def query_items_by_tenant(self, tenant_id: str) -> list:
        """
        Query all items for a specific tenant

        Args:
            tenant_id (str): The tenant identifier

        Returns:
            list: List of items for the tenant

        Raises:
            DynamoDBError: If there's an error accessing DynamoDB
        """
        try:
            response = self.table.query(
                KeyConditionExpression=Key('tenant_id').eq(tenant_id)
            )
            return response.get('Items', [])
        except ClientError as e:
            raise e
