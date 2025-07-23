import os
from typing import List, Dict
import logging

from boto3.dynamodb.conditions import Key

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.constants.db_constants import DBConstants

logger = logging.getLogger(__name__)

class TenantRepository:

    def __init__(self,resource):
        self.resource = resource
        self.table_name = os.getenv('DYNAMODB_TABLE_TENANTS', 'tenants')
        self.table = self.resource.Table(self.table_name)


    def get_tenant(self, tenant_id: str) -> Dict[str, str]|None:
        try:
            response = self.table.query(
                KeyConditionExpression=AppConstants.TENANT_ID + f' = :{AppConstants.TENANT_ID}',
                ExpressionAttributeValues={
                    f':{AppConstants.TENANT_ID }': tenant_id
                }
            )
            data= response.get(AppConstants.DYNAMO_ITEMS,None)
            if data is None or  len(data) == 0:
                return None
            return data[0]
        except Exception as e:
            raise e


    def get_all_tenants(self) -> List[Dict]:
        try:
            response = self.table.scan()
            return response.get('Items', [])
        except Exception as e:
            raise e

    def create_tenant(self, tenant:dict[str, str]) -> dict[str, str]:
        try:

           response=  self.table.put_item(
                Item=tenant
            )
           logging.info(f"Created tenant {tenant['tenant_id']}")
           return tenant
        except Exception as e:
            raise e

    def update_tenant(self, tenant_id: str,email:str, update_data: dict) -> dict:
        """
        Update a tenant's information.
        
        Args:
            tenant_id: The ID of the tenant to update
            update_data: Dictionary containing the fields to update
            
        Returns:
            The updated tenant data
            
        Raises:
            Exception: If the update operation fails
        """
        try:
            # Build the update expression
            update_expression = 'SET '
            expression_attribute_values = {}
            expression_attribute_names = {}
            
            # Add each field from update_data to the update expression
            for i, (key, value) in enumerate(update_data.items()):
                if key != AppConstants.TENANT_ID:  # Prevent updating the tenant_id
                    placeholder = f'#{key}'
                    value_placeholder = f':val{i}'
                    update_expression += f"{placeholder} = {value_placeholder}, "
                    expression_attribute_names[placeholder] = key
                    expression_attribute_values[value_placeholder] = value
            
            # Remove the trailing comma and space
            update_expression = update_expression.rstrip(', ')
            
            response = self.table.update_item(
                Key={AppConstants.TENANT_ID: tenant_id,AppConstants.EMAIL:email},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues='ALL_NEW'
            )
            
            updated_tenant = response.get('Attributes', {})
            logging.info(f"Updated tenant {tenant_id}")
            return updated_tenant
            
        except Exception as e:
            logging.error(f"Error updating tenant {tenant_id}: {str(e)}")
            raise e

    def get_by_email(self, email: str):
        """
        Query tenant data using email (via GSI).
        Returns first match (or list if multi-tenant by email).
        """
        response = self.table.query(
            IndexName=DBConstants.EMAIL_INDEX,  # GSI name
            KeyConditionExpression=Key(AppConstants.EMAIL).eq(email)
        )

        items = response.get("Items", [])
        return items[0] if items else None
