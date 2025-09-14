import os
from typing import List, Optional, Dict, Any

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.entity.whatsapp_accounts import WhatsAppAccount, WhatsAppAccountBatch
import logging as logger

from personalize_commons.utils.datetime_utils import ist_now_iso


class WhatsAppAccountRepository:
    def __init__(self, resource):
        self.dynamodb = resource
        self.table = self.dynamodb.Table(os.getenv('DYNAMO_TABLE_WHATSAPP_ACCOUNTS'))

    def _get_dynamo_item(self, account: WhatsAppAccount) -> Dict[str, Any]:
        """Convert WhatsAppAccount model to DynamoDB item format"""
        item= {
            AppConstants.ACCOUNT_ID: account.account_id,
            AppConstants.TENANT_ID: account.tenant_id,
            'account_name': account.account_name,
            'phone_number_id': account.phone_number_id,
            'business_account_id': account.business_account_id,
            'accessTokenSecretArn': account.access_token_secret_arn,
            'is_active': account.is_active,
            'created_at': account.created_at.isoformat(),
            'updated_at': account.updated_at.isoformat()
        }
        # Add new token management fields if they exist
        if account.app_id:
            item['appId'] = account.app_id
        if account.app_secret_arn:
            item['appSecretArn'] = account.app_secret_arn
        if account.token_expires_at:
            item['tokenExpiresAt'] = account.token_expires_at.isoformat()
        if account.token_last_refreshed:
            item['tokenLastRefreshed'] = account.token_last_refreshed.isoformat()

        return item

    def create(self, account: WhatsAppAccount) -> WhatsAppAccount:
        """
        Create a new WhatsApp account
        """
        try:
            dynamo_item = self._get_dynamo_item(account)
            self.table.put_item(Item=dynamo_item)
            logger.info(f"Created WhatsApp account {account.account_id} for tenant {account.tenant_id}")
            return account
        except ClientError as e:
            logger.error(f"Failed to create WhatsApp account: {str(e)}")
            raise Exception(f"Failed to create account: {str(e)}")

    def get_by_id(self, account_id: str, tenant_id: str) -> Optional[WhatsAppAccount]:
        """
        Get a WhatsApp account by ID and tenant ID
        """
        try:
            response = self.table.get_item(
                Key={
                    AppConstants.ACCOUNT_ID: account_id,
                    AppConstants.TENANT_ID: tenant_id
                }
            )
            item = response.get('Item')
            if not item:
                logger.debug(f"Account {account_id} not found for tenant {tenant_id}")
                return None

            return WhatsAppAccount.from_dynamo_item(item)
        except ClientError as e:
            logger.error(f"Failed to get account {account_id}: {str(e)}")
            raise Exception(f"Failed to get account: {str(e)}")

    def get_by_phone_number(self, phone_number_id: str, tenant_id: str) -> Optional[WhatsAppAccount]:
        """
        Get a WhatsApp account by phone number ID and tenant ID
        """
        try:
            response = self.table.query(
                IndexName='PhoneNumberIndex',  # You'll need to create this GSI
                KeyConditionExpression=Key(AppConstants.TENANT_ID).eq(tenant_id) & Key('phone_number_id').eq(
                    phone_number_id),
                Limit=1
            )

            items = response.get('Items', [])
            if not items:
                logger.debug(f"Account with phone number {phone_number_id} not found for tenant {tenant_id}")
                return None

            return WhatsAppAccount.from_dynamo_item(items[0])
        except ClientError as e:
            logger.error(f"Failed to get account by phone number {phone_number_id}: {str(e)}")
            raise Exception(f"Failed to get account by phone number: {str(e)}")

    def get_all_by_tenant(self, tenant_id: str, active_only: bool = False) -> List[WhatsAppAccount]:
        """
        Get all WhatsApp accounts for a specific tenant
        """
        try:
            response = self.table.query(
                 IndexName='tenant_id_index',
                KeyConditionExpression=Key(AppConstants.TENANT_ID).eq(tenant_id)
            )

            items = response.get('Items', [])
            accounts = WhatsAppAccountBatch.from_dynamo_items(items)

            if active_only:
                accounts = [acc for acc in accounts if acc.is_active]

            logger.debug(f"Found {len(accounts)} accounts for tenant {tenant_id}")
            return accounts
        except ClientError as e:
            logger.error(f"Failed to get accounts for tenant {tenant_id}: {str(e)}")
            raise Exception(f"Failed to get accounts: {str(e)}")

    def update(self, account_id: str, tenant_id: str, update_data: Dict[str, Any]) -> Optional[WhatsAppAccount]:
        """
        Update WhatsApp account attributes - only updates specified fields
        """
        try:
            # Build update expression with only the specified fields
            update_expression_parts = []
            expression_attribute_values = {}
            expression_attribute_names = {}

            # Always update the updated_at field
            update_expression_parts.append("#updated_at = :updated_at")
            expression_attribute_values[':updated_at'] = ist_now_iso()
            expression_attribute_names['#updated_at'] = 'updated_at'

            # Map model field names to DynamoDB attribute names
            field_mapping = {
                'account_name': 'account_name',
                'phone_number_id': 'phone_number_id',
                'business_account_id': 'business_account_id',
                'access_token_secret_arn': 'accessTokenSecretArn',
                'is_active': 'is_active'
            }

            for field, value in update_data.items():
                if value is not None and field in field_mapping:
                    dynamo_field = field_mapping[field]
                    update_expression_parts.append(f"#{dynamo_field} = :{dynamo_field}")
                    expression_attribute_values[f":{dynamo_field}"] = value
                    expression_attribute_names[f"#{dynamo_field}"] = dynamo_field

            if len(update_expression_parts) == 1:  # Only updated_at was added
                return self.get_by_id(account_id, tenant_id)

            update_expression = "SET " + ", ".join(update_expression_parts)

            response = self.table.update_item(
                Key={
                    AppConstants.ACCOUNT_ID: account_id,
                    AppConstants.TENANT_ID: tenant_id
                },
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues='ALL_NEW'
            )

            updated_item = response.get('Attributes')
            if not updated_item:
                return None

            logger.info(f"Updated account {account_id} for tenant {tenant_id}")
            return WhatsAppAccount.from_dynamo_item(updated_item)
        except ClientError as e:
            logger.error(f"Failed to update account {account_id}: {str(e)}")
            raise Exception(f"Failed to update account: {str(e)}")

    def delete(self, account_id: str, tenant_id: str) -> bool:
        """
        Delete a WhatsApp account
        """
        try:
            self.table.delete_item(
                Key={
                    AppConstants.ACCOUNT_ID: account_id,
                    AppConstants.TENANT_ID: tenant_id
                }
            )
            logger.info(f"Deleted account {account_id} for tenant {tenant_id}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete account {account_id}: {str(e)}")
            raise Exception(f"Failed to delete account: {str(e)}")

    def activate(self, account_id: str, tenant_id: str) -> Optional[WhatsAppAccount]:
        """
        Activate a WhatsApp account - only updates is_active field
        """
        return self.update(account_id, tenant_id, {'is_active': True})

    def deactivate(self, account_id: str, tenant_id: str) -> Optional[WhatsAppAccount]:
        """
        Deactivate a WhatsApp account - only updates is_active field
        """
        return self.update(account_id, tenant_id, {'is_active': False})

    def update_account_name(self, account_id: str, tenant_id: str, new_name: str) -> Optional[WhatsAppAccount]:
        """
        Update only the account name
        """
        return self.update(account_id, tenant_id, {'account_name': new_name})

    def update_access_token(self, account_id: str, tenant_id: str, new_token_arn: str) -> Optional[WhatsAppAccount]:
        """
        Update only the access token ARN
        """
        return self.update(account_id, tenant_id, {'access_token_secret_arn': new_token_arn})

    def batch_create(self, accounts: List[WhatsAppAccount]) -> List[WhatsAppAccount]:
        """
        Create multiple WhatsApp accounts in batch
        """
        try:
            with self.table.batch_writer() as batch:
                for account in accounts:
                    dynamo_item = self._get_dynamo_item(account)
                    batch.put_item(Item=dynamo_item)

            logger.info(f"Batch created {len(accounts)} WhatsApp accounts")
            return accounts
        except ClientError as e:
            logger.error(f"Failed to batch create accounts: {str(e)}")
            raise Exception(f"Failed to batch create accounts: {str(e)}")

    def batch_delete(self, account_ids: List[str], tenant_id: str) -> int:
        """
        Delete multiple WhatsApp accounts in batch
        """
        try:
            with self.table.batch_writer() as batch:
                for account_id in account_ids:
                    batch.delete_item(
                        Key={
                            AppConstants.ACCOUNT_ID: account_id,
                            AppConstants.TENANT_ID: tenant_id
                        }
                    )

            logger.info(f"Batch deleted {len(account_ids)} WhatsApp accounts for tenant {tenant_id}")
            return len(account_ids)
        except ClientError as e:
            logger.error(f"Failed to batch delete accounts: {str(e)}")
            raise Exception(f"Failed to batch delete accounts: {str(e)}")

    def exists(self, account_id: str, tenant_id: str) -> bool:
        """
        Check if a WhatsApp account exists
        """
        try:
            response = self.table.get_item(
                Key={
                    AppConstants.ACCOUNT_ID: account_id,
                    AppConstants.TENANT_ID: tenant_id
                },
                ProjectionExpression=AppConstants.ACCOUNT_ID
            )
            return 'Item' in response
        except ClientError as e:
            logger.error(f"Failed to check account existence {account_id}: {str(e)}")
            raise Exception(f"Failed to check account existence: {str(e)}")

    def count_by_tenant(self, tenant_id: str, active_only: bool = False) -> int:
        """
        Count WhatsApp accounts for a tenant
        """
        try:
            accounts = self.get_all_by_tenant(tenant_id, active_only)
            return len(accounts)
        except Exception as e:
            logger.error(f"Failed to count accounts for tenant {tenant_id}: {str(e)}")
            raise Exception(f"Failed to count accounts: {str(e)}")


'''

# Initialize repository
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
repo = WhatsAppAccountRepository(dynamodb)

# Get all accounts for a tenant
accounts = repo.get_all_by_tenant("tenant-123", active_only=True)

# Update only account name
updated_account = repo.update_account_name("account-123", "tenant-123", "New Account Name")

# Update only access token
updated_account = repo.update_access_token("account-123", "tenant-123", "arn:new-token")

# Update multiple fields selectively
updated_account = repo.update("account-123", "tenant-123", {
    'account_name': 'Updated Name',
    'is_active': False
})
'''