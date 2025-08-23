import logging
import os
from typing import Dict, List, Any, Optional

from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.model.user_model import QueryResponse

logger = logging.getLogger(__name__)
deserializer = TypeDeserializer()


class UserRepository:

    def __init__(self, client,resource):
        self.resource = resource
        self.dynamodb_client = client
        self.table_name = os.getenv("DYNAMODB_TABLE_USERS")
        self.table = self.resource.Table(self.table_name)

        self._initialized = True
        logger.info(f"Initialized UserRepository with table: {self.table_name}")

    def execute_partiql(self, statement: str, parameters: Optional[List[Dict[str, Any]]] = None) -> List[
        Dict[str, Any]]:
        """Execute a PartiQL statement against DynamoDB"""
        try:
            request_params = {
                'Statement': statement
            }

            if parameters:
                request_params['Parameters'] = parameters

            logger.debug(f"Executing PartiQL: {statement}")
            logger.debug(f"With params: {parameters}")
            print("Request params:", request_params)
            print(statement)
            response = self.dynamodb_client.execute_statement(**request_params)
            # Deserialize the DynamoDB items to Python types
            items = []
            for item in response.get('Items', []):
                deserialized = {k: deserializer.deserialize(v) for k, v in item.items()}
                items.append(deserialized)

            return items

        except Exception as e:
            logger.error(f"Error executing PartiQL query: {str(e)}")
            logger.error(f"Statement: {statement}")
            logger.error(f"Parameters: {parameters}")
            raise


    def get_all_users_by_tenant(self, tenant_id: str) -> QueryResponse:
        """
        Retrieve all users for a specific tenant using a PartiQL query.

        Args:
            tenant_id: The ID of the tenant to retrieve users for

        Returns:
            QueryResponse containing the list of users and count
        """
        try:
            query = f"""
                SELECT * FROM {self.table_name} 
                WHERE tenant_id = ?
            """
            
            # Convert tenant_id to DynamoDB type format
            params = [self._convert_to_dynamodb_type(tenant_id)]
            
            logger.info(f"Fetching all users for tenant: {tenant_id}")
            items = self.execute_partiql(query, params)
            
            return QueryResponse(users=items, count=len(items))
            
        except Exception as e:
            logger.error(f"Error retrieving users for tenant {tenant_id}: {str(e)}", exc_info=True)
            raise

    def _convert_to_dynamodb_type(self, value: Any) -> Dict[str, Any]:
        """Convert Python value to DynamoDB attribute value format"""
        if value is None:
            return {'NULL': True}
        elif isinstance(value, bool):
            return {'BOOL': value}
        elif isinstance(value, (int, float)):
            return {'N': str(value)}
        elif isinstance(value, str):
            return {'S': value}
        elif isinstance(value, (list, tuple)):
            if not value:
                return {'L': []}
            # Check if all elements are of the same type
            first_type = type(value[0])
            if all(isinstance(x, first_type) for x in value):
                if first_type == str:
                    return {'SS': list(value)}
                elif first_type in (int, float):
                    return {'NS': [str(x) for x in value]}
            return {'L': [self._convert_to_dynamodb_type(x) for x in value]}
        elif isinstance(value, dict):
            return {'M': {k: self._convert_to_dynamodb_type(v) for k, v in value.items()}}
        else:
            return {'S': str(value)}


    def _convert_value_by_dtype(self,value: Any, dtype: str) -> Dict[str, Any]:
        """
        Convert a Python value to a DynamoDB PartiQL parameter based on dtype.

        Args:
            value: The actual value to convert.
            dtype: Data type as a string. Supported: "integer", "float", "string", "boolean"

        Returns:
            Dict[str, Any]: DynamoDB PartiQL parameter dictionary.
        """
        dtype = dtype.lower()

        if dtype in ("integer", "int"):
            return {'N': str(int(value))}
        elif dtype in ("float", "double"):
            return {'N': str(float(value))}
        elif dtype == "string":
            return {'S': str(value)}
        elif dtype in ("bool", "boolean"):
            return {'BOOL': bool(value)}
        else:
            raise ValueError(f"Unsupported dtype: {dtype}")

    def add_user(self, item:dict[str,Any]) -> None:
        """Add a new user to the DynamoDB table."""
        try:

            self.dynamodb_client.put_item(TableName=self.table_name, Item=item)
            logger.info(f"User {item[AppConstants.USER_ID]} added to table {self.table_name}.")
        except Exception as e:
            logger.error(f"Failed to add user {item[AppConstants.USER_ID]}: {str(e)}")
            raise

    def query_users_by_tenant(self, tenant_id: str) -> list:
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
                KeyConditionExpression=Key(AppConstants.TENANT_ID).eq(tenant_id)
            )
            return response.get('Items', [])
        except ClientError as e:
            raise e

    def _build_partiql_query(self, rules: Dict[str, Any]) -> str:
        """
        Recursively build a DynamoDB PartiQL WHERE clause from nested rules JSON.

        Args:
            rules (Dict[str, Any]): Nested JSON filter rules.

        Returns:
            str: Fully constructed PartiQL WHERE clause string.
        """

        def typecast_value(value: Any, dtype: str, for_in: bool = False) -> str:
            """Typecast Python value to PartiQL literal."""
            dtype = dtype.lower()

            if isinstance(value, list):
                # Format list properly: ('a','b','c')
                formatted_list = ", ".join(typecast_value(v, dtype, for_in=True) for v in value)
                return f"({formatted_list})"

            if dtype in ("string",):
                return f"'{value}'"
            elif dtype in ("int", "integer"):
                return str(int(value))
            elif dtype in ("float", "double"):
                # avoid unnecessary .0 for integers
                return str(int(value)) if float(value).is_integer() else str(float(value))
            else:
                raise ValueError(f"Unsupported dtype: {dtype}")

        def map_operator(op: str) -> str:
            """Map JSON operator to PartiQL operator."""
            mapping = {
                "==": "=",
                "!=": "<>",
                ">=": ">=",
                "<=": "<=",
                ">": ">",
                "<": "<",
                "in": "IN",
                "not in": "NOT IN"
            }
            return mapping.get(op.lower(), op)

        def process_rule(rule: Dict[str, Any]) -> str:
            if "op" in rule and "rules" in rule:
                # Nested group
                subclauses = [process_rule(r) for r in rule["rules"]]
                return "(" + f" {rule['op'].upper()} ".join(subclauses) + ")"
            else:
                # Simple condition
                field = rule["field_name"]
                operator = map_operator(rule["operator"])
                value = rule["value"]
                dtype = rule["dtype"]

                if operator == "IN":
                    return f"{field} IN {typecast_value(value, dtype)}"
                else:
                    return f"{field} {operator} {typecast_value(value, dtype)}"

        return process_rule(rules)

    def query_with_rules(self, rules: Dict[str, Any],tenant_id:str) -> QueryResponse:
        """
        Build and execute a PartiQL query from nested filter rules JSON.
        """
        where_clause = self._build_partiql_query(rules)
        statement = f"SELECT * FROM {self.table_name} WHERE tenant_id = '{tenant_id}' AND {where_clause}"
        logger.info(f"Generated PartiQL: {statement}")
        items = self.execute_partiql(statement)
        return QueryResponse(users=items, count=len(items))