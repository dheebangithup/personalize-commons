import logging
import os
from typing import Dict, List, Any, Optional

from boto3.dynamodb.types import TypeDeserializer

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.model.user_model import QueryResponse

logger = logging.getLogger(__name__)
deserializer = TypeDeserializer()


class UserRepository:

    def __init__(self, client):
        self.dynamodb_client = client
        self.table_name = os.getenv("DYNAMODB_TABLE_USERS")
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

    def query_users(self, conditions: Dict[str, Any],tenant_id:str) -> QueryResponse:
        """
        Query users with flexible conditions using PartiQL

        Args:
            conditions: Dictionary of field conditions with operators

        Returns:
            Dictionary containing query results and metadata
            :param conditions:
            :param tenant_id:
        """
        try:
            if not conditions:
                raise ValueError("At least one condition is required")
                # Always include tenant_id in the query
            if AppConstants.TENANT_ID not in conditions:
                conditions[AppConstants.TENANT_ID] = tenant_id

            # Build WHERE clause with proper parameter placeholders
            where_parts = []
            params = []

            for field, condition in conditions.items():
                # Handle both ConditionValue objects and raw values
                if hasattr(condition, 'dict'):  # It's a Pydantic model
                    condition = condition.dict()

                # If it's a dict but not from Pydantic
                if isinstance(condition, dict):
                    operator = condition.get('operator', '=')
                    value = condition.get('value')
                    value2 = condition.get('value2')
                else:
                    operator = '='
                    value = condition
                    value2 = None

                operator = operator.lower()

                if operator in ("=", "!=", "<", "<=", ">", ">="):
                    where_parts.append(f"{field} {operator} ?")
                    params.append(self._convert_to_dynamodb_type(value))

                elif operator == "begins_with":
                    where_parts.append(f"begins_with({field}, ?)")
                    params.append(self._convert_to_dynamodb_type(value))

                elif operator == "contains":
                    where_parts.append(f"contains({field}, ?)")
                    params.append(self._convert_to_dynamodb_type(value))

                elif operator == "not_contains":
                    where_parts.append(f"NOT contains({field}, ?)")
                    params.append(self._convert_to_dynamodb_type(value))

                elif operator == "between" and value2 is not None:
                    where_parts.append(f"{field} BETWEEN ? AND ?")
                    params.extend([
                        self._convert_to_dynamodb_type(value),
                        self._convert_to_dynamodb_type(value2)
                    ])

                elif operator == "in" and isinstance(value, (list, tuple)):
                    placeholders = ", ".join(["?" for _ in value])
                    where_parts.append(f"{field} IN ({placeholders})")
                    params.extend([self._convert_to_dynamodb_type(v) for v in value])

                else:
                    # Default to equals if operator is not recognized
                    where_parts.append(f"{field} = ?")
                    params.append(self._convert_to_dynamodb_type(condition))

            if not where_parts:
                raise ValueError("No valid conditions provided")

            where_clause = " AND ".join(where_parts)
            query = f"SELECT * FROM {self.table_name} WHERE {where_clause}"

            logger.info(f"Executing query: {query}")
            logger.info(f"Query parameters: {params}")

            items = self.execute_partiql(query, params)

            return QueryResponse(users=items, count=len(items))

        except Exception as e:
            logger.error(f"Error querying users: {str(e)}", exc_info=True)
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

    def add_user(self, item:dict[str,Any]) -> None:
        """Add a new user to the DynamoDB table."""
        try:

            self.dynamodb_client.put_item(TableName=self.table_name, Item=item)
            logger.info(f"User {item[AppConstants.USER_ID]} added to table {self.table_name}.")
        except Exception as e:
            logger.error(f"Failed to add user {item[AppConstants.USER_ID]}: {str(e)}")
            raise
