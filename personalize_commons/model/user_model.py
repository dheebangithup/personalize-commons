from pydantic import BaseModel, Field, validator
from typing import Dict, Any, List, Optional, Union
from enum import Enum


class Operator(str, Enum):
    """Supported comparison operators"""
    EQUALS = "="
    NOT_EQUALS = "<>"
    LESS_THAN = "<"
    LESS_THAN_EQUAL = "<="
    GREATER_THAN = ">"
    GREATER_THAN_EQUAL = ">="
    BEGINS_WITH = "begins_with"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    BETWEEN = "between"
    IN = "in"


class ConditionValue(BaseModel):
    """Represents a condition with operator and value(s)"""
    operator: Operator = Operator.EQUALS
    value: Any
    value2: Optional[Any] = None  # Used for BETWEEN operator


class QueryRequest(BaseModel):
    """Request model for querying with flexible conditions"""
    conditions: Dict[str, Union[Any, ConditionValue]]  # Can be simple value or ConditionValue

    # @validator('conditions', pre=True)
    # def validate_conditions(cls, v):
    #     """Convert simple values to ConditionValue with EQUALS operator"""
    #     result = {}
    #     for field, condition in v.users():
    #         if isinstance(condition, dict):
    #             # Already a ConditionValue-like dict
    #             result[field] = ConditionValue(**condition)
    #         else:
    #             # Simple value, convert to ConditionValue with EQUALS
    #             result[field] = ConditionValue(operator=Operator.EQUALS, value=condition)
    #     return result

    def toItem(self) -> Dict[str, Any]:
        """Converts the request to a DynamoDB item"""
        return self.model_dump(by_alias=True)

    @classmethod
    def toEntity(cls, item: Dict[str, Any]) -> 'QueryRequest':
        """Converts a DynamoDB item to a QueryRequest instance"""
        return cls.model_validate(item)


class QueryResponse(BaseModel):
    """Response model for query results"""
    count: int
    users: Optional[List[Dict[str, Any]]]
    message: Optional[str] = None