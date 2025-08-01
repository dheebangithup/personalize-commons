import pandas as pd

from personalize_commons.constants.app_constants import AppConstants


class RecombeeUtil:

    @staticmethod
    def infer_property_type( series: pd.Series) -> str:
        if pd.api.types.is_datetime64_any_dtype(series) or pd.api.types.is_timedelta64_dtype(series):
            return "timestamp"
        # Integer
        if pd.api.types.is_integer_dtype(series):
            return "int"
        if pd.api.types.is_numeric_dtype(series):
            return "double"
        elif pd.api.types.is_bool_dtype(series):
            return "boolean"
        return "string"

    @staticmethod
    def infer_property_type( dtype:str) -> str:
        if dtype=='datetime':
            return "timestamp"
        # Integer
        if dtype=='int':
            return "int"
        if dtype=='float':
            return "double"
        if dtype=='boolean':
            return "boolean"
        return "string"