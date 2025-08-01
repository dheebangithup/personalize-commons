import pandas as pd


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