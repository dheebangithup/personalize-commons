import pytest

from personalize_commons.constants.app_constants import AppConstants
from personalize_commons.utils.reql_filter_compiler import ReQLFilterCompiler, ReQLCompilationError


@pytest.fixture
def schema():
    return {
        'category': 'string',
        'brand': 'string',
        'price': 'float',
        'rating': 'float',
        'published_at': 'datetime',
        'is_available': 'boolean',
        'itemId': 'int'
    }

@pytest.fixture
def compiler(schema):
    return ReQLFilterCompiler(schema)

# ----------------------------
# extract_fields Test Cases
# ----------------------------

def test_extract_basic_fields(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {AppConstants.FIELD_NAME: "category", "operator": "==", "value": "Books", "dtype": "string"},
            {AppConstants.FIELD_NAME: "price", "operator": "<=", "value": 500, "dtype": "float"}
        ]
    }
    expected = {'category': 'string', 'price': 'float'}
    assert compiler.extract_fields(dsl) == expected

def test_extract_nested_groups(compiler):
    dsl = {
        "op": "OR",
        "rules": [
            {
                "op": "AND",
                "rules": [
                    {AppConstants.FIELD_NAME: "category", "operator": "==", "value": "Books", "dtype": "string"},
                    {AppConstants.FIELD_NAME: "brand", "operator": "==", "value": "Nike", "dtype": "string"}
                ]
            },
            {AppConstants.FIELD_NAME: "price", "operator": "<=", "value": 100, "dtype": "float"}
        ]
    }
    expected = {'category': 'string', 'brand': 'string', 'price': 'float'}
    assert compiler.extract_fields(dsl) == expected

def test_extract_duplicate_fields(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {AppConstants.FIELD_NAME: "price", "operator": ">", "value": 100, "dtype": "float"},
            {AppConstants.FIELD_NAME: "price", "operator": "<", "value": 500, "dtype": "int"}  # Should keep first dtype
        ]
    }
    expected = {'price': 'float'}  # First occurrence wins
    assert compiler.extract_fields(dsl) == expected

def test_extract_empty_rules(compiler):
    dsl = {"op": "AND", "rules": []}
    assert compiler.extract_fields(dsl) == {}

def test_extract_missing_dtype_uses_schema(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {AppConstants.FIELD_NAME: "category", "operator": "==", "value": "Books"},  # Missing dtype
            {AppConstants.FIELD_NAME: "price", "operator": "<=", "value": 500, "dtype": "float"}
        ]
    }
    expected = {'category': 'string', 'price': 'float'}  # Gets 'string' from schema
    assert compiler.extract_fields(dsl) == expected

def test_extract_with_context_item(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {AppConstants.FIELD_NAME: "brand", "operator": "==", "value": {"$context_item": "brand"}, "dtype": "string"},
            {AppConstants.FIELD_NAME: "price", "operator": "<=", "value": 100, "dtype": "float"}
        ]
    }
    expected = {'brand': 'string', 'price': 'float'}
    assert compiler.extract_fields(dsl) == expected

def test_extract_deeply_nested(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {AppConstants.FIELD_NAME: "category", "operator": "==", "value": "Books", "dtype": "string"},
            {
                "op": "OR",
                "rules": [
                    {AppConstants.FIELD_NAME: "brand", "operator": "==", "value": "Nike", "dtype": "string"},
                    {
                        "op": "AND",
                        "rules": [
                            {AppConstants.FIELD_NAME: "price", "operator": ">=", "value": 100, "dtype": "float"},
                            {AppConstants.FIELD_NAME: "rating", "operator": ">=", "value": 4, "dtype": "float"}
                        ]
                    }
                ]
            }
        ]
    }
    expected = {
        'category': 'string',
        'brand': 'string',
        'price': 'float',
        'rating': 'float'
    }
    assert compiler.extract_fields(dsl) == expected

# ----------------------------
# Negative Test Cases
# ----------------------------

def test_extract_invalid_structure(compiler):
    dsl = {"op": "AND", "rules": "not_a_list"}  # Invalid structure
    with pytest.raises(ReQLCompilationError):
        compiler.extract_fields(dsl)

def test_extract_non_dict_node(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {AppConstants.FIELD_NAME: "valid", "operator": "==", "value": "X", "dtype": "string"},
            "invalid_rule"  # Non-dict in rules
        ]
    }
    expected = {'valid': 'string'}  # Should skip invalid rule
    assert compiler.extract_fields(dsl) == expected



def test_extract_missing_field_name(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {"operator": "==", "value": "X", "dtype": "string"}  # Missing field
        ]
    }
    assert compiler.extract_fields(dsl) == {}  # Should skip invalid rule

def test_extract_empty_input(compiler):
    assert compiler.extract_fields({}) == {}


# ----------------------------

# ----------------------------
# validate_extracted_fields Test Cases
# ----------------------------

def test_validate_matching_fields(compiler):
    """Test validation passes with matching fields and compatible types"""
    extracted = {'price': 'float', 'category': 'string'}
    # Uses schema from fixture which includes these fields
    compiler.validate_extracted_fields(extracted)  # Should not raise


def test_validate_missing_field(compiler):
    """Test detection of fields missing in schema"""
    extracted = {'price': 'float', 'invalid_field': 'string'}
    with pytest.raises(ReQLCompilationError) as excinfo:
        compiler.validate_extracted_fields(extracted)
    assert "Missing fields in schema: invalid_field" in str(excinfo.value)


def test_validate_type_mismatch(compiler):
    """Test detection of incompatible types"""
    extracted = {'price': 'int', 'category': 'string'}  # price should be float
    with pytest.raises(ReQLCompilationError) as excinfo:
        compiler.validate_extracted_fields(extracted)
    assert "Type mismatches: price (filter:int vs schema:float)" in str(excinfo.value)


def test_validate_compatible_types(compiler):
    """Test type compatibility aliases"""
    extracted = {
        'price': 'float',  # exact match
        'category': 'string'  # exact match
    }
    compiler.validate_extracted_fields(extracted)  # Should not raise  # Should not raise


def test_validate_empty_input(compiler):
    """Test empty input doesn't raise errors"""
    compiler.validate_extracted_fields({})  # Should not raise


def test_validate_multiple_errors(compiler):
    """Test reporting of multiple validation issues"""
    extracted = {
        'missing1': 'string',
        'price': 'int',  # should be float
        'missing2': 'float',
        'category': 'string'  # valid
    }
    with pytest.raises(ReQLCompilationError) as excinfo:
        compiler.validate_extracted_fields(extracted)

    error_msg = str(excinfo.value)
    assert "Missing fields in schema: missing1, missing2" in error_msg
    assert "Type mismatches: price (filter:int vs schema:float)" in error_msg


# ----------------------------
# _types_compatible Test Cases (unchanged)
# ----------------------------

def test_type_compatibility_strings(compiler):
    assert compiler._types_compatible('string', 'varchar') is True
    assert compiler._types_compatible('string', 'text') is True
    assert compiler._types_compatible('string', 'int') is False


def test_type_compatibility_numbers(compiler):
    assert compiler._types_compatible('int', 'integer') is True
    assert compiler._types_compatible('float', 'decimal') is True
    assert compiler._types_compatible('int', 'float') is False


def test_type_compatibility_special_cases(compiler):
    assert compiler._types_compatible('boolean', 'bool') is True
    assert compiler._types_compatible('datetime', 'timestamp') is True
    assert compiler._types_compatible('string', 'datetime') is False