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
# Positive Test Cases
# ----------------------------

def test_basic_and_filter(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {AppConstants.FIELD_NAME: "category", "operator": "==", "value": "Books"},
            {AppConstants.FIELD_NAME: "price", "operator": "<=", "value": 500},
            {AppConstants.FIELD_NAME: "itemId", "operator": "in", "value": [42, 77]}
        ]
    }
    expected = "'category' == \"Books\" AND 'price' <= 500 AND 'itemId' in {42, 77}"
    assert compiler.compile(dsl) == expected

def test_context_item_allowed(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {AppConstants.FIELD_NAME: "brand", "operator": "==", "value": {"$context_item": "brand"}},
            {AppConstants.FIELD_NAME: "price", "operator": "<=", "value": 100}
        ]
    }
    expected = "'brand' == context_item[\"brand\"] AND 'price' <= 100"
    assert compiler.compile(dsl, allow_context_item=True) == expected

def test_nested_groups(compiler):
    dsl = {
        "op": "OR",
        "rules": [
            {
                "op": "AND",
                "rules": [
                    {AppConstants.FIELD_NAME: "category", "operator": "==", "value": "Books"},
                    {AppConstants.FIELD_NAME: "price", "operator": "<", "value": 200}
                ]
            },
            {AppConstants.FIELD_NAME: "is_available", "operator": "==", "value": True}
        ]
    }
    expected = "('category' == \"Books\" AND 'price' < 200) OR 'is_available' == true"
    assert compiler.compile(dsl) == expected

def test_in_operator_with_strings(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {AppConstants.FIELD_NAME: "category", "operator": "in", "value": ["A", "B"]}
        ]
    }
    expected = "'category' in {\"A\", \"B\"}"
    assert compiler.compile(dsl) == expected

# ----------------------------
# Negative Test Cases
# ----------------------------

def test_unknown_property(compiler):
    dsl = {"op": "AND", "rules": [{AppConstants.FIELD_NAME: "unknown", "operator": "==", "value": "X"}]}
    with pytest.raises(ReQLCompilationError) as excinfo:
        compiler.compile(dsl)
    assert "Unknown property" in str(excinfo.value)

def test_operator_not_allowed_for_type(compiler):
    dsl = {"op": "AND", "rules": [{AppConstants.FIELD_NAME: "price", "operator": "contains", "value": "X"}]}
    with pytest.raises(ReQLCompilationError) as excinfo:
        compiler.compile(dsl)
    assert "Operator 'contains' not allowed" in str(excinfo.value)

def test_in_operator_requires_list(compiler):
    dsl = {"op": "AND", "rules": [{AppConstants.FIELD_NAME: "category", "operator": "in", "value": "A"}]}
    with pytest.raises(ReQLCompilationError) as excinfo:
        compiler.compile(dsl)
    assert "requires a list" in str(excinfo.value)

def test_list_not_allowed_for_eq(compiler):
    dsl = {"op": "AND", "rules": [{AppConstants.FIELD_NAME: "category", "operator": "==", "value": ["A", "B"]}]}
    with pytest.raises(ReQLCompilationError) as excinfo:
        compiler.compile(dsl)
    assert "does not accept list" in str(excinfo.value)

def test_context_item_not_allowed(compiler):
    dsl = {
        "op": "AND",
        "rules": [
            {AppConstants.FIELD_NAME: "brand", "operator": "==", "value": {"$context_item": "brand"}}
        ]
    }
    with pytest.raises(ReQLCompilationError) as excinfo:
        compiler.compile(dsl, allow_context_item=False)
    assert "$context_item usage is not allowed" in str(excinfo.value)
