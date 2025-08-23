from decimal import Decimal

import pytest

from personalize_commons.repositories.user_repository import UserRepository


class DummyClient:
    def execute_statement(self, **kwargs):
        return {"Items": []}  # Mock empty response


class DummyResource:
    def Table(self, name):
        return None


@pytest.fixture
def repo():
    return UserRepository(DummyClient(), DummyResource())


def test_simple_rule(repo):
    rules = {
        "field_name": "price",
        "operator": ">=",
        "value": 50,
        "dtype": "int"
    }
    query = repo._build_partiql_query(rules)
    assert query == "price >= 50"

def test_not_in_rule(repo):
    rules = {
        "field_name": "name",
        "operator": "not in",
        "value": [50,100],
        "dtype": "string"
    }
    query = repo._build_partiql_query(rules)
    assert query == "name NOT IN ('50', '100')"


def test_string_equals(repo):
    rules = {
        "field_name": "brand",
        "operator": "==",
        "value": "Nike",
        "dtype": "string"
    }
    query = repo._build_partiql_query(rules)
    assert query == "brand = 'Nike'"


def test_in_operator(repo):
    rules = {
        "field_name": "category",
        "operator": "in",
        "value": ["Shoes", "Sweets"],
        "dtype": "string"
    }
    query = repo._build_partiql_query(rules)
    assert query == "category IN ('Shoes', 'Sweets')"


def test_nested_and_or(repo):
    rules = {
        "op": "AND",
        "rules": [
            {
                "field_name": "price",
                "operator": ">=",
                "value": 100,
                "dtype": "float"
            },
            {
                "op": "OR",
                "rules": [
                    {
                        "field_name": "category",
                        "operator": "in",
                        "value": ["Sweets"],
                        "dtype": "string"
                    },
                    {
                        "field_name": "brand",
                        "operator": "==",
                        "value": "Puma",
                        "dtype": "string"
                    }
                ]
            }
        ]
    }
    query = repo._build_partiql_query(rules)
    expected = "(price >= 100 AND (category IN ('Sweets') OR brand = 'Puma'))"
    assert query == expected

def test_nested_and_or_root_or(repo):
    rules = {
        "op": "OR",
        "rules": [
            {
                "field_name": "price",
                "operator": ">=",
                "value": 100,
                "dtype": "float"
            },
            {
                "op": "OR",
                "rules": [
                    {
                        "field_name": "category",
                        "operator": "in",
                        "value": ["Sweets"],
                        "dtype": "string"
                    },
                    {
                        "field_name": "brand",
                        "operator": "==",
                        "value": "Puma",
                        "dtype": "string"
                    }
                ]
            }
        ]
    }
    query = repo._build_partiql_query(rules)
    expected = "(price >= 100 OR (category IN ('Sweets') OR brand = 'Puma'))"
    assert query == expected

def test_invalid_dtype(repo):
    rules = {
        "field_name": "price",
        "operator": "==",
        "value": 10,
        "dtype": "unknown"
    }
    with pytest.raises(ValueError):
        repo._build_partiql_query(rules)

def test_string_json(repo):
    rules ={'op': 'AND', 'rules':
        [
            {'dtype': 'int', 'value': '40', 'operator': '<=', 'field_name': 'age'},
            {'dtype': 'string', 'value': 'Chennai', 'operator': '==', 'field_name': 'location'},
            {'op': 'OR', 'rules': [{'dtype': 'int', 'value': Decimal('10'), 'operator': '>=', 'field_name': 'age'}]}
        ]}
    query = repo._build_partiql_query(rules)
    print(query)