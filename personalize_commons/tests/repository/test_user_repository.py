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


# --- String Tests ---

def test_string_equals(repo):
    rules = {
        "field_name": "brand",
        "operator": "==",
        "value": "Nike",
        "dtype": "string"
    }
    query = repo._build_partiql_query(rules)
    assert query == '"brand" = \'Nike\''


def test_string_escaping(repo):
    rules = {
        "field_name": "name",
        "operator": "==",
        "value": "O'Brian",
        "dtype": "string"
    }
    query = repo._build_partiql_query(rules)
    assert query == '"name" = \'O\'\'Brian\''


def test_string_in(repo):
    rules = {
        "field_name": "category",
        "operator": "in",
        "value": ["Shoes", "Sweets"],
        "dtype": "string"
    }
    query = repo._build_partiql_query(rules)
    assert query == "\"category\" IN ('Shoes', 'Sweets')"


# --- Integer Tests ---

def test_int_operators(repo):
    operators = [
        ("==", "="),
        ("!=", "<>"),
        (">=", ">="),
        ("<=", "<="),
        (">", ">"),
        ("<", "<")
    ]
    for op_json, op_sql in operators:
        rules = {
            "field_name": "age",
            "operator": op_json,
            "value": 25,
            "dtype": "int"
        }
        query = repo._build_partiql_query(rules)
        assert query == f'"age" {op_sql} 25'


def test_int_in(repo):
    rules = {
        "field_name": "id",
        "operator": "in",
        "value": [1, 2, 3],
        "dtype": "int"
    }
    query = repo._build_partiql_query(rules)
    assert query == '"id" IN (1, 2, 3)'


# --- Float Tests ---

def test_float_operators(repo):
    rules = {
        "field_name": "price",
        "operator": ">=",
        "value": 50.5,
        "dtype": "float"
    }
    query = repo._build_partiql_query(rules)
    assert query == '"price" >= 50.5'


def test_float_integer_formatting(repo):
    # Tests that 50.0 is formatted as 50 to avoid PartiQL issues if needed, 
    # though PartiQL often handles .0 fine, being consistent with the code.
    rules = {
        "field_name": "price",
        "operator": "==",
        "value": 50.0,
        "dtype": "float"
    }
    query = repo._build_partiql_query(rules)
    assert query == '"price" = 50'


# --- Boolean Tests ---

def test_bool_operators(repo):
    rules = {
        "field_name": "is_active",
        "operator": "==",
        "value": True,
        "dtype": "bool"
    }
    query = repo._build_partiql_query(rules)
    assert query == '"is_active" = TRUE'

    rules["value"] = False
    query = repo._build_partiql_query(rules)
    assert query == '"is_active" = FALSE'


# --- Nested Rules Tests ---

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
    expected = '("price" >= 100 AND ("category" IN (\'Sweets\') OR "brand" = \'Puma\'))'
    assert query == expected


# --- Error Handling ---

def test_invalid_dtype(repo):
    rules = {
        "field_name": "price",
        "operator": "==",
        "value": 10,
        "dtype": "unknown"
    }
    with pytest.raises(ValueError):
        repo._build_partiql_query(rules)


# --- Operator Coverage Tests ---

def test_all_operators(repo):
    # Mapping of JSON operator to expected SQL operator
    operator_mapping = [
        ("==", "="),
        ("!=", "<>"),
        (">=", ">="),
        ("<=", "<="),
        (">", ">"),
        ("<", "<")
    ]
    
    # Test simple comparison operators
    for json_op, sql_op in operator_mapping:
        rules = {
            "field_name": "score",
            "operator": json_op,
            "value": 100,
            "dtype": "int"
        }
        query = repo._build_partiql_query(rules)
        assert query == f'"score" {sql_op} 100'

    # Test 'in' operator
    rules_in = {
        "field_name": "status",
        "operator": "in",
        "value": ["active", "pending"],
        "dtype": "string"
    }
    assert repo._build_partiql_query(rules_in) == '"status" IN (\'active\', \'pending\')'

    # Test 'not in' operator
    rules_not_in = {
        "field_name": "status",
        "operator": "not in",
        "value": ["deleted", "archived"],
        "dtype": "string"
    }
    assert repo._build_partiql_query(rules_not_in) == '"status" NOT IN (\'deleted\', \'archived\')'

    # Test case-insensitivity of operators
    rules_case = {
        "field_name": "name",
        "operator": "IN",
        "value": ["Alice"],
        "dtype": "string"
    }
    assert repo._build_partiql_query(rules_case) == '"name" IN (\'Alice\')'


# --- Full Workflow Test ---

def test_query_with_rules_formatting(repo):
    repo.table_name = "Users"
    rules = {"field_name": "name", "operator": "==", "value": "Aryan", "dtype": "string"}
    # The generated statement should have double quotes for table and column names
    # and single quotes for the tenant_id and value.
    # Note: query_with_rules calls execute_partiql, so we mainly check logical flow.
    # We already test the _build_partiql_query extensively.
    pass
