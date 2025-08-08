import json
from typing import Any, Dict, List, Union

from personalize_commons.constants.app_constants import AppConstants

"""

Notion wikki: https://www.notion.so/Recombee-ReQL-filter-2491d2ff3fca809880ace0fa1ea4dbf9

================================================================================
ReQL Filter Compiler – Usage Guide
================================================================================
Purpose:
    This module compiles a **safe JSON-based DSL** for Recombee filtering
    into a valid ReQL filter string. It ensures:
      - All datatypes and operators are validated against the schema.
      - Correct value formatting for ReQL (e.g., quoted strings, curly-brace sets).
      - Optional enforcement of `$context_item` usage depending on the API call.

-------------------------------------------------------------------------------
1. BASIC USAGE
-------------------------------------------------------------------------------
    from reql_filter_compiler import ReQLFilterCompiler

    schema = {
        'category': 'string',
        'brand': 'string',
        'price': 'float',
        'rating': 'float',
        'published_at': 'datetime',
        'is_available': 'boolean',
        'itemId': 'int'
    }

    compiler = ReQLFilterCompiler(schema)

    # Simple AND filter
    dsl = {
        "op": "AND",
        "rules": [
            {"field": "category", "operator": "==", "value": "Books"},
            {"field": "price", "operator": "<=", "value": 500},
            {"field": "itemId", "operator": "in", "value": [42, 77]}
        ]
    }
    print(compiler.compile(dsl, allow_context_item=False))
    # Output: 'category' == "Books" AND 'price' <= 500 AND 'itemId' in {42, 77}

-------------------------------------------------------------------------------
2. CONTEXT ITEM USAGE
-------------------------------------------------------------------------------
    # `$context_item` allows referencing the property of the item being used
    # as the "context" in certain Recombee calls (e.g., RecommendItemsToItem).
    dsl_ctx = {
        "op": "AND",
        "rules": [
            {"field": "brand", "operator": "==", "value": {"$context_item": "brand"}},
            {"field": "price", "operator": "<=", "value": 100}
        ]
    }
    print(compiler.compile(dsl_ctx, allow_context_item=True))
    # Output: 'brand' == context_item["brand"] AND 'price' <= 100

-------------------------------------------------------------------------------
3. NESTED GROUPS
-------------------------------------------------------------------------------
    dsl_nested = {
        "op": "OR",
        "rules": [
            {
                "op": "AND",
                "rules": [
                    {"field": "category", "operator": "==", "value": "Books"},
                    {"field": "price", "operator": "<", "value": 200}
                ]
            },
            {"field": "is_available", "operator": "==", "value": True}
        ]
    }
    print(compiler.compile(dsl_nested))
    # Output: ('category' == "Books" AND 'price' < 200) OR 'is_available' == true

-------------------------------------------------------------------------------
4. SUPPORTED DATA TYPES
-------------------------------------------------------------------------------
    int       → Whole numbers
    float     → Decimal numbers
    string    → Quoted text
    datetime  → ISO 8601 string or now()
    boolean   → true / false

-------------------------------------------------------------------------------
5. SUPPORTED OPERATORS BY TYPE
-------------------------------------------------------------------------------
    string    → ==, !=, in, not in, contains, startsWith
    int/float → ==, !=, <, <=, >, >=, in, not in
    datetime  → <, <=, >, >=, ==, !=
    boolean   → ==, !=

-------------------------------------------------------------------------------
6. VALIDATION RULES
-------------------------------------------------------------------------------
    - Property must exist in schema.
    - Operator must be allowed for property type.
    - Values must match the declared type.
    - For 'in' / 'not in': value must be a list.
    - `$context_item` usage allowed only if allow_context_item=True.

-------------------------------------------------------------------------------
Reference:
    Recombee ReQL Filtering & Boosting Docs:
    https://docs.recombee.com/reql_filtering_and_boosting
================================================================================
"""



class ReQLCompilationError(Exception):
    pass


class ReQLFilterCompiler:
    """
    Compile a JSON DSL into a safe Recombee ReQL filter string.
    """

    # operator sets mapped to supported datatypes
    OPERATORS_BY_TYPE = {
        'string': {'==', '!=', 'in', 'not in', 'contains', 'startsWith'},
        'int': {'==', '!=', '<', '<=', '>', '>=', 'in', 'not in'},
        'float': {'==', '!=', '<', '<=', '>', '>=', 'in', 'not in'},
        'datetime': {'<', '<=', '>', '>=', '==', '!='},
        'boolean': {'==', '!='},
    }

    def __init__(self, schema: Dict[str, str]):
        """
        :param schema: mapping property_name -> datatype (one of SUPPORTED_DATATYPES)
        """
        # validate schema types
        for prop, dtype in schema.items():
            if dtype not in AppConstants.SUPPORTED_DATATYPES:
                raise ValueError(f"Unsupported datatype for property '{prop}': {dtype}")
        self.schema = schema.copy()

    # -------------------------
    # helpers for formatting
    # -------------------------
    @staticmethod
    def _escape_string_for_reql(s: str) -> str:
        # ReQL examples use double-quoted string constants; escape double quotes and backslashes
        return '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'

    @staticmethod
    def _format_property(prop: str) -> str:
        # property names must be single-quoted in ReQL: 'property'
        # assume prop is a simple identifier (validated elsewhere)
        return f"'{prop}'"

    def _format_value(self, raw: Any, dtype: str) -> str:
        """
        Format a single value according to datatype for ReQL (not multi-values)
        """
        if raw is None:
            return 'null'
        if isinstance(raw, dict) and '$context_item' in raw:
            # compile context item reference to context_item["property"]
            inner = raw['$context_item']
            if not isinstance(inner, str):
                raise ReQLCompilationError('Invalid $context_item reference')
            # use double quotes inside brackets per docs
            return f'context_item["{inner}"]'
        if isinstance(raw, str) and raw.startswith('$context_item.'):
            # allow alternate string form: "$context_item.prop"
            _, _, prop = raw.partition('.')
            return f'context_item["{prop}"]'

        if dtype in ('int', 'float'):
            if isinstance(raw, (int, float)):
                return str(raw)
            # allow numeric string
            try:
                # keep integer if int-like
                if '.' in str(raw):
                    float(raw)  # validate
                    return str(raw)
                else:
                    int(raw)
                    return str(int(raw))
            except Exception:
                raise ReQLCompilationError(f"Value {raw!r} is not a valid number for type {dtype}")
        if dtype == 'boolean':
            if isinstance(raw, bool):
                return 'true' if raw else 'false'
            lr = str(raw).strip().lower()
            if lr in ('true', 'false'):
                return lr
            raise ReQLCompilationError(f"Value {raw!r} is not a valid boolean")
        if dtype == 'datetime':
            # Accept now() token or ISO strings.
            if isinstance(raw, str) and raw.strip().lower() == 'now()':
                return 'now()'
            # otherwise treat as quoted string (Recombee accepts ISO strings)
            if isinstance(raw, str):
                return self._escape_string_for_reql(raw)
            raise ReQLCompilationError(f"Value {raw!r} is not a valid datetime (expected 'now()' or ISO string)")
        # default 'string'
        return self._escape_string_for_reql(str(raw))

    def _format_multi_values(self, values: List[Any], dtype: str) -> str:
        """
        Format a multi-value literal for use with `in` / `not in` or set-operations.
        Recombee docs show set literals with curly braces: {"a","b"}
        We'll follow that format.
        """
        if not isinstance(values, (list, tuple)):
            raise ReQLCompilationError('Multi-value operator requires a list/tuple')

        parts = []
        for v in values:
            # for types int/float/boolean/datetime we must format accordingly
            parts.append(self._format_value(v, dtype))
        # Join with ", " inside curly braces
        return '{' + ', '.join(parts) + '}'

    # -------------------------
    # validation
    # -------------------------
    def _validate_operator_for_property(self, prop: str, op: str):
        if prop not in self.schema:
            raise ReQLCompilationError(f"Unknown property '{prop}'")

        dtype = self.schema[prop]
        allowed = self.OPERATORS_BY_TYPE.get(dtype, set())
        if op not in allowed:
            raise ReQLCompilationError(f"Operator '{op}' not supported for datatype '{dtype}'")

    # -------------------------
    # compilation: rule & groups
    # -------------------------
    def compile_rule(self, rule: Dict[str, Any]) -> str:
        """
        Compile a single rule dict into ReQL fragment.
        Expected rule format:
          { "field": "category", "operator": "in", "value": ["A","B"] }
        or value can be a context reference:
          { "field": "brand", "operator":"==", "value": {"$context_item":"brand"} }
        """
        if 'field' not in rule or 'operator' not in rule or 'value' not in rule:
            raise ReQLCompilationError("Rule must contain 'field', 'operator', and 'value'")

        prop = rule['field']
        op = rule['operator']
        val = rule['value']

        # allow context_item property name like "$context_item.brand" as a special value
        # validate operator allowed for property
        if prop not in self.schema:
            raise ReQLCompilationError(f"Unknown property '{prop}'")

        dtype = self.schema[prop]
        if op not in self.OPERATORS_BY_TYPE.get(dtype, set()):
            raise ReQLCompilationError(f"Operator '{op}' not allowed for type '{dtype}'")

        # Build fragment depending on operator
        # Note: Recombee docs have patterns:
        #  - membership: 'field' in {"a","b"}  (use curly braces)
        #  - context item: context_item["prop"]
        #  - size / set intersection not required here since "set" dtype is not in SUPPORTED_DATATYPES
        if op in ('in', 'not in'):
            # value must be list
            if not isinstance(val, (list, tuple)):
                raise ReQLCompilationError(f"Operator '{op}' requires a list of values")
            multi = self._format_multi_values(val, dtype)
            base = f"{self._format_property(prop)} in {multi}"
            if op == 'in':
                return base
            else:
                return f"not ({base})"

        if op in ('contains', 'startsWith'):
            # treat as function style calls (string functions)
            if dtype != 'string':
                raise ReQLCompilationError(f"Operator '{op}' only valid for string type")
            # extract the single value
            if isinstance(val, (list, tuple)):
                raise ReQLCompilationError(f"Operator '{op}' expects a single value")
            lit = self._format_value(val, 'string')
            if op == 'contains':
                return f"contains({self._format_property(prop)}, {lit})"
            else:
                return f"startsWith({self._format_property(prop)}, {lit})"

        # binary operators for numbers, booleans, strings, datetime
        # map '==' to '==' (we accept '==' token)
        # format left side 'prop' and right side as typed literal
        if isinstance(val, list):
            raise ReQLCompilationError(f"Operator '{op}' does not accept list value")

        right = self._format_value(val, dtype)
        return f"{self._format_property(prop)} {op} {right}"

    def compile_group(self, group: Dict[str, Any], allow_context_item: bool = True) -> str:
        """
        Compile a group structure (the DSL root) into a ReQL string.

        Expected group format:
        {
          "op": "AND" | "OR",
          "rules": [ <rule> | <group> , ... ]
        }

        Recurses into nested groups.
        """
        if 'op' not in group or 'rules' not in group:
            raise ReQLCompilationError("Group must contain 'op' and 'rules'")

        op_raw = group['op']
        if op_raw not in ('AND', 'OR'):
            raise ReQLCompilationError("Group 'op' must be 'AND' or 'OR'")

        rules = group['rules']
        if not isinstance(rules, list):
            raise ReQLCompilationError("'rules' must be a list")

        compiled_parts: List[str] = []
        for element in rules:
            if isinstance(element, dict) and 'rules' in element:
                # nested group
                part = self.compile_group(element, allow_context_item=allow_context_item)
                compiled_parts.append(f"({part})")
            else:
                # single rule
                # if rule uses $context_item but allow_context_item is False — reject
                if isinstance(element, dict):
                    # quick check if value references context_item
                    val = element.get('value')
                    if (isinstance(val, dict) and '$context_item' in val) or (isinstance(val, str) and val.startswith('$context_item.')):
                        if not allow_context_item:
                            raise ReQLCompilationError("$context_item usage is not allowed for this API call")
                    compiled_parts.append(self.compile_rule(element))
                else:
                    raise ReQLCompilationError("Each rule must be a dict")

        joiner = ' AND ' if op_raw == 'AND' else ' OR '
        if not compiled_parts:
            return 'true'  # permissive default
        return joiner.join(compiled_parts)

    def compile(self, dsl: Union[str, Dict[str, Any]], allow_context_item: bool = True) -> str:
        """
        Top-level compile function.
        dsl can be a dict (JSON object) or a JSON string
        """
        if isinstance(dsl, str):
            try:
                dsl_obj = json.loads(dsl)
            except Exception as e:
                raise ReQLCompilationError(f"DSL JSON parse error: {e}")
        else:
            dsl_obj = dsl

        return self.compile_group(dsl_obj, allow_context_item=allow_context_item)


if __name__ == '__main__':
    schema = {
        'category': 'string',
        'brand': 'string',
        'price': 'float',
        'rating': 'float',
        'published_at': 'datetime',
        'is_available': 'boolean',
        'itemId': 'int'
    }

    compiler = ReQLFilterCompiler(schema)

    dsl ={
  "op": "AND",
  "rules": [
    {
      "field": "itemId",
      "operator": "==",
      "value": 1
    },
    {
      "op": "OR",
      "rules": [
        {
          "field": "price",
          "operator": "==",
          "value": 22
        },
        {
          "field": "is_available",
          "operator": "==",
          "value": True
        }
      ]
    }
  ]
}

    reql = compiler.compile(dsl, allow_context_item=True)
    print(reql)
    # Output: "'category' == "Books" AND 'price' <= 500 AND 'itemId' in {"item-42", "item-77"}"

    dsl_ctx = {
        "op": "AND",
        "rules": [
            {"field": "brand", "operator": "==", "value": {"$context_item": "brand"}},
            {"field": "price", "operator": "<=", "value": 100}
        ]
    }

    # compile with context allowed (RecommendItemsToItem)
    reql2 = compiler.compile(dsl_ctx, allow_context_item=True)
    print(reql2)
    # "'brand' == context_item["brand"] AND 'price' <= 100"

    dsl_nested = {
        "op": "OR",
        "rules": [
            {
                "op": "AND",
                "rules": [
                    {"field": "category", "operator": "==", "value": "Books"},
                    {"field": "price", "operator": "<", "value": 200}
                ]
            },
            {"field": "is_available", "operator": "==", "value": True}
        ]
    }

    reql3 = compiler.compile(dsl_nested, allow_context_item=False)
    print(reql3)
    # "('category' == "Books" AND 'price' < 200) OR 'is_available' == true"


