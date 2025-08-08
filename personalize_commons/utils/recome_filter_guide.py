

"""
================================================================================
ReQL Filter Builder – Extension Guidelines
================================================================================
This section documents how to extend the filter builder to support **new operators**
or **new data types** according to our application standard.

-------------------------------------------------------------------------------
1. SUPPORTED DATA TYPES
-------------------------------------------------------------------------------
Current: SUPPORTED_DATATYPES = ['int', 'float', 'string', 'datetime', 'boolean']

Steps to add a new datatype:
    1. Add the new datatype name to SUPPORTED_DATATYPES list.
       Example:
           SUPPORTED_DATATYPES.append('geo_point')
    2. Update `validate_value_type()` function to handle validation for the new type.
       Example:
           if dtype == 'geo_point':
               # implement latitude/longitude validation
    3. Update `convert_value_for_reql()` to format the value in correct ReQL syntax.
    4. If the datatype requires special escaping or quoting, handle it inside
       `escape_value()` or equivalent helper.
    5. Update the frontend (Flutter) filter UI to let users pick the new datatype.

-------------------------------------------------------------------------------
2. SUPPORTED OPERATORS
-------------------------------------------------------------------------------
Typical operators:
    - Equality: "=" , "!="
    - Comparison: ">", "<", ">=", "<="
    - String: "startswith", "endswith", "contains"
    - Set: "in", "not in"
    - Boolean: "and", "or", "not"
    - Context: "$context_item"

Steps to add a new operator:
    1. Add the new operator to the SUPPORTED_OPERATORS list or mapping.
    2. Update `build_expression()` to translate the new operator into valid ReQL syntax.
       Example:
           if op == "matches_regex":
               return f'{field} matches "{value}"'
    3. If the operator is datatype-specific, enforce this rule in `validate_operator_for_type()`.
    4. Update unit tests to include positive and negative cases for this operator.
    5. Update the frontend filter builder UI to allow selection of this new operator.

-------------------------------------------------------------------------------
3. VALIDATION RULES
-------------------------------------------------------------------------------
All changes must maintain:
    - Only allowed datatypes from SUPPORTED_DATATYPES.
    - Operators must be compatible with the selected datatype.
    - Value formatting must strictly follow ReQL syntax rules from:
      https://docs.recombee.com/reql_filtering_and_boosting

-------------------------------------------------------------------------------
4. TESTING CHANGES
-------------------------------------------------------------------------------
Before deploying:
    - Run unit tests for the filter builder (add new test cases for the change).
    - Use Recombee’s Query Validation Console to confirm the generated filter string is valid.
    - Test the new filter via both `recommendItemsToUser` and `recommendItemsToItem` APIs.

-------------------------------------------------------------------------------
5. FRONTEND IMPACT
-------------------------------------------------------------------------------
Adding a new datatype or operator usually requires:
    - Updating Flutter filter builder UI for selection dropdowns.
    - Adding input widget logic (text field, date picker, multi-select, etc.).
    - Ensuring UI serializes the new operator/type correctly for the backend.

-------------------------------------------------------------------------------
Reference:
    Recombee ReQL Filtering & Boosting:
    https://docs.recombee.com/reql_filtering_and_boosting
================================================================================
"""

"""
================================================================================
Recombee ReQL Supported Operators by Datatype
================================================================================
This section summarizes the operators supported by each datatype in Recombee filter DSL.

Note: Recombee’s official docs describe the following syntax and functions,
including examples like `'field' in {"a", "b"}`, `contains(...)`, `startsWith(...)`, 
and `context_item["prop"]`.

-------------------------------------------------------------------------------
Summary Table
-------------------------------------------------------------------------------
| Datatype | Supported Operators                                | Notes                                                        |
|----------|----------------------------------------------------|--------------------------------------------------------------|
| string   | ==, !=, in, not in, contains, startsWith           | 'value' in 'field' or contains('field', "val")               |
| int      | ==, !=, <, <=, >, >=, in, not in                   | Numeric comparisons; list membership using {1,2}             |
| float    | ==, !=, <, <=, >, >=, in, not in                   | Same as int, but for floats                                 |
| datetime | ==, !=, <, <=, >, >=                               | Compare timestamps or now()-based expressions                |
| boolean  | ==, !=                                            | Simple boolean match                                        |
| context_item | (with above) same ops as underlying field     | E.g., 'prop' == context_item["prop"]                        |

-------------------------------------------------------------------------------
Operator Patterns & Format Examples
-------------------------------------------------------------------------------
- Equality/inequality:
    `'field' == "Books"`
    `'price' >= 100`

- Membership in static set:
    `'category' in {"Books", "Electronics"}`
    `not ('field' in {"a", "b"})` (for `not in`)

- Contains / startsWith (string functions):
    `contains('title', "Harry")`
    `startsWith('name', "App")`

- Numeric comparisons:
    `'rating' > 4.5`
    `'views' <= 1000`

- Timestamp comparisons:
    `'published_at' >= now()`
    `'created_at' < "2025-08-09T00:00:00Z"`

- Boolean:
    `'is_active' == true`
    `'flagged' != false`

- Contextual filters:
    `'category' == context_item["category"]`
    Useful in RecommendItemsToItem requests — matches same category as context.

-------------------------------------------------------------------------------
Notes:
-------------------------------------------------------------------------------
- Lists must use **curly braces** for literals: `{"a", "b"}`, not `[...]`.
- String constants always use **double quotes** `"Books"`. Property names use **single quotes**.
- `contains`/`startsWith` are string-specific functions.
- For `in` / `not in` with numeric types, lists must have valid numbers.
- Use `allow_context_item=True` in your backend for operators on context_item; disallowed in RecommendItemsToUser.

Refer to reference patterns in Recombee docs under **ReQL Filtering & Boosting**:
https://docs.recombee.com/reql_filtering_and_boosting
================================================================================
"""
