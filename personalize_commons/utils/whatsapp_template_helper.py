from typing import Any, Dict

from personalize_commons.constants.app_constants import AppConstants


class WhatsAppTemplateHelper:

    @staticmethod
    def build_template_variables(whatsapp_config: dict, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Builds template variables from the given config and row data.
        
        For values starting with 'user.' or 'item.', it looks up the value in the row data.
        For all other values, it treats them as custom values and uses them as-is.
        
        Args:
            whatsapp_config: Dictionary containing template variables configuration
            row: Dictionary containing the data to extract values from
            
        Returns:
            Dictionary of resolved template variables
        """
        if not whatsapp_config or AppConstants.TEMPLATE_VARIABLES not in whatsapp_config:
            return {}
            
        try:
            # Sort template variables by placeholder number (1, 2, 3…)
            sorted_vars = sorted(
                whatsapp_config[AppConstants.TEMPLATE_VARIABLES].items(),
                key=lambda x: int(x[0])
            )
        except (ValueError, TypeError):
            # If sorting fails, use the original order
            sorted_vars = whatsapp_config[AppConstants.TEMPLATE_VARIABLES].items()

        resolved = {}
        for key, value in sorted_vars:
            if not isinstance(value, str):
                resolved[key] = str(value)
                continue
                
            # Handle custom values (not starting with user. or item.)
            if not (value.startswith('user.') or value.startswith('item.')):
                resolved[key] = value
                continue
                
            # Handle user/item values
            try:
                if "." not in value:
                    resolved[key] = value  # Use as-is if no dot
                    continue
                    
                entity, field = value.split(".", 1)
                if entity in row and field in row[entity]:
                    resolved[key] = row[entity][field]
                else:
                    resolved[key] = value  # Fallback to original value if not found
            except Exception:
                resolved[key] = value  # Fallback to original value on any error

        return resolved

    @staticmethod
    def validate_template_variables(template_vars: dict, schema: dict) -> dict:
        """
        Validates WhatsApp template variables against schema.
        
        Only validates fields that start with 'user.' or 'item.'.
        All other fields are treated as custom values and pass validation.

        Args:
            template_vars: dict of template variables ({"1": "user.name", ...})
            schema: dict of tenant and their fields

        Returns:
            dict with validation results
            
        Raises:
            ValueError: If schema is not a dictionary or is empty
        """
        if not isinstance(schema, dict) or not schema:
            raise ValueError("Schema must be a non-empty dictionary")
            
        if not template_vars:
            return {
                "is_valid": True,
                'errors': {},
                'message': 'No template variables to validate'
            }
            
        errors = {}
        for var_key, var_value in template_vars.items():
            # Skip validation for non-string values
            if not isinstance(var_value, str):
                errors[var_key] = "Value must be a string"
                continue
                
            # Only validate values that start with 'user.' or 'item.'
            if not (var_value.startswith('user.') or var_value.startswith('item.')):
                # Skip validation for all other values (treat as custom values)
                continue
                
            # Must contain "." (should always be true due to above check)
            if "." not in var_value:
                errors[var_key] = f"Invalid format: '{var_value}', must be entity.field"
                continue

            entity, field = var_value.split(".", 1)

            # Validate entity exists in schema
            if entity not in schema:
                errors[var_key] = f"Invalid entity: '{entity}'. Must be one of: {', '.join(schema.keys())}"
                continue

            # Get all valid fields for the entity
            fields = [f.get("field_name") for f in schema[entity] if isinstance(f, dict)]
            
            # Validate field exists in entity
            if field not in fields:
                available_fields = ", ".join(fields) if fields else "No fields available"
                errors[var_key] = (
                    f"Invalid field: '{field}' for entity '{entity}'. "
                    f"Available fields: {available_fields}"
                )

        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "message": f"Found {len(errors)} validation error(s)" if errors else "Validation successful"
        }

if __name__ == '__main__':
    # Example usage
    helper = WhatsAppTemplateHelper()
    whatsapp_config = {
        "template_variables": {
            "1": "user.name",
            "2": "user.age",
            "3": "dheeban"
        }
    }
    row = {
        "user": {"name": "John", "age": 25},
        "item": {"name": "boost", "rate": 43}
    }

    print(helper.build_template_variables(whatsapp_config, row))

    whatsapp_config = {
        "template_variables": {
            "1": "user.name",
            "2": "user.ages",
            "3": "item.name",
            "4": "order.id"  # ❌ invalid example
        }
    }

    schema = {
        "user": [
            {"display_name": "user_id", "dtype": "string", "field_name": "user_id"},
            {"display_name": "name", "dtype": "string", "field_name": "name"},
            {"display_name": "location", "dtype": "string", "field_name": "location"},
            {"display_name": "age", "dtype": "int", "field_name": "age"},
        ],
        "item": [
            {"display_name": "item_id", "dtype": "string", "field_name": "item_id"},
            {"display_name": "category", "dtype": "string", "field_name": "category"},
            {"display_name": "name", "dtype": "string", "field_name": "name"},
            {"display_name": "price", "dtype": "float", "field_name": "price"},
            {"display_name": "brand", "dtype": "string", "field_name": "brand"},
        ]
    }

    result=WhatsAppTemplateHelper().validate_template_variables(whatsapp_config['template_variables'],schema)

    print(result)
