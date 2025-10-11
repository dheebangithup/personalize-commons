from typing import Any, Dict

from personalize_commons.constants.app_constants import AppConstants


class WhatsAppTemplateHelper:

    @staticmethod
    def build_template_variables( whatsapp_config: dict, row: Dict[str, Any]) -> Dict[str, Any]:
        # Sort template variables by placeholder number (1, 2, 3…)
        sorted_vars = sorted(
            whatsapp_config[AppConstants.TEMPLATE_VARIABLES].items(),
            key=lambda x: int(x[0])
        )

        resolved = {}
        for key, value in sorted_vars:
            entity, field = value.split(".")
            resolved[key] = row[entity][field]

        return resolved

    @staticmethod
    def validate_template_variables(template_vars: dict, schema: dict) -> dict:
        """
        Validates WhatsApp template variables against schema.

        Args:
            template_vars: dict of template variables ({"1": "user.name", ...})
            schema: dict of tenant and their fields

        Returns:
            dict with validation results
        """
        if not template_vars:
            return {
                "is_valid": True,
                'errors': [],
            }
        errors = {}
        for var_key, var_value in template_vars.items():
            # Must contain "."
            if "." not in var_value:
                errors[var_key] = f"Invalid format: '{var_value}', must be entity.field"
                continue

            entity, field = var_value.split(".", 1)

            # Validate entity
            if entity not in schema:
                errors[var_key] = f"Invalid entity: '{entity}'"
                continue

            # Validate field
            fields = [f["field_name"] for f in schema[entity]]
            if field not in fields:
                errors[var_key] = f"Invalid field: '{field}' for entity '{entity}'"
                continue

        return {
            "is_valid": len(errors) == 0,
            "errors": errors
        }

if __name__ == '__main__':
    # Example usage
    helper = WhatsAppTemplateHelper()
    whatsapp_config = {
        "template_variables": {
            "1": "user.name",
            "2": "user.age",
            "3": "item.name"
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
            "2": "user.age",
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
