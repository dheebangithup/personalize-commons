from typing import Any, Dict

class WhatsAppTemplateHelper:

    def build_template_variables(self, whatsapp_config: dict, row: Dict[str, Any]) -> Dict[str, Any]:
        # Sort template variables by placeholder number (1, 2, 3…)
        sorted_vars = sorted(
            whatsapp_config["template_variables"].items(),
            key=lambda x: int(x[0])
        )

        resolved = {}
        for key, value in sorted_vars:
            entity, field = value.split(".")
            resolved[key] = row[entity][field]

        return resolved


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
