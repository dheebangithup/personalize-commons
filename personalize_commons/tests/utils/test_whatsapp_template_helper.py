import unittest

from personalize_commons.utils.whatsapp_template_helper import WhatsAppTemplateHelper


class TestWhatsAppTemplateHelper(unittest.TestCase):

    def setUp(self):
        """Runs before each test."""
        self.helper = WhatsAppTemplateHelper()
        self.row = {
            "user": {"name": "John", "age": 25},
            "item": {"name": "boost", "rate": 43}
        }
        self.schema = {
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

    def test_basic_variable_resolution(self):
        whatsapp_config = {
            "template_variables": {
                "1": "user.name",
                "2": "user.age",
                "3": "item.name"
            }
        }
        expected = {"1": "John", "2": 25, "3": "boost"}
        result = self.helper.build_template_variables(whatsapp_config, self.row)
        self.assertEqual(result, expected)

    def test_missing_field_raises_keyerror(self):
        whatsapp_config = {
            "template_variables": {
                "1": "user.unknown_field"
            }
        }
        with self.assertRaises(KeyError):
            self.helper.build_template_variables(whatsapp_config, self.row)

    def test_partial_resolution(self):
        whatsapp_config = {
            "template_variables": {
                "1": "item.rate"
            }
        }
        expected = {"1": 43}
        result = self.helper.build_template_variables(whatsapp_config, self.row)
        self.assertEqual(result, expected)

    def test_build_template_variables_success(self):
        whatsapp_config = {
            "template_variables": {
                "1": "user.name",
                "2": "user.age",
                "3": "item.name"
            }
        }
        row = {
            "user": {"name": "John", "age": 25},
            "item": {"name": "Boost", "price": 43}
        }
        expected = {"1": "John", "2": 25, "3": "Boost"}
        result = self.helper.build_template_variables(whatsapp_config, row)
        self.assertEqual(result, expected)

    def test_validate_template_variables_success(self):
        template_vars = {
            "1": "user.name",
            "2": "user.age",
            "3": "item.name"
        }
        result = self.helper.validate_template_variables(template_vars, self.schema)
        self.assertTrue(result["is_valid"])
        self.assertEqual(result["errors"], {})

    def test_validate_template_variables_invalid_format(self):
        template_vars = {"1": "username"}  # missing "."
        result = self.helper.validate_template_variables(template_vars, self.schema)
        self.assertFalse(result["is_valid"])
        self.assertIn("1", result["errors"])
        self.assertIn("must be entity.field", result["errors"]["1"])

    def test_validate_template_variables_invalid_entity(self):
        template_vars = {"1": "order.id"}  # invalid entity
        result = self.helper.validate_template_variables(template_vars, self.schema)
        self.assertFalse(result["is_valid"])
        self.assertIn("1", result["errors"])
        self.assertIn("Invalid entity", result["errors"]["1"])

    def test_validate_template_variables_invalid_field(self):
        template_vars = {"1": "user.invalid_field"}
        result = self.helper.validate_template_variables(template_vars, self.schema)
        self.assertFalse(result["is_valid"])
        self.assertIn("1", result["errors"])
        self.assertIn("Invalid field", result["errors"]["1"])

if __name__ == "__main__":
    unittest.main()
