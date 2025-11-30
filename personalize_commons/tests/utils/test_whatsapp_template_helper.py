import unittest
from unittest.mock import patch

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

    def test_custom_values_resolution(self):
        """Test that custom values (not starting with user. or item.) are passed through as-is."""
        whatsapp_config = {
            "template_variables": {
                "1": "Hello, {name}!",
                "2": "42",
                "3": "custom.value",
                "4": "https://example.com"
            }
        }
        expected = {
            "1": "Hello, {name}!",
            "2": "42",
            "3": "custom.value",
            "4": "https://example.com"
        }
        result = self.helper.build_template_variables(whatsapp_config, self.row)
        self.assertEqual(result, expected)

    def test_mixed_variables_resolution(self):
        """Test a mix of user/item variables and custom values."""
        whatsapp_config = {
            "template_variables": {
                "1": "Hello, {name}!",
                "2": "user.name",
                "3": "item.name",
                "4": "Custom value"
            }
        }
        expected = {
            "1": "Hello, {name}!",
            "2": "John",
            "3": "boost",
            "4": "Custom value"
        }
        result = self.helper.build_template_variables(whatsapp_config, self.row)
        self.assertEqual(result, expected)

    def test_non_string_values(self):
        """Test that non-string values are converted to strings."""
        whatsapp_config = {
            "template_variables": {
                "1": 42,
                "2": 3.14,
                "3": True,
                "4": None
            }
        }
        result = self.helper.build_template_variables(whatsapp_config, self.row)
        self.assertEqual(result, {"1": "42", "2": "3.14", "3": "True", "4": "None"})

    def test_missing_field_does_not_raise(self):
        """Test that missing fields in row don't raise exceptions."""
        whatsapp_config = {
            "template_variables": {
                "1": "user.unknown_field",
                "2": "item.missing"
            }
        }
        # This should not raise an exception
        result = self.helper.build_template_variables(whatsapp_config, self.row)
        self.assertEqual(result, {"1": "user.unknown_field", "2": "item.missing"})

    def test_validate_template_variables_success(self):
        """Test validation of valid template variables."""
        template_vars = {
            "1": "user.name",
            "2": "user.age",
            "3": "item.name"
        }
        result = self.helper.validate_template_variables(template_vars, self.schema)
        self.assertTrue(result["is_valid"])
        self.assertEqual(result["errors"], {})

    def test_validate_custom_values(self):
        """Test that custom values pass validation."""
        template_vars = {
            "1": "Hello, {name}!",
            "2": "42",
            "3": "custom.value"
        }
        result = self.helper.validate_template_variables(template_vars, self.schema)
        self.assertTrue(result["is_valid"])
        self.assertEqual(result["errors"], {})

    def test_validate_mixed_values(self):
        """Test a mix of valid, invalid, and custom values."""
        template_vars = {
            "1": "user.name",          # valid user field
            "2": "user.invalid_field",  # invalid user field
            "3": "item.name",          # valid item field
            "4": "item.invalid_field",  # invalid item field
            "5": "custom_value",        # custom value (no dot)
            "6": "custom.value",        # custom value (with dot)
            "7": "order.id"             # custom value (treated as is, not validated)
        }
        result = self.helper.validate_template_variables(template_vars, self.schema)
        
        # Should be invalid due to invalid fields
        self.assertFalse(result["is_valid"])
        
        # Check which variables have errors (only invalid user/item fields)
        self.assertIn("2", result["errors"], "Should have error for invalid user field")
        self.assertIn("4", result["errors"], "Should have error for invalid item field")
        
        # These should not have errors
        self.assertNotIn("1", result["errors"], "Valid user field should not have errors")
        self.assertNotIn("3", result["errors"], "Valid item field should not have errors")
        self.assertNotIn("5", result["errors"], "Custom value without dot should not have errors")
        self.assertNotIn("6", result["errors"], "Custom value with dot should not have errors")
        self.assertNotIn("7", result["errors"], "Non-user/non-item values should not be validated")
        
        # Check error messages
        self.assertIn("Invalid field", result["errors"]["2"], "Error message should mention invalid field")
        self.assertIn("Invalid field", result["errors"]["4"], "Error message should mention invalid field")

    def test_validate_template_variables_empty(self):
        """Test validation with empty input."""
        result = self.helper.validate_template_variables({}, self.schema)
        self.assertTrue(result["is_valid"])
        self.assertEqual(result["errors"], {})

    def test_validate_template_variables_invalid_schema(self):
        """Test validation with invalid schema."""
        # Test with None schema
        with self.assertRaises(ValueError) as context:
            self.helper.validate_template_variables({"1": "user.name"}, None)
        self.assertEqual(str(context.exception), "Schema must be a non-empty dictionary")
        
        # Test with empty dict schema
        with self.assertRaises(ValueError) as context:
            self.helper.validate_template_variables({"1": "user.name"}, {})

    def test_build_template_variables_empty_config(self):
        """Test with empty or invalid config."""
        self.assertEqual(self.helper.build_template_variables({}, self.row), {})
        self.assertEqual(self.helper.build_template_variables(None, self.row), {})
        self.assertEqual(self.helper.build_template_variables({"other_key": {}}, self.row), {})


if __name__ == "__main__":
    unittest.main()
