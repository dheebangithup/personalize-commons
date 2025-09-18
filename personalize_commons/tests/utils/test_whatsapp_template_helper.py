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


if __name__ == "__main__":
    unittest.main()
