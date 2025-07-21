import re


class MessageResolver:
    _instance = None
    # Supports ${user.name} or ${item.discount}
    PLACEHOLDER_PATTERN = re.compile(r"\$\{(user|item)\.([\w\d_]+)\}")

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MessageResolver, cls).__new__(cls)
        return cls._instance

    def resolve_message(self, template: str, user: dict, item: dict) -> str:
        # pre process objects
        item = self._handle_list_values(item)
        user = self._handle_list_values(user)

        return self.__resolve_message(template, user, item)


    def __resolve_message(self, template: str, user: dict, item: dict) -> str:
        def replacer(match):
            obj_type = match.group(1)
            field = match.group(2)
            data = user if obj_type == "user" else item
            return str(data.get(field, ""))

        return self.PLACEHOLDER_PATTERN.sub(replacer, template)


    def _handle_list_values(self, data: dict) -> dict[str, any]:
        """if any key has list value, convert it to string.

          Args:
              data (dict[str, any]): A dictionary containing values to be processed.

          Returns:
              dict[str, any]: A new dictionary with the same keys as the input, but with the list values processed as described.
          """
        if not data:
            return {}
            
        result = {}
        for key, value in data.items():
            if isinstance(value, list):  # list and non-empty
                result[key] = '' if value is None else str(value[0])
            else:
                result[key] = value
        return result

    def validate_template(self, template: str, user_fields: list[str] = None, item_fields: list[str] = None) -> tuple[bool, list[str]]:
        """
        Validates that all placeholders in the template exist in the provided fields.
        
        Args:
            template (str): The message template containing placeholders like ${user.name} or ${item.discount}
            user_fields (list[str], optional): List of valid user field names. Defaults to None.
            item_fields (list[str], optional): List of valid item field names. Defaults to None.
            
        Returns:
            tuple[bool, list[str]]: A tuple containing:
                - bool: True if all placeholders are valid, False otherwise
                - list[str]: List of invalid field names found in the template
        """
        if user_fields is None:
            user_fields = []
        if item_fields is None:
            item_fields = []
            
        invalid_fields = []
        
        # Find all placeholders in the template
        for match in self.PLACEHOLDER_PATTERN.finditer(template):
            obj_type = match.group(1)
            field = match.group(0)  # The full match including ${user.} or ${item.}
            field_name = match.group(2)  # Just the field name
            
            if obj_type == 'user' and field_name not in user_fields:
                invalid_fields.append(field)
            elif obj_type == 'item' and field_name not in item_fields:
                invalid_fields.append(field)
        
        return (len(invalid_fields) == 0, invalid_fields)
