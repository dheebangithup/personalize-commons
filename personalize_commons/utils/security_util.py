import base64


class SecurityUtil:

    @staticmethod
    def encode_b64(value: str) -> str:
        """
        Encode a string into a URL-safe Base64 string.

        Args:
            value (str): The string to encode.

        Returns:
            str: URL-safe Base64 encoded string.
        """
        if value is None:
            return None
        return base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8")

    @staticmethod
    def decode_b64(value: str) -> str:
        """
        Decode a URL-safe Base64 string back to the original string.

        Args:
            value (str): The Base64 string to decode.

        Returns:
            str: Original decoded string.
        """
        if value is None:
            return None
        return base64.urlsafe_b64decode(value.encode("utf-8")).decode("utf-8")
