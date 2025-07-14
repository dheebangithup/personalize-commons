from typing import Dict, Any, List, Optional

from pydantic import BaseModel, Field, ValidationError


class MessageTemplate(BaseModel):
    """Model for message templates with variable substitution."""
    channel: str = Field(..., description="Communication channel (e.g., WhatsApp, Email, SMS)")
    variables: List[str] = Field(
        default_factory=list,
        description="List of variable names used in the template"
    )
    body: str = Field(..., description="Message body with variable placeholders")
    subject: Optional[str] = Field(
        None,
        description="Message subject (optional, used in email)"
    )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MessageTemplate':
        """
        Create a MessageTemplate from a dictionary.

        Args:
            data: Dictionary containing message template data

        Returns:
            MessageTemplate: A new MessageTemplate instance

        Raises:
            ValueError: If required fields are missing or invalid
        """
        try:
            return cls(
                channel=data.get('channel', '').strip(),
                variables=data.get('variables', []),
                body=data.get('body', '').strip(),
                subject=data.get('subject', '').strip() if data.get('subject') else None
            )
        except Exception as e:
            raise ValueError(f"Failed to create MessageTemplate from dict: {str(e)}")

    def render(self, **kwargs) -> Dict[str, str]:
        """
        Render the template with provided variables.

        Args:
            **kwargs: Variable values to substitute in the template

        Returns:
            Dict with rendered message components

        Raises:
            ValueError: If required variables are missing
        """
        # Check for missing required variables
        missing_vars = [var for var in self.variables if var not in kwargs]
        if missing_vars:
            raise ValueError(f"Missing required variables: {', '.join(missing_vars)}")

        # Render the message body
        rendered_body = self.body.format(**kwargs)

        # If subject exists, render it as well
        rendered_subject = None
        if self.subject:
            rendered_subject = self.subject.format(**kwargs)

        return {
            'channel': self.channel,
            'body': rendered_body,
            'subject': rendered_subject,
            'variables_used': list(kwargs.keys())
        }