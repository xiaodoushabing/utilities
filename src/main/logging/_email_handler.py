from notifiers.providers.email import SMTP
import os
from typing import List, Dict, Any, Optional


def send_email(email_config: Dict[str, Any],
               message: str,
               subject: str = None, 
               to: Optional[List[str]] = None,        
               html: bool = None,
               attachments: Optional[List[str]] = None
) -> bool:
    """
    Send email using notifiers.
    
    Args:
        email_config (Dict[str, Any]): Email configuration dict
        message (str): Email message body
        subject (str): Email subject
        to (Optional[List[str]]): Override recipients (if None, uses email_config['to']). Defaults to None.
        html (bool): Whether the message is parsed as HTML. Defaults to None.
        attachments (Optional[List[str]]): List of file paths to attach. Defaults to None.

    Returns:
        bool: Result if successful, False otherwise
    """

    try:
        # Validate required fields
        required_fields = ['from', 'to', 'host', 'port']
        for field in required_fields:
            if field not in email_config:
                raise ValueError(f"Required field '{field}' missing from email config")
        
        # Determine values with following precedence: argument > email_config > default
        to_emails = to if to is not None else email_config.get('to')
        email_subject = subject if subject is not None else email_config.get('subject', '[LOG] Message')
        email_html = html if html is not None else email_config.get('html', False)

        # Update config for notifiers
        email_config.update({
            'to': to_emails,
            'message': message,
            'subject': email_subject,
            'html': email_html,
        })
        
        # Add attachments if provided and valid
        if isinstance(attachments, list):
            valid_attachments = [f for f in attachments if os.path.exists(f)]
            if valid_attachments:
                email_config['attachments'] = valid_attachments
        
        # Send the email
        res = SMTP().notify(**email_config)
        return res
        
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False

def create_email_sink_from_config(email_config: Dict[str, Any]):
    """
    Create a loguru sink function that sends emails based on the provided config.
    """
    def email_sink(message):
        log_message = str(message).strip()
        send_email(email_config=email_config, message=log_message)

    return email_sink
