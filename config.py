"""
Configuration management for Azure File Manager.
Loads settings from .env file.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class for Azure Storage settings."""

    def __init__(self):
        """Initialize configuration from environment variables."""
        self.storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        self.tenant_id = os.getenv('AZURE_TENANT_ID')

        # Validate required configuration
        if not self.storage_account_name:
            raise ValueError(
                "AZURE_STORAGE_ACCOUNT_NAME is required. "
                "Please copy .env.example to .env and set your storage account name."
            )

    @property
    def account_url(self):
        """Get the Azure Storage account URL."""
        return f"https://{self.storage_account_name}.blob.core.windows.net"

    def __repr__(self):
        """String representation of configuration."""
        return f"Config(storage_account={self.storage_account_name}, tenant_id={self.tenant_id or 'Not set'})"


def get_config():
    """Get configuration instance."""
    return Config()
