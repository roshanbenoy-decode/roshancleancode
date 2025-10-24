"""
Flask application configuration.

Manages configuration settings for the web application including
session management, security settings, and Azure OAuth configuration.
"""

import os
from datetime import timedelta


class WebConfig:
    """Configuration class for Flask web application."""

    # Flask core settings
    SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')

    # Session configuration
    SESSION_TYPE = 'filesystem'  # Store sessions in filesystem
    SESSION_PERMANENT = True
    PERMANENT_SESSION_LIFETIME = timedelta(hours=2)  # Sessions expire after 2 hours

    # Session cookie security settings
    SESSION_COOKIE_SECURE = os.getenv('FLASK_ENV') == 'production'  # HTTPS only in production
    SESSION_COOKIE_HTTPONLY = True  # Prevent JavaScript access to cookies
    SESSION_COOKIE_SAMESITE = 'Lax'  # CSRF protection

    # Azure OAuth settings
    AZURE_CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
    AZURE_CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')
    AZURE_TENANT_ID = os.getenv('AZURE_TENANT_ID')

    # Azure Storage settings (from existing config)
    AZURE_STORAGE_ACCOUNT_NAME = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')

    # Flask environment
    ENV = os.getenv('FLASK_ENV', 'development')
    DEBUG = ENV == 'development'

    # Application settings
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file upload

    @classmethod
    def validate(cls):
        """Validate that required configuration is present.

        Raises:
            ValueError: If required configuration is missing
        """
        errors = []

        if not cls.AZURE_CLIENT_ID:
            errors.append("AZURE_CLIENT_ID environment variable is required")

        if not cls.AZURE_CLIENT_SECRET:
            errors.append("AZURE_CLIENT_SECRET environment variable is required")

        if not cls.AZURE_TENANT_ID:
            errors.append("AZURE_TENANT_ID environment variable is required")

        if not cls.AZURE_STORAGE_ACCOUNT_NAME:
            errors.append("AZURE_STORAGE_ACCOUNT_NAME environment variable is required")

        if cls.ENV == 'production' and cls.SECRET_KEY == 'dev-secret-key-change-in-production':
            errors.append("FLASK_SECRET_KEY must be set to a random value in production")

        if errors:
            error_message = "Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_message)

    @classmethod
    def get_redirect_uri(cls, request):
        """Generate redirect URI based on current request.

        Args:
            request: Flask request object

        Returns:
            Full redirect URI for OAuth callback
        """
        # In production (Azure App Service), use HTTPS
        if cls.ENV == 'production':
            return request.url_root.replace('http://', 'https://') + 'callback'
        # In development, use the actual scheme
        return request.url_root + 'callback'


def init_app_config(app):
    """Initialize Flask app with configuration.

    Args:
        app: Flask application instance
    """
    app.config.from_object(WebConfig)

    # Validate configuration
    try:
        WebConfig.validate()
    except ValueError as e:
        print(f"\n{'='*60}")
        print("CONFIGURATION ERROR")
        print('='*60)
        print(str(e))
        print('='*60)
        print("\nPlease check your .env file and ensure all required variables are set.")
        print("See .env.example.web for reference.\n")
        raise
