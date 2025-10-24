"""
Authentication helper for OAuth and Azure credential management.

Handles Microsoft OAuth flow using MSAL and creates Azure credentials
from user access tokens for blob storage operations.
"""

import os
from typing import Optional, Dict, Any
import msal
from azure.core.credentials import AccessToken, TokenCredential
from datetime import datetime, timedelta


class UserTokenCredential(TokenCredential):
    """Custom credential class that wraps a user's access token.

    This allows us to use a user-provided OAuth token with Azure SDK clients.
    """

    def __init__(self, access_token: str, expires_on: Optional[int] = None):
        """Initialize with access token.

        Args:
            access_token: The OAuth access token from Microsoft login
            expires_on: Unix timestamp when token expires (default: 1 hour from now)
        """
        self.access_token = access_token

        # Default expiry: 1 hour from now
        if expires_on is None:
            expires_on = int((datetime.now() + timedelta(hours=1)).timestamp())

        self.expires_on = expires_on

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        """Get the access token.

        Args:
            *scopes: Token scopes (ignored, we use the existing token)
            **kwargs: Additional arguments (ignored)

        Returns:
            AccessToken object with token and expiry
        """
        return AccessToken(self.access_token, self.expires_on)


class MSALHelper:
    """Helper class for Microsoft Authentication Library (MSAL) operations.

    Manages OAuth flow for user authentication and token acquisition.
    """

    # Azure Storage scope for user impersonation
    STORAGE_SCOPE = ["https://storage.azure.com/user_impersonation"]

    # Additional scopes for user profile info
    PROFILE_SCOPES = ["openid", "profile", "email"]

    def __init__(self, client_id: str, client_secret: str, tenant_id: str):
        """Initialize MSAL helper.

        Args:
            client_id: Azure AD application (client) ID
            client_secret: Azure AD application client secret
            tenant_id: Azure AD tenant (directory) ID
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id

        # Build authority URL
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"

        # Combine all required scopes
        self.scopes = self.STORAGE_SCOPE + self.PROFILE_SCOPES

        # Create MSAL confidential client application
        self.app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=self.authority
        )

    def get_authorization_url(self, redirect_uri: str, state: Optional[str] = None) -> str:
        """Generate the Microsoft login URL for OAuth flow.

        Args:
            redirect_uri: URL where Microsoft will redirect after login
            state: Optional state parameter for CSRF protection

        Returns:
            Authorization URL to redirect user to
        """
        auth_url = self.app.get_authorization_request_url(
            scopes=self.scopes,
            redirect_uri=redirect_uri,
            state=state
        )
        return auth_url

    def acquire_token_by_auth_code(
        self,
        code: str,
        redirect_uri: str
    ) -> Dict[str, Any]:
        """Exchange authorization code for access token.

        Args:
            code: Authorization code from OAuth callback
            redirect_uri: Same redirect URI used in authorization request

        Returns:
            Token response dictionary containing:
            - access_token: The access token
            - id_token_claims: User information (name, email, etc.)
            - expires_in: Token validity duration in seconds
        """
        result = self.app.acquire_token_by_authorization_code(
            code=code,
            scopes=self.scopes,
            redirect_uri=redirect_uri
        )

        return result

    def create_credential_from_token(
        self,
        access_token: str,
        expires_in: Optional[int] = None
    ) -> UserTokenCredential:
        """Create Azure credential from access token.

        Args:
            access_token: OAuth access token
            expires_in: Token validity duration in seconds (default: 3600)

        Returns:
            UserTokenCredential that can be used with Azure SDK
        """
        # Calculate expiry timestamp
        if expires_in:
            expires_on = int((datetime.now() + timedelta(seconds=expires_in)).timestamp())
        else:
            expires_on = None  # UserTokenCredential will set default

        return UserTokenCredential(access_token, expires_on)

    @staticmethod
    def extract_user_info(token_claims: Dict[str, Any]) -> Dict[str, str]:
        """Extract user information from ID token claims.

        Args:
            token_claims: ID token claims dictionary

        Returns:
            Dictionary with user information:
            - name: User's display name
            - email: User's email
            - oid: User's object ID
        """
        return {
            'name': token_claims.get('name', 'Unknown User'),
            'email': token_claims.get('preferred_username', token_claims.get('email', '')),
            'oid': token_claims.get('oid', ''),
            'tenant_id': token_claims.get('tid', '')
        }

    def is_token_valid(self, token_result: Dict[str, Any]) -> bool:
        """Check if token acquisition was successful.

        Args:
            token_result: Result from acquire_token_by_auth_code

        Returns:
            True if token is valid, False otherwise
        """
        return 'access_token' in token_result and 'error' not in token_result


def create_msal_helper_from_env() -> MSALHelper:
    """Create MSALHelper instance from environment variables.

    Requires environment variables:
    - AZURE_CLIENT_ID
    - AZURE_CLIENT_SECRET
    - AZURE_TENANT_ID

    Returns:
        Configured MSALHelper instance

    Raises:
        ValueError: If required environment variables are missing
    """
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')

    if not client_id:
        raise ValueError("AZURE_CLIENT_ID environment variable is required")
    if not client_secret:
        raise ValueError("AZURE_CLIENT_SECRET environment variable is required")
    if not tenant_id:
        raise ValueError("AZURE_TENANT_ID environment variable is required")

    return MSALHelper(client_id, client_secret, tenant_id)
