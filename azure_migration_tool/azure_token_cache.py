# Author: Sa-tish Chauhan

"""
Azure AD Token Cache Utility

This module provides token caching for Azure AD MFA authentication.
It authenticates once and caches the token locally for reuse across all database connections.
"""

import json
import os
import time
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime, timezone

try:
    from msal import PublicClientApplication, SerializableTokenCache
except ImportError:
    print("Warning: msal library not installed. Install with: pip install msal")
    print("Token caching will not be available.")
    PublicClientApplication = None
    SerializableTokenCache = None


# Azure AD configuration for SQL Database
SQL_DATABASE_SCOPE = "https://database.windows.net/.default"

# Well-known Microsoft public client ID for Azure services (works across all tenants)
# This is the client ID used by Azure CLI and other Microsoft tools
# Users can override this by setting AZURE_CLIENT_ID environment variable
DEFAULT_AZURE_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"  # Microsoft Azure CLI client ID (public, multi-tenant)

# Get client ID from environment variable or use default
AZURE_CLIENT_ID = os.environ.get("AZURE_CLIENT_ID", DEFAULT_AZURE_CLIENT_ID)


class AzureTokenCache:
    """Manages Azure AD token caching for SQL Database connections."""
    
    def __init__(self, cache_file: Optional[str] = None):
        """
        Initialize token cache.
        
        Args:
            cache_file: Path to token cache file (default: .azure_token_cache.json in user's home)
        """
        if PublicClientApplication is None:
            raise ImportError("msal library is required. Install with: pip install msal")
        
        self.cache_file = cache_file or str(Path.home() / ".azure_token_cache.json")
        self.token_cache = SerializableTokenCache()
        
        # Load existing cache if available
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = f.read()
                    if cache_data:
                        self.token_cache.deserialize(cache_data)
                        print(f"Loaded token cache from: {self.cache_file}")
            except Exception as e:
                print(f"Warning: Could not load token cache: {e}")
                import traceback
                traceback.print_exc()
        
        self.app = None
        self._cached_token: Optional[Dict] = None
        self._username: Optional[str] = None
        
        # Try to load cached token from separate file for quick access
        token_cache_file = self.cache_file.replace('.json', '_token.json')
        if os.path.exists(token_cache_file):
            try:
                with open(token_cache_file, 'r', encoding='utf-8') as f:
                    token_data = json.load(f)
                    cached_token = token_data.get('token')
                    if cached_token and self._is_token_valid(cached_token):
                        self._cached_token = cached_token
                        self._username = token_data.get('username')
                        expires_in = int((cached_token.get('expires_on', 0) - time.time()) / 60)
                        if expires_in > 0:
                            print(f"Loaded valid cached token (expires in {expires_in} minutes)")
            except Exception as e:
                # Ignore errors loading token file - will use MSAL cache instead
                pass
    
    def get_token(self, username: str, tenant_id: Optional[str] = None, refresh_if_expiring_soon: bool = True) -> Optional[str]:
        """
        Get access token for SQL Database. Uses cached token if available and valid.
        Tokens are cached persistently across script runs.
        
        Args:
            username: User principal name (email)
            tenant_id: Azure AD tenant ID (optional, will use common endpoint if not provided)
            refresh_if_expiring_soon: If True, proactively refresh token when < 5 minutes remaining (default: True)
        
        Returns:
            Access token string, or None if authentication failed
        """
        self._username = username
        
        # Check if we have a valid in-memory cached token
        if self._cached_token:
            expires_in = int((self._cached_token.get('expires_on', 0) - time.time()) / 60)
            
            # If token is still valid and not expiring soon, use it
            if expires_in > 5:
                print(f"Using cached token (expires in {expires_in} minutes)")
                return self._cached_token.get('access_token')
            
            # If token is expiring soon (< 5 minutes) and refresh_if_expiring_soon is True, refresh proactively
            if refresh_if_expiring_soon and expires_in > 0:
                print(f"Token expiring soon ({expires_in} minutes). Proactively refreshing...")
                # Try to refresh silently using refresh token
                refreshed_token = self._refresh_token_silent(username, tenant_id)
                if refreshed_token:
                    return refreshed_token
                # If silent refresh failed, continue to normal flow
            elif expires_in <= 0:
                print(f"Token expired (expired {abs(expires_in)} minutes ago). Will refresh...")
        
        # Initialize app first (needed to check accounts and use cache)
        authority = f"https://login.microsoftonline.com/{tenant_id}" if tenant_id else "https://login.microsoftonline.com/common"
        if self.app is None:
            self.app = PublicClientApplication(
                client_id=AZURE_CLIENT_ID,
                authority=authority,
                token_cache=self.token_cache
            )
        
        # Try to get token from persistent cache (silent acquisition)
        accounts = self._get_accounts()
        print(f"Checking token cache... Found {len(accounts)} cached account(s)")
        
        if accounts:
            # Try to find account matching the username
            matching_account = None
            for account in accounts:
                account_username = account.get('username', '').lower()
                if account_username == username.lower():
                    matching_account = account
                    break
            
            # Use matching account, or first account if no match
            account_to_use = matching_account or accounts[0]
            print(f"Attempting silent token acquisition for account: {account_to_use.get('username', 'unknown')}")
            
            # Try to acquire token silently first (uses cached refresh token)
            result = self.app.acquire_token_silent(
                scopes=[SQL_DATABASE_SCOPE],
                account=account_to_use
            )
            
            if result and 'access_token' in result:
                # Check if expires_on is set, if not calculate from expires_in
                if 'expires_on' not in result or result.get('expires_on', 0) == 0:
                    # MSAL sometimes returns 'expires_in' (seconds from now) instead of 'expires_on' (Unix timestamp)
                    expires_in_seconds = result.get('expires_in', 3600)  # Default 1 hour
                    result['expires_on'] = time.time() + expires_in_seconds
                
                self._cached_token = result
                self._save_cache()
                expires_in = int((result.get('expires_on', 0) - time.time()) / 60)
                if expires_in > 0:
                    print(f"[OK] Using cached token from previous session (expires in {expires_in} minutes)")
                    return result['access_token']
                else:
                    print(f"Token expired (expired {abs(expires_in)} minutes ago). Will refresh...")
            elif result and 'error' in result:
                # Token expired or invalid, will need to re-authenticate
                error_code = result.get('error', 'unknown')
                error_desc = result.get('error_description', '')
                print(f"Token acquisition failed: {error_code} - {error_desc}")
                if 'expired' in error_code.lower() or 'invalid_grant' in error_code.lower():
                    print("Token expired. Will re-authenticate.")
                else:
                    print("Will re-authenticate to get fresh token.")
        else:
            print("No cached accounts found. Will need to authenticate interactively.")
        
        # If silent acquisition failed, do interactive authentication
        return self._acquire_token_interactive(username, tenant_id)
    
    def _acquire_token_interactive(self, username: str, tenant_id: Optional[str] = None) -> Optional[str]:
        """Acquire token interactively (will prompt for MFA)."""
        authority = f"https://login.microsoftonline.com/{tenant_id}" if tenant_id else "https://login.microsoftonline.com/common"
        
        if self.app is None:
            self.app = PublicClientApplication(
                client_id=AZURE_CLIENT_ID,
                authority=authority,
                token_cache=self.token_cache
            )
        
        print(f"\n{'='*80}")
        print("Azure AD Authentication Required")
        print(f"User: {username}")
        print("You will be prompted to authenticate with MFA.")
        print(f"{'='*80}\n")
        
        try:
            result = self.app.acquire_token_interactive(
                scopes=[SQL_DATABASE_SCOPE],
                login_hint=username
            )
        except Exception as e:
            error_msg = str(e)
            if "AADSTS700016" in error_msg or "application" in error_msg.lower() and "not found" in error_msg.lower():
                print(f"\n⚠ Authentication Error: The application ID '{AZURE_CLIENT_ID}' is not registered in your tenant.")
                print(f"   This can happen if your organization restricts which applications can be used.")
                print(f"\n   Solutions:")
                print(f"   1. Contact your Azure AD administrator to register this application ID")
                print(f"   2. Or set a custom client ID via environment variable:")
                print(f"      set AZURE_CLIENT_ID=your-client-id")
                print(f"   3. Or use a registered application ID from your tenant")
                print(f"\n   Current client ID: {AZURE_CLIENT_ID}")
            raise
        
        if 'access_token' in result:
            # Check if expires_on is set, if not calculate from expires_in
            if 'expires_on' not in result or result.get('expires_on', 0) == 0:
                # MSAL sometimes returns 'expires_in' (seconds from now) instead of 'expires_on' (Unix timestamp)
                expires_in_seconds = result.get('expires_in', 3600)  # Default 1 hour
                result['expires_on'] = time.time() + expires_in_seconds
            
            self._cached_token = result
            # Force save cache immediately after authentication
            # MSAL automatically updates the token_cache, so we just need to save it
            self._save_cache()
            
            # Double-check: ensure MSAL cache is saved (sometimes has_state_changed doesn't trigger)
            try:
                cache_dir = Path(self.cache_file).parent
                cache_dir.mkdir(parents=True, exist_ok=True)
                with open(self.cache_file, 'w', encoding='utf-8') as f:
                    f.write(self.token_cache.serialize())
            except Exception as e:
                print(f"Warning: Could not save MSAL cache: {e}")
            
            expires_in = int((result.get('expires_on', 0) - time.time()) / 60)
            print(f"[OK] Authentication successful. Token cached for future use (expires in {max(0, expires_in)} minutes).\n")
            print(f"Token cache files saved:")
            print(f"  - MSAL cache: {self.cache_file}")
            print(f"  - Token file: {self.cache_file.replace('.json', '_token.json')}")
            print(f"Next run will use cached token automatically (no MFA prompt needed).\n")
            return result['access_token']
        else:
            error = result.get('error_description', result.get('error', 'Unknown error'))
            error_code = result.get('error', '')
            
            print(f"✗ Authentication failed: {error}\n")
            
            # Provide helpful error messages for common issues
            if "AADSTS700016" in error or ("application" in error.lower() and "not found" in error.lower()):
                print(f"⚠ The application ID '{AZURE_CLIENT_ID}' is not registered in your tenant.")
                print(f"\n   Solutions:")
                print(f"   1. Contact your Azure AD administrator to register this application ID")
                print(f"   2. Or set a custom client ID via environment variable:")
                print(f"      set AZURE_CLIENT_ID=your-client-id")
                print(f"   3. Or use a registered application ID from your tenant")
                print(f"\n   Current client ID: {AZURE_CLIENT_ID}")
                print(f"   You can find registered applications in Azure Portal:")
                print(f"   Azure Active Directory > App registrations\n")
            
            return None
    
    def _get_accounts(self) -> list:
        """Get cached accounts from token cache."""
        if self.app is None:
            # Initialize app if not already done (needed to read cache)
            authority = "https://login.microsoftonline.com/common"
            self.app = PublicClientApplication(
                client_id=AZURE_CLIENT_ID,
                authority=authority,
                token_cache=self.token_cache
            )
        return self.app.get_accounts()
    
    def _refresh_token_silent(self, username: str, tenant_id: Optional[str] = None) -> Optional[str]:
        """
        Proactively refresh token using silent acquisition (no browser prompt).
        This uses the cached refresh token to get a new access token.
        
        Returns:
            New access token string, or None if refresh failed
        """
        # Initialize app if needed
        authority = f"https://login.microsoftonline.com/{tenant_id}" if tenant_id else "https://login.microsoftonline.com/common"
        if self.app is None:
            self.app = PublicClientApplication(
                client_id=AZURE_CLIENT_ID,
                authority=authority,
                token_cache=self.token_cache
            )
        
        # Get accounts from cache
        accounts = self._get_accounts()
        if not accounts:
            return None
        
        # Find matching account
        matching_account = None
        for account in accounts:
            account_username = account.get('username', '').lower()
            if account_username == username.lower():
                matching_account = account
                break
        
        account_to_use = matching_account or accounts[0]
        
        # Try silent token acquisition (uses refresh token)
        result = self.app.acquire_token_silent(
            scopes=[SQL_DATABASE_SCOPE],
            account=account_to_use
        )
        
        if result and 'access_token' in result:
            # Check if expires_on is set, if not calculate from expires_in
            if 'expires_on' not in result or result.get('expires_on', 0) == 0:
                expires_in_seconds = result.get('expires_in', 3600)
                result['expires_on'] = time.time() + expires_in_seconds
            
            self._cached_token = result
            self._save_cache()
            expires_in = int((result.get('expires_on', 0) - time.time()) / 60)
            print(f"[OK] Token refreshed successfully (expires in {max(0, expires_in)} minutes)")
            return result['access_token']
        
        return None
    
    def get_valid_token(self, username: str, tenant_id: Optional[str] = None) -> Optional[str]:
        """
        Get a valid access token, proactively refreshing if expiring soon.
        This is the recommended method for long-running processes.
        
        Args:
            username: User principal name (email)
            tenant_id: Azure AD tenant ID (optional)
        
        Returns:
            Access token string, or None if authentication failed
        """
        return self.get_token(username, tenant_id, refresh_if_expiring_soon=True)
    
    def _is_token_valid(self, token: Dict) -> bool:
        """Check if token is still valid (not expired)."""
        if 'access_token' not in token:
            return False
        if 'expires_on' not in token:
            return False
        
        expires_on = token.get('expires_on', 0)
        # Add 5 minute buffer before expiry
        return expires_on > (time.time() + 300)
    
    def _save_cache(self):
        """Save token cache to file."""
        try:
            # Always save MSAL cache if it has changed
            if self.token_cache.has_state_changed:
                cache_dir = Path(self.cache_file).parent
                cache_dir.mkdir(parents=True, exist_ok=True)
                with open(self.cache_file, 'w', encoding='utf-8') as f:
                    f.write(self.token_cache.serialize())
                print(f"Saved MSAL token cache to: {self.cache_file}")
            
            # Also save the cached token separately for quick access
            if self._cached_token:
                token_cache_file = self.cache_file.replace('.json', '_token.json')
                cache_dir = Path(token_cache_file).parent
                cache_dir.mkdir(parents=True, exist_ok=True)
                with open(token_cache_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'token': self._cached_token,
                        'username': self._username,
                        'cached_at': datetime.now(timezone.utc).isoformat()
                    }, f, indent=2)
                print(f"Saved token to: {token_cache_file}")
        except Exception as e:
            print(f"Warning: Could not save token cache: {e}")
            import traceback
            traceback.print_exc()
    
    def clear_cache(self):
        """Clear the token cache."""
        self._cached_token = None
        self._username = None
        self.token_cache = SerializableTokenCache()
        if os.path.exists(self.cache_file):
            try:
                os.remove(self.cache_file)
            except Exception as e:
                print(f"Warning: Could not remove cache file: {e}")
        # Also remove token cache file
        token_cache_file = self.cache_file.replace('.json', '_token.json')
        if os.path.exists(token_cache_file):
            try:
                os.remove(token_cache_file)
            except Exception as e:
                print(f"Warning: Could not remove token cache file: {e}")


# Global token cache instance
_token_cache: Optional[AzureTokenCache] = None


def get_cached_token(username: str, tenant_id: Optional[str] = None, cache_file: Optional[str] = None, refresh_if_expiring_soon: bool = True) -> Optional[str]:
    """
    Get cached Azure AD token for SQL Database authentication.
    
    This function authenticates once and caches the token for reuse.
    Subsequent calls will use the cached token if it's still valid.
    For long-running processes, tokens are proactively refreshed when expiring soon.
    
    Args:
        username: User principal name (email)
        tenant_id: Azure AD tenant ID (optional)
        cache_file: Path to cache file (optional)
        refresh_if_expiring_soon: If True, proactively refresh token when < 5 minutes remaining (default: True)
    
    Returns:
        Access token string, or None if authentication failed
    """
    global _token_cache
    
    if _token_cache is None:
        try:
            _token_cache = AzureTokenCache(cache_file=cache_file)
        except ImportError:
            print("Warning: msal not available. Token caching disabled.")
            return None
    
    return _token_cache.get_token(username, tenant_id, refresh_if_expiring_soon=refresh_if_expiring_soon)


def clear_token_cache():
    """Clear the cached token (force re-authentication on next call)."""
    global _token_cache
    if _token_cache:
        _token_cache.clear_cache()

