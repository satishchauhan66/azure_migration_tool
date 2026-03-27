# Author: Satish Chauhan

"""
Azure Key Vault client — fetch secrets used by ADF Migration.

Requires:
    pip install azure-identity azure-keyvault-secrets
"""

from typing import Dict, Optional, Callable

try:
    from azure.keyvault.secrets import SecretClient
    KEYVAULT_SDK_AVAILABLE = True
except ImportError:
    KEYVAULT_SDK_AVAILABLE = False

# Well-known Key Vault secret names -> logical field keys (ADF Migration tab).
DEFAULT_SECRET_MAP: Dict[str, str] = {
    # ADF fields
    "adf-subscription-id":  "adf_subscription_id",
    "adf-resource-group":   "adf_resource_group",
    "adf-factory-name":     "adf_factory_name",
    "adf-pipeline-name":    "adf_pipeline_name",
    # Control DB fields
    "ctrl-db-server":       "ctrl_server",
    "ctrl-db-name":         "ctrl_db",
    "ctrl-db-user":         "ctrl_user",
    "ctrl-db-password":     "ctrl_password",
    "ctrl-db-auth":         "ctrl_auth",
    # Environment
    "migration-environment": "environment",
}

# Short labels for UI help (same keys as DEFAULT_SECRET_MAP).
SECRET_NAME_LABELS: Dict[str, str] = {
    "adf-subscription-id": "ADF subscription ID",
    "adf-resource-group": "ADF resource group",
    "adf-factory-name": "ADF factory name",
    "adf-pipeline-name": "ADF pipeline name",
    "ctrl-db-server": "Control database server",
    "ctrl-db-name": "Control database name",
    "ctrl-db-user": "Control database user",
    "ctrl-db-password": "Control database password",
    "ctrl-db-auth": "Control database auth (entra_mfa, sql, …)",
    "migration-environment": "Environment (e.g. UAT)",
}


def fetch_secrets(
    vault_url: str,
    secret_names: Optional[Dict[str, str]] = None,
    logger: Optional[Callable[[str], None]] = None,
    credential=None,
) -> Dict[str, str]:
    """Fetch secrets from Azure Key Vault.

    Args:
        vault_url:    Full vault URL, e.g. https://my-vault.vault.azure.net
        secret_names: Mapping of Key Vault secret name -> logical field key.
                      Defaults to ``DEFAULT_SECRET_MAP``.
        logger:       Optional log callback.
        credential:   Optional Azure credential. If omitted, uses the same shared
                      credential as ``ADFClient`` (fewer duplicate MFA prompts).

    Returns:
        Dict mapping logical field key -> secret value for every secret
        that was found.  Missing secrets are silently skipped.
    """
    if not KEYVAULT_SDK_AVAILABLE:
        raise ImportError(
            "Azure Key Vault SDK not installed.  Run:\n"
            "  pip install azure-identity azure-keyvault-secrets"
        )

    log = logger or (lambda m: None)
    mapping = secret_names or DEFAULT_SECRET_MAP

    if credential is None:
        from azure_migration_tool.utils.azure_shared_credential import (
            get_shared_azure_credential,
        )

        credential = get_shared_azure_credential(log)

    client = SecretClient(vault_url=vault_url, credential=credential)

    results: Dict[str, str] = {}
    for kv_name, field_key in mapping.items():
        try:
            secret = client.get_secret(kv_name)
            results[field_key] = secret.value
            log(f"  [OK]  {kv_name}")
        except Exception as exc:
            err = str(exc)
            if "SecretNotFound" in err or "404" in err or "not found" in err.lower():
                log(f"  [--]  {kv_name}  (not found, skipped)")
            else:
                log(f"  [WARN]  {kv_name}: {exc}")

    return results
