# -*- coding: utf-8 -*-
"""Point-in-time restore of an Azure SQL Managed Instance database to another MI via ARM."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

# ARM API versions (SQL MI / managed databases)
API_VERSION_DATABASE = "2023-08-01"
# Managed instance GET occasionally needs same or older family depending on cloud; try newest first.
API_VERSIONS_INSTANCE = ("2023-08-01", "2021-11-01")
ARM_SCOPE = "https://management.azure.com/.default"
BASE = "https://management.azure.com"

API_VERSION_SUBSCRIPTIONS = "2020-01-01"
API_VERSION_RESOURCE_GROUPS = "2021-04-01"

# System / non-user DB names to hide from restore pickers (MI may still list some).
_MANAGED_DB_EXCLUDE = frozenset(
    {
        "master",
        "model",
        "msdb",
        "tempdb",
        "distribution",
    }
)


def managed_database_id(subscription_id: str, resource_group: str, managed_instance: str, database: str) -> str:
    return (
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Sql/managedInstances/{managed_instance}/databases/{database}"
    )


def _headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }


def get_access_token(credential: Any) -> str:
    """Bearer token for management.azure.com."""
    tok = credential.get_token(ARM_SCOPE)
    return tok.token


def get_managed_instance_location(
    credential: Any,
    subscription_id: str,
    resource_group: str,
    managed_instance: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    GET managed instance to read ``location`` (required on database PUT).
    Returns (location, error_message).
    """
    try:
        token = get_access_token(credential)
        last_err: Optional[str] = None
        for api_ver in API_VERSIONS_INSTANCE:
            url = (
                f"{BASE}/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
                f"/providers/Microsoft.Sql/managedInstances/{managed_instance}"
                f"?api-version={api_ver}"
            )
            r = requests.get(url, headers=_headers(token), timeout=120)
            if r.status_code == 200:
                body = r.json()
                loc = body.get("location")
                if not loc:
                    return None, "Response missing 'location' for managed instance."
                return str(loc), None
            last_err = _format_http_error("GET managed instance", r)
        return None, last_err or "GET managed instance failed."
    except Exception as ex:
        return None, str(ex)


def normalize_restore_point_in_time(value: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse user input to ISO8601 UTC ending with Z (ARM-friendly).
    Accepts e.g. ``2026-04-07T14:30:00``, ``2026-04-07T14:30:00Z``, ``2026-04-07 14:30``.
    """
    s = (value or "").strip()
    if not s:
        return None, "Restore point in time is required."
    try:
        s_norm = s.replace("Z", "+00:00").replace(" ", "T", 1)
        if "T" not in s_norm and len(s_norm) == 10:
            s_norm = s_norm + "T00:00:00"
        dt = datetime.fromisoformat(s_norm)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        iso = dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        return iso, None
    except Exception as ex:
        return None, f"Invalid date/time: {ex}"


def _format_http_error(action: str, r: requests.Response) -> str:
    try:
        j = r.json()
        err = j.get("error") or {}
        msg = err.get("message") or r.text
        code = err.get("code") or ""
        return f"{action} failed HTTP {r.status_code} {code}: {msg[:2000]}"
    except Exception:
        return f"{action} failed HTTP {r.status_code}: {r.text[:2000]}"


@dataclass
class StartRestoreResult:
    ok: bool
    async_operation_url: Optional[str] = None
    http_status: int = 0
    error: Optional[str] = None
    response_body: Optional[Any] = None


def start_point_in_time_restore(
    credential: Any,
    *,
    target_subscription_id: str,
    target_resource_group: str,
    target_managed_instance: str,
    new_database_name: str,
    source_database_arm_id: str,
    restore_point_in_time_utc: str,
    location: str,
) -> StartRestoreResult:
    """
    PUT managed database with createMode PointInTimeRestore (cross-instance when
    ``source_database_arm_id`` points at another MI).
    """
    try:
        token = get_access_token(credential)
        url = (
            f"{BASE}/subscriptions/{target_subscription_id}/resourceGroups/{target_resource_group}"
            f"/providers/Microsoft.Sql/managedInstances/{target_managed_instance}/databases/{new_database_name}"
            f"?api-version={API_VERSION_DATABASE}"
        )
        payload = {
            "location": location,
            "properties": {
                "createMode": "PointInTimeRestore",
                "restorePointInTime": restore_point_in_time_utc,
                "sourceDatabaseId": source_database_arm_id.strip(),
            },
        }
        r = requests.put(url, headers=_headers(token), json=payload, timeout=300)
        if r.status_code not in (200, 201, 202):
            return StartRestoreResult(
                ok=False,
                http_status=r.status_code,
                error=_format_http_error("Start PITR restore", r),
                response_body=_safe_json(r),
            )
        async_url = r.headers.get("Azure-AsyncOperation") or r.headers.get("Operation-Location")
        if async_url:
            return StartRestoreResult(
                ok=True,
                async_operation_url=async_url.strip(),
                http_status=r.status_code,
                response_body=_safe_json(r),
            )
        # Synchronous completion (unusual for restore)
        return StartRestoreResult(ok=True, http_status=r.status_code, response_body=_safe_json(r))
    except Exception as ex:
        return StartRestoreResult(ok=False, error=str(ex))


def delete_managed_database(
    credential: Any,
    *,
    subscription_id: str,
    resource_group: str,
    managed_instance: str,
    database_name: str,
) -> StartRestoreResult:
    """
    DELETE an existing user database on a managed instance (ARM).
    Returns async poll URL when Azure returns 202, else ok for 200/204. 404 is treated as success (already gone).
    """
    try:
        token = get_access_token(credential)
        url = (
            f"{BASE}/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Sql/managedInstances/{managed_instance}/databases/{database_name}"
            f"?api-version={API_VERSION_DATABASE}"
        )
        r = requests.delete(url, headers=_headers(token), timeout=300)
        if r.status_code == 404:
            return StartRestoreResult(ok=True, http_status=404, response_body=_safe_json(r))
        if r.status_code not in (200, 202, 204):
            return StartRestoreResult(
                ok=False,
                http_status=r.status_code,
                error=_format_http_error("Delete managed database", r),
                response_body=_safe_json(r),
            )
        async_url = r.headers.get("Azure-AsyncOperation") or r.headers.get("Operation-Location")
        if async_url:
            return StartRestoreResult(
                ok=True,
                async_operation_url=async_url.strip(),
                http_status=r.status_code,
                response_body=_safe_json(r),
            )
        return StartRestoreResult(ok=True, http_status=r.status_code, response_body=_safe_json(r))
    except Exception as ex:
        return StartRestoreResult(ok=False, error=str(ex))


def _safe_json(r: requests.Response) -> Any:
    try:
        return r.json()
    except Exception:
        return r.text


def _arm_get_all_pages(credential: Any, first_url: str) -> Tuple[List[dict], Optional[str]]:
    """Follow ``nextLink`` until exhausted. Returns (merged ``value`` rows, error_message)."""
    rows: List[dict] = []
    url: Optional[str] = first_url
    try:
        while url:
            token = get_access_token(credential)
            r = requests.get(url, headers=_headers(token), timeout=180)
            if r.status_code != 200:
                return rows, _format_http_error("ARM list GET", r)
            body = r.json() or {}
            chunk = body.get("value") or []
            if isinstance(chunk, list):
                rows.extend(chunk)
            url = body.get("nextLink")
        return rows, None
    except Exception as ex:
        return rows, str(ex)


def list_subscriptions(credential: Any) -> Tuple[List[Dict[str, str]], Optional[str]]:
    """
    List enabled Azure subscriptions for the signed-in identity.
    Each item: ``subscription_id``, ``display_name``.
    """
    url = f"{BASE}/subscriptions?api-version={API_VERSION_SUBSCRIPTIONS}"
    raw, err = _arm_get_all_pages(credential, url)
    if err:
        return [], err
    out: List[Dict[str, str]] = []
    for v in raw:
        sid = (v.get("subscriptionId") or "").strip()
        if not sid and isinstance(v.get("id"), str):
            parts = v["id"].split("/")
            if len(parts) >= 3 and parts[1].lower() == "subscriptions":
                sid = parts[2]
        disp = (v.get("displayName") or sid or "Subscription").strip()
        state = (v.get("state") or "").strip()
        if state and state.lower() != "enabled":
            disp = f"{disp}  [{state}]"
        if sid:
            out.append({"subscription_id": sid, "display_name": disp})
    out.sort(key=lambda x: x["display_name"].lower())
    return out, None


def list_resource_groups(credential: Any, subscription_id: str) -> Tuple[List[str], Optional[str]]:
    url = (
        f"{BASE}/subscriptions/{subscription_id}/resourcegroups"
        f"?api-version={API_VERSION_RESOURCE_GROUPS}"
    )
    raw, err = _arm_get_all_pages(credential, url)
    if err:
        return [], err
    names = sorted({(v.get("name") or "").strip() for v in raw if (v.get("name") or "").strip()})
    return names, None


def list_managed_instances_in_resource_group(
    credential: Any, subscription_id: str, resource_group: str
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    """Each item: ``name``, ``resource_group``, ``location``."""
    last_err: Optional[str] = None
    for api_ver in API_VERSIONS_INSTANCE:
        url = (
            f"{BASE}/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Sql/managedInstances"
            f"?api-version={api_ver}"
        )
        raw, err = _arm_get_all_pages(credential, url)
        if err:
            last_err = err
            continue
        out: List[Dict[str, str]] = []
        for v in raw:
            name = (v.get("name") or "").strip()
            if not name:
                continue
            loc = (v.get("location") or "").strip()
            rg = resource_group
            out.append({"name": name, "resource_group": rg, "location": loc})
        out.sort(key=lambda x: x["name"].lower())
        return out, None
    return [], last_err or "Could not list managed instances."


def list_managed_databases(
    credential: Any, subscription_id: str, resource_group: str, managed_instance: str
) -> Tuple[List[str], Optional[str]]:
    url = (
        f"{BASE}/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Sql/managedInstances/{managed_instance}/databases"
        f"?api-version={API_VERSION_DATABASE}"
    )
    raw, err = _arm_get_all_pages(credential, url)
    if err:
        return [], err
    names: List[str] = []
    for v in raw:
        name = (v.get("name") or "").strip()
        if not name or name.lower() in _MANAGED_DB_EXCLUDE:
            continue
        props = v.get("properties") or {}
        # Skip deleted / dropping if status present
        st = (props.get("status") or "").strip().lower()
        if st in ("dropped", "deleting"):
            continue
        names.append(name)
    names.sort(key=lambda x: x.lower())
    return names, None


def poll_async_operation(
    credential: Any,
    async_operation_url: str,
    *,
    poll_interval_sec: float = 15.0,
    timeout_sec: float = 7200.0,
    log: Optional[Callable[[str], None]] = None,
) -> Tuple[bool, str, Optional[Any]]:
    """
    Poll ARM async operation URL until terminal state.
    Returns (success, message, last_json).
    """
    log = log or (lambda _m: None)
    deadline = time.monotonic() + timeout_sec
    last_body: Any = None
    while time.monotonic() < deadline:
        try:
            token = get_access_token(credential)
            r = requests.get(async_operation_url, headers=_headers(token), timeout=120)
            if r.status_code != 200:
                msg = _format_http_error("Poll async operation", r)
                log(msg)
                return False, msg, _safe_json(r)
            last_body = r.json()
            status = (last_body or {}).get("status")
            if not status:
                # Some payloads nest provisioning state
                status = (last_body or {}).get("properties", {}).get("status")
            status_str = str(status) if status is not None else ""
            log(f"Async status: {status_str}")
            sl = status_str.lower()
            if sl in ("succeeded", "completed"):
                return True, status_str or "Succeeded", last_body
            if sl in ("failed", "canceled", "cancelled"):
                err = (last_body or {}).get("error") or {}
                em = err.get("message") if isinstance(err, dict) else str(last_body)
                return False, em or status_str or "Failed", last_body
        except Exception as ex:
            log(f"Poll error (will retry): {ex}")
        time.sleep(poll_interval_sec)
    return False, "Timed out waiting for restore operation.", last_body
