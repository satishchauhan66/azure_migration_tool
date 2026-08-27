# -*- coding: utf-8 -*-
"""Point-in-time restore of an Azure SQL Managed Instance database to another MI via ARM."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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


def managed_instance_id(subscription_id: str, resource_group: str, managed_instance: str) -> str:
    return (
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Sql/managedInstances/{managed_instance}"
    )


def _subscription_from_arm_id(arm_id: str) -> Optional[str]:
    """Extract the subscription GUID from an ARM resource id (``/subscriptions/<id>/...``)."""
    parts = [p for p in (arm_id or "").strip().split("/") if p]
    if len(parts) >= 2 and parts[0].lower() == "subscriptions":
        return parts[1]
    return None


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


# A "latest" restore point is held slightly behind now: MI log backups land every few
# minutes, so a timestamp at the very edge of the window is often rejected.
LATEST_RESTORE_POINT_LAG_SECONDS = 360

# Values in the restore-point field that mean "work it out from the source database".
AUTO_RESTORE_POINT_KEYWORDS = frozenset({"", "latest", "auto", "now", "newest"})


def _to_arm_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def _parse_arm_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse an ARM timestamp to aware UTC, tolerating Z and >6 fractional digits."""
    s = (value or "").strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    if "." in s:
        head, _, tail = s.partition(".")
        digits = ""
        rest = ""
        for i, ch in enumerate(tail):
            if ch.isdigit():
                digits += ch
            else:
                rest = tail[i:]
                break
        s = f"{head}.{digits[:6]}{rest}" if digits else head + rest
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def parse_managed_database_arm_id(arm_id: str) -> Optional[Dict[str, str]]:
    """Split a managed database ARM id into subscription / resource group / instance / database."""
    parts = [p for p in (arm_id or "").strip().split("/") if p]
    lowered = [p.lower() for p in parts]
    try:
        return {
            "subscription_id": parts[lowered.index("subscriptions") + 1],
            "resource_group": parts[lowered.index("resourcegroups") + 1],
            "managed_instance": parts[lowered.index("managedinstances") + 1],
            "database": parts[lowered.index("databases") + 1],
        }
    except (ValueError, IndexError):
        return None


@dataclass
class RestoreWindow:
    """Bounds Azure will accept for a point-in-time restore of one managed database."""

    earliest: Optional[datetime] = None
    creation_date: Optional[datetime] = None
    error: Optional[str] = None

    @property
    def lower_bound(self) -> Optional[datetime]:
        return self.earliest or self.creation_date


def get_managed_database_restore_window(
    credential: Any,
    *,
    subscription_id: str,
    resource_group: str,
    managed_instance: str,
    database: str,
) -> RestoreWindow:
    """GET the managed database to read ``earliestRestorePoint`` and ``creationDate``."""
    try:
        token = get_access_token(credential)
        url = (
            f"{BASE}{managed_database_id(subscription_id, resource_group, managed_instance, database)}"
            f"?api-version={API_VERSION_DATABASE}"
        )
        r = requests.get(url, headers=_headers(token), timeout=120)
        if r.status_code != 200:
            return RestoreWindow(error=_format_http_error("GET managed database", r))
        props = (r.json() or {}).get("properties") or {}
        return RestoreWindow(
            earliest=_parse_arm_datetime(props.get("earliestRestorePoint")),
            creation_date=_parse_arm_datetime(props.get("creationDate")),
        )
    except Exception as ex:
        return RestoreWindow(error=str(ex))


def resolve_restore_point_in_time(
    credential: Any,
    *,
    source_database_arm_id: str,
    requested: str = "",
    log: Optional[Callable[[str], None]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Return a restore point Azure will accept, as ISO8601 UTC ending in Z.

    ``requested`` may be blank or ``latest``/``auto``/``now`` to take the most recent
    restorable point. An explicit timestamp is clamped into the database's real window,
    which is what Azure otherwise rejects with "The point in time ... is not valid.
    Valid point in time range from 7 days early to now and not before source server
    creation time."

    Returns (restore_point, error_message).
    """
    log = log or (lambda _m: None)
    latest = datetime.now(timezone.utc) - timedelta(seconds=LATEST_RESTORE_POINT_LAG_SECONDS)

    window = RestoreWindow()
    parts = parse_managed_database_arm_id(source_database_arm_id)
    if parts is None:
        log("(could not parse source database ARM id; skipping restore-window check)")
    else:
        window = get_managed_database_restore_window(credential, **parts)
        if window.error:
            log(f"(could not read restore window: {window.error})")

    wanted = (requested or "").strip()
    if wanted.lower() in AUTO_RESTORE_POINT_KEYWORDS:
        target = latest
        log(f"Restore point: latest available -> {_to_arm_utc(target)}")
    else:
        iso, err = normalize_restore_point_in_time(wanted)
        if err or not iso:
            return None, err or "Invalid restore time."
        parsed = _parse_arm_datetime(iso)
        if parsed is None:
            return None, f"Invalid date/time: {wanted}"
        target = parsed

    if target > latest:
        log(f"Restore point {_to_arm_utc(target)} is too recent; using {_to_arm_utc(latest)}")
        target = latest

    lower = window.lower_bound
    if lower is not None and target < lower:
        adjusted = min(lower + timedelta(minutes=1), latest)
        log(
            f"Restore point {_to_arm_utc(target)} is before this database's earliest restorable "
            f"point ({_to_arm_utc(lower)}); using {_to_arm_utc(adjusted)}"
        )
        target = adjusted

    if lower is not None and target < lower:
        return None, (
            f"No valid restore point available: earliest restorable point is {_to_arm_utc(lower)}, "
            f"which is later than the newest allowed point {_to_arm_utc(latest)}. "
            "The source database was created too recently — wait a few minutes and retry."
        )
    if window.earliest is not None:
        log(f"Source restore window: {_to_arm_utc(window.earliest)} .. {_to_arm_utc(latest)}")
    return _to_arm_utc(target), None


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
    PUT managed database with createMode PointInTimeRestore.

    - Same subscription (cross-instance or same-instance): uses ``sourceDatabaseId``.
    - Cross-subscription (source subscription differs from target): Azure requires
      ``crossSubscriptionTargetManagedInstanceId`` + ``crossSubscriptionSourceDatabaseId``
      instead. This is auto-detected from the source database ARM id.
    """
    try:
        token = get_access_token(credential)
        url = (
            f"{BASE}/subscriptions/{target_subscription_id}/resourceGroups/{target_resource_group}"
            f"/providers/Microsoft.Sql/managedInstances/{target_managed_instance}/databases/{new_database_name}"
            f"?api-version={API_VERSION_DATABASE}"
        )
        properties: Dict[str, Any] = {
            "createMode": "PointInTimeRestore",
            "restorePointInTime": restore_point_in_time_utc,
        }
        source_sub = _subscription_from_arm_id(source_database_arm_id)
        is_cross_subscription = bool(source_sub) and source_sub.lower() != (target_subscription_id or "").lower()
        if is_cross_subscription:
            properties["crossSubscriptionTargetManagedInstanceId"] = managed_instance_id(
                target_subscription_id, target_resource_group, target_managed_instance
            )
            properties["crossSubscriptionSourceDatabaseId"] = source_database_arm_id.strip()
        else:
            properties["sourceDatabaseId"] = source_database_arm_id.strip()
        payload = {
            "location": location,
            "properties": properties,
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
        token = get_access_token(credential)
        while url:
            r = requests.get(url, headers=_headers(token), timeout=180)
            if r.status_code == 401:
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
    started = time.monotonic()
    deadline = started + timeout_sec
    last_body: Any = None

    def _elapsed() -> str:
        secs = int(time.monotonic() - started)
        return f"{secs // 3600}h {(secs % 3600) // 60:02d}m" if secs >= 3600 else f"{secs // 60}m {secs % 60:02d}s"

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
            log(f"Async status: {status_str} (elapsed {_elapsed()})")
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
    return False, f"Timed out waiting for restore operation after {_elapsed()}.", last_body
