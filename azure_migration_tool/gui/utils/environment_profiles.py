# Author: Satish Chauhan

"""Per-environment MI + blob profiles (dev / qa / uat / prod) for worker and MCP."""

from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional

STANDARD_ENVIRONMENTS = ("dev", "qa", "uat", "prod")

# Single-letter codes (e.g. db22q → qa) and URL/name tokens.
LETTER_TO_ENV = {"d": "dev", "q": "qa", "u": "uat", "p": "prod"}
WORD_TO_ENV = {
    "dev": "dev",
    "development": "dev",
    "qa": "qa",
    "qat": "qa",
    "uat": "uat",
    "stg": "uat",
    "stage": "uat",
    "staging": "uat",
    "prd": "prod",
    "prod": "prod",
    "production": "prod",
}


def _settings_path() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", os.path.expanduser("~"))) / "AzureMigrationTool"
    else:
        base = Path(os.path.expanduser("~")) / ".azure_migration_tool"
    base.mkdir(parents=True, exist_ok=True)
    return base / "environment_profiles.json"


def _default_profile_blank() -> Dict[str, Any]:
    return {
        "mi_server": "",
        "storage_account_url": "",
        "container": "sqlbackups",
        "blob_auth_mode": "managed_identity",
        "connection_string": "",
        "blob_folder_prefix": "",
        "target_managed_instance": True,
        "restore_user": "",
        "display_name": "",
        "owner_team": "",
        "owner_contact": "",
        "application": "",
        "catalog_notes": "",
        "database_name_patterns": [],
        "hostname_patterns": [],
        "storage_url_patterns": [],
    }


def _default_profiles() -> Dict[str, Any]:
    blank = _default_profile_blank()
    return {
        "default_environment": "uat",
        "profiles": {name: deepcopy(blank) for name in STANDARD_ENVIRONMENTS},
    }


def load_environment_profiles() -> Dict[str, Any]:
    path = _settings_path()
    if not path.is_file():
        return _default_profiles()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return _default_profiles()
        data.setdefault("default_environment", "uat")
        profiles = data.setdefault("profiles", {})
        if not isinstance(profiles, dict):
            profiles = {}
            data["profiles"] = profiles
        for name in STANDARD_ENVIRONMENTS:
            base = deepcopy(_default_profile_blank())
            existing = profiles.get(name)
            if isinstance(existing, dict):
                base.update(existing)
            profiles[name] = base
        return data
    except (OSError, json.JSONDecodeError):
        return _default_profiles()


def save_environment_profiles(data: Dict[str, Any]) -> None:
    path = _settings_path()
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def get_profile(environment: str) -> Optional[Dict[str, Any]]:
    env = (environment or "").strip().lower()
    if not env:
        return None
    if env in WORD_TO_ENV:
        env = WORD_TO_ENV[env]
    profiles = load_environment_profiles().get("profiles") or {}
    prof = profiles.get(env)
    return prof if isinstance(prof, dict) else None


def _normalize_env_name(raw: str) -> Optional[str]:
    s = (raw or "").strip().lower()
    if not s:
        return None
    if s in STANDARD_ENVIRONMENTS:
        return s
    return WORD_TO_ENV.get(s)


def _score_token_in_text(text: str, env: str) -> int:
    if not text:
        return 0
    t = text.lower()
    score = 0
    if env == "dev" and re.search(r"(?:^|[-_])dev(?:[-_.]|$)", t):
        score += 3
    if env == "qa" and re.search(r"(?:^|[-_])qa(?:[-_.]|$)|(?:^|[-_])qat(?:[-_.]|$)", t):
        score += 3
    if env == "uat" and re.search(r"(?:^|[-_])uat(?:[-_.]|$)|(?:^|[-_])stg(?:[-_.]|$)", t):
        score += 3
    if env == "prod" and re.search(
        r"(?:^|[-_])prod(?:[-_.]|$)|(?:^|[-_])prd(?:[-_.]|$)|production", t
    ):
        score += 3
    return score


def _pattern_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [p.strip() for p in value.replace("\n", ",").split(",") if p.strip()]
    if isinstance(value, list):
        return [str(p).strip() for p in value if str(p).strip()]
    return []


def _glob_to_regex(pattern: str) -> str:
    p = pattern.strip()
    if not p:
        return ""
    escaped = re.escape(p).replace(r"\*", ".*").replace(r"\?", ".")
    return f"^{escaped}$"


def _text_matches_patterns(text: str, patterns: List[str]) -> bool:
    if not text or not patterns:
        return False
    t = text.strip()
    for raw in patterns:
        if not raw:
            continue
        try:
            if re.search(_glob_to_regex(raw), t, re.IGNORECASE):
                return True
        except re.error:
            if raw.lower() in t.lower():
                return True
    return False


def _score_profile_catalog_patterns(
    database: str,
    restore_server: str,
    storage_url: str,
    profile: Dict[str, Any],
) -> int:
    score = 0
    db_patterns = _pattern_list(profile.get("database_name_patterns"))
    host_patterns = _pattern_list(profile.get("hostname_patterns"))
    url_patterns = _pattern_list(profile.get("storage_url_patterns"))
    if _text_matches_patterns(database, db_patterns):
        score += 8
    if _text_matches_patterns(restore_server, host_patterns):
        score += 6
    if _text_matches_patterns(storage_url, url_patterns):
        score += 4
    return score


def profile_semantic_summary(environment: str) -> Dict[str, Any]:
    """Catalog / semantic fields for an environment (no secrets)."""
    prof = get_profile(environment) or {}
    return {
        "environment": (environment or "").strip().lower(),
        "display_name": (prof.get("display_name") or "").strip(),
        "owner_team": (prof.get("owner_team") or "").strip(),
        "owner_contact": (prof.get("owner_contact") or "").strip(),
        "application": (prof.get("application") or "").strip(),
        "catalog_notes": (prof.get("catalog_notes") or "").strip(),
        "mi_server": (prof.get("mi_server") or "").strip(),
        "storage_account_url": (prof.get("storage_account_url") or "").strip(),
        "database_name_patterns": _pattern_list(prof.get("database_name_patterns")),
        "hostname_patterns": _pattern_list(prof.get("hostname_patterns")),
        "storage_url_patterns": _pattern_list(prof.get("storage_url_patterns")),
    }


def resolve_database_catalog(
    database: str,
    *,
    restore_server: str = "",
    storage_url: str = "",
) -> Dict[str, Any]:
    """
    Match a database name (and optional host/URL hints) to catalog metadata + MI mapping.
    """
    db = (database or "").strip()
    detection = detect_environment_from_hints(
        database=db,
        restore_server=restore_server,
        storage_url=storage_url,
    )
    env = detection.get("environment") or "uat"
    prof = get_profile(env) or {}
    match_reasons: List[str] = []
    if _text_matches_patterns(db, _pattern_list(prof.get("database_name_patterns"))):
        match_reasons.append(f"database_name_patterns ({env})")
    if _text_matches_patterns(restore_server, _pattern_list(prof.get("hostname_patterns"))):
        match_reasons.append(f"hostname_patterns ({env})")
    if detection.get("confidence") == "inferred":
        match_reasons.append(f"inferred:{env}")
    if detection.get("confidence") == "explicit":
        match_reasons.append("explicit environment")

    return {
        "database": db,
        "environment": env,
        "detection": detection,
        "match_reasons": match_reasons,
        "semantic": profile_semantic_summary(env),
        "restore_target": {
            "server": (prof.get("mi_server") or "").strip(),
            "target_managed_instance": bool(prof.get("target_managed_instance", True)),
            "user": (prof.get("restore_user") or "").strip(),
        },
        "blob": {
            "storage_account_url": (prof.get("storage_account_url") or "").strip(),
            "container": (prof.get("container") or "sqlbackups").strip(),
            "folder": (prof.get("blob_folder_prefix") or "").strip(),
        },
    }


def _score_db_letter_code(database: str) -> Dict[str, int]:
    """Patterns like db22q, db22u, _db22p → env from trailing letter."""
    scores: Dict[str, int] = {e: 0 for e in STANDARD_ENVIRONMENTS}
    if not database:
        return scores
    for m in re.finditer(r"(?i)db\d*([dqup])(?:\b|_|$)", database):
        letter = m.group(1).lower()
        env = LETTER_TO_ENV.get(letter)
        if env:
            scores[env] += 4
    return scores


def detect_environment_from_hints(
    *,
    environment: str = "",
    database: str = "",
    restore_server: str = "",
    storage_url: str = "",
) -> Dict[str, Any]:
    """
    Infer environment from explicit value, DB name, MI hostname, storage URL.
    Returns {environment, confidence, scores, ambiguous, candidates}.
    """
    explicit = _normalize_env_name(environment)
    if explicit:
        return {
            "environment": explicit,
            "confidence": "explicit",
            "scores": {explicit: 100},
            "ambiguous": False,
            "candidates": [explicit],
        }

    scores: Dict[str, int] = {e: 0 for e in STANDARD_ENVIRONMENTS}
    for env in STANDARD_ENVIRONMENTS:
        scores[env] += _score_token_in_text(restore_server, env)
        scores[env] += _score_token_in_text(storage_url, env)
        scores[env] += _score_token_in_text(database, env)

    db_letter = _score_db_letter_code(database)
    for env, pts in db_letter.items():
        scores[env] += pts

    profiles = load_environment_profiles().get("profiles") or {}
    for env in STANDARD_ENVIRONMENTS:
        prof = profiles.get(env)
        if isinstance(prof, dict):
            scores[env] += _score_profile_catalog_patterns(database, restore_server, storage_url, prof)

    ranked = sorted(scores.items(), key=lambda x: (-x[1], x[0]))
    top_score = ranked[0][1] if ranked else 0
    if top_score <= 0:
        default = _normalize_env_name(load_environment_profiles().get("default_environment") or "uat")
        return {
            "environment": default or "uat",
            "confidence": "default",
            "scores": scores,
            "ambiguous": False,
            "candidates": [default or "uat"],
        }

    winners = [env for env, sc in ranked if sc == top_score and sc > 0]
    ambiguous = len(winners) > 1
    chosen = winners[0] if winners else "uat"
    return {
        "environment": chosen,
        "confidence": "inferred" if not ambiguous else "ambiguous",
        "scores": scores,
        "ambiguous": ambiguous,
        "candidates": winners,
    }
