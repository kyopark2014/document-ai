"""Resolve ESS session user_id for testcase-generator scripts."""

from __future__ import annotations

import os
from pathlib import Path

# Path segments that must not be inferred as session user_id from storage paths.
_INFERRED_USER_DENY = frozenset(
    {
        "ess",
        "artifacts",
        "test_cases",
        "regulations",
        "projects",
        "drawings",
        "out",
        "converted",
        "upload",
        "skills",
        "tc",
        "default",
        "mnt",
        "workspace",
        "app-data",
        ".session_storage",
    }
)


def safe_user(user_id: str | None) -> str:
    raw = (user_id or "").strip() or "default"
    return (
        raw.replace("/", "_").replace("\\", "_").replace("..", "_")[:128] or "default"
    )


def infer_user_from_storage_path(path: str | None) -> str | None:
    """Extract ``{user}`` from ``…/{user}/artifacts/…`` or ``…/{user}/ess/…``."""
    if not path:
        return None
    try:
        parts = Path(os.path.abspath(os.path.expanduser(path))).parts
    except OSError:
        return None
    for idx, part in enumerate(parts):
        if part in ("artifacts", "ess") and idx > 0:
            candidate = parts[idx - 1]
            if candidate not in _INFERRED_USER_DENY:
                return candidate
    return None


def resolve_session_user_id(
    explicit: str | None = None,
    *,
    storage_path: str | None = None,
) -> str:
    """Prefer injected session env; do not infer ``ess`` from ``…/ess/artifacts/…`` paths."""
    for key in ("ESS_USER_ID", "AGENTCORE_USER_ID", "USER_ID"):
        val = (os.environ.get(key) or "").strip()
        if val:
            return safe_user(val)

    inferred = infer_user_from_storage_path(storage_path)
    if inferred:
        return safe_user(inferred)

    if explicit and explicit.strip():
        seg = safe_user(explicit.strip())
        if seg not in _INFERRED_USER_DENY:
            return seg

    return safe_user(explicit)
