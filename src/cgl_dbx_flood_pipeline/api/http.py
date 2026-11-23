"""Reusable thread-local HTTP execution without model-specific payloads."""

from threading import local

import requests

_THREAD_LOCAL = local()


def thread_session() -> requests.Session:
    """Reuse one requests session per worker thread."""
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        _THREAD_LOCAL.session = session
    return session


def post_json(
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, str],
    payload: dict[str, object],
    timeout: int = 30,
    verify: bool = True,
    session: requests.Session | None = None,
) -> dict[str, object]:
    """POST one JSON request and fail explicitly on HTTP errors."""
    response = (session if session is not None else thread_session()).post(
        url, headers=headers, params=params, json=payload, timeout=timeout, verify=verify
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Flood API returned HTTP {response.status_code}.")
    return response.json()
