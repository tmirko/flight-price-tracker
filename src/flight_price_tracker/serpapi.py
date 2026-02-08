"""SerpApi client.

Uses dlt's verified REST API source to fetch JSON responses from SerpApi.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dlt.sources.rest_api import RESTAPIConfig


class SerpApiError(RuntimeError):
    """Raised when a SerpApi request fails or returns an unexpected response."""


def search_google_flights(
    *,
    api_key: str,
    params: Mapping[str, Any],
    timeout_seconds: float = 60.0,
) -> tuple[dict[str, Any], str]:
    """Search Google Flights via SerpApi.

    Args:
        api_key: SerpApi API key.
        params: Query params excluding the API key. This function adds `engine=google_flights`.
        timeout_seconds: Currently unused (reserved for future HTTP client configuration).

    Returns:
        A tuple of (parsed_response_json, raw_json_string).
    """
    from dlt.sources.rest_api import rest_api_resources

    merged = {**dict(params), "api_key": api_key, "engine": "google_flights"}

    # Note: we intentionally do not pass a custom `session` object here because it must be
    # serializable; some session implementations contain thread-locals and break extraction.
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://serpapi.com/",
        },
        "resources": [
            {
                "name": "search",
                "endpoint": {
                    "path": "search.json",
                    "params": merged,
                    "paginator": {"type": "single_page"},
                    "data_selector": "$",
                },
            }
        ],
    }

    try:
        (resource,) = list(rest_api_resources(config))
        items = list(resource)
    except Exception as e:  # noqa: BLE001
        raise SerpApiError(_redact_secret(str(e))) from e

    if not items:
        raise SerpApiError("SerpApi returned no data")

    data = items[0]
    if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict):
        data = data[0]
    if not isinstance(data, dict):
        raise SerpApiError(f"SerpApi returned unexpected type: {type(data)!r}")

    raw = json.dumps(data, ensure_ascii=False, sort_keys=True)
    return data, raw


def _redact_secret(text: str) -> str:
    """Best-effort redaction of secrets in error messages."""
    # Redact common query-string secret patterns.
    return re.sub(r"(api_key=)[^&\s]+", r"\1***", text)
