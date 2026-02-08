"""Tests for run orchestration helpers."""

from __future__ import annotations

from flight_price_tracker.run import _build_serpapi_params
from flight_price_tracker.settings import AppConfig


def test_build_serpapi_params_includes_route_and_filters() -> None:
    """Params should include one-way type, route, and airline include filter."""
    config = AppConfig.model_validate(
        {
            "route": {"origin": "VIE", "destination": "TGD"},
            "serpapi": {
                "hl": "en",
                "gl": "at",
                "currency": "EUR",
                "adults": 1,
                "travel_class": 1,
                "deep_search": False,
                "include_airlines": ["OS"],
            },
        }
    )

    params = _build_serpapi_params(config=config, outbound_date="2026-03-01")

    assert params["type"] == "2"
    assert params["departure_id"] == "VIE"
    assert params["arrival_id"] == "TGD"
    assert params["outbound_date"] == "2026-03-01"
    assert params["currency"] == "EUR"
    assert params["include_airlines"] == "OS"


def test_build_serpapi_params_omits_filters_when_none() -> None:
    """Optional include/exclude airline filters should be omitted when unset."""
    config = AppConfig.model_validate(
        {
            "route": {"origin": "VIE", "destination": "TGD"},
            "serpapi": {
                "hl": "en",
                "gl": "at",
                "currency": "EUR",
                "adults": 1,
                "travel_class": 1,
                "deep_search": True,
            },
        }
    )

    params = _build_serpapi_params(config=config, outbound_date="2026-03-01")

    assert params["deep_search"] == "true"
    assert "include_airlines" not in params
    assert "exclude_airlines" not in params
