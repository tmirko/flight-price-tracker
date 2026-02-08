"""Tests for SerpApi response normalization."""

from __future__ import annotations

import json
from pathlib import Path

from flight_price_tracker.normalize import cheapest_offer, extract_offers


def test_extract_offers_and_cheapest_offer() -> None:
    """Ensure we can extract offers and pick the cheapest one."""
    p = Path(__file__).parent / "fixtures" / "serpapi_google_flights_sample.json"
    resp = json.loads(p.read_text(encoding="utf-8"))

    offers = extract_offers(resp, outbound_date="2026-03-01", default_currency="USD")
    assert len(offers) == 2
    assert offers[0]["price"] == 123.0

    cheapest = cheapest_offer(offers)
    assert cheapest is not None
    assert cheapest["price"] == 123.0
    assert cheapest["airlines"] == "Air Test"
