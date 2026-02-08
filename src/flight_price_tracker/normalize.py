"""Normalization helpers for SerpApi Google Flights responses."""

from __future__ import annotations

import re
from typing import Any

_NUMBER_RE = re.compile(r"(\d+(?:[\.,]\d+)*)")


def extract_offers(
    response: dict[str, Any],
    *,
    outbound_date: str,
    default_currency: str | None = None,
) -> list[dict[str, Any]]:
    """Extract a sorted list of flight offers from a SerpApi response.

    Args:
        response: Parsed SerpApi JSON response.
        outbound_date: Outbound date (YYYY-MM-DD) used for this query.
        default_currency: Currency to assume when the response doesn't specify one.

    Returns:
        A list of offers sorted by price ascending.
    """
    offers: list[dict[str, Any]] = []

    seen_booking_tokens: set[str] = set()

    containers: list[dict[str, Any]] = [response]
    airports = response.get("airports")
    if isinstance(airports, list):
        for a in airports:
            if isinstance(a, dict):
                containers.append(a)

    for container in containers:
        for bucket_name in ("best_flights", "other_flights"):
            bucket = container.get(bucket_name)
            if not isinstance(bucket, list):
                continue
            for entry in bucket:
                if not isinstance(entry, dict):
                    continue

                booking_token = entry.get("booking_token")
                if isinstance(booking_token, str) and booking_token in seen_booking_tokens:
                    continue

                price, currency = _extract_price(entry, default_currency=default_currency)
                if price is None:
                    continue

                flights = entry.get("flights")
                if not isinstance(flights, list):
                    flights = []

                airlines = _extract_airlines(flights, entry)
                depart_time, arrive_time = _extract_times(flights)
                duration_minutes = _extract_duration_minutes(entry, flights)
                stops = _extract_stops(entry, flights)

                offers.append(
                    {
                        "outbound_date": outbound_date,
                        "bucket": bucket_name,
                        "price": price,
                        "currency": currency,
                        "airlines": airlines,
                        "depart_time": depart_time,
                        "arrive_time": arrive_time,
                        "duration_minutes": duration_minutes,
                        "stops": stops,
                    }
                )

                if isinstance(booking_token, str):
                    seen_booking_tokens.add(booking_token)

    offers.sort(
        key=lambda o: (o["price"], o.get("stops") if o.get("stops") is not None else 9999)
    )
    return offers


def cheapest_offer(offers: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the cheapest offer (by `price`).

    Args:
        offers: Offers as returned by :func:`extract_offers`.

    Returns:
        The cheapest offer, or None if there are no offers.
    """
    return min(offers, key=lambda o: o["price"], default=None)


def _extract_price(
    entry: dict[str, Any], *, default_currency: str | None
) -> tuple[float | None, str | None]:
    """Extract the price from a SerpApi flight entry.

    Args:
        entry: A single element of `best_flights` or `other_flights`.
        default_currency: Currency to use if the entry does not include one.

    Returns:
        Tuple of (price, currency). Price is None if not found.
    """
    for key in ("price", "total_price", "price_amount"):
        if key in entry:
            return _parse_price(entry.get(key), default_currency=default_currency)
    return None, default_currency


def _parse_price(value: Any, *, default_currency: str | None) -> tuple[float | None, str | None]:
    """Parse a price value that may appear in multiple SerpApi formats.

    Args:
        value: Price value (number, string, or mapping containing amount/currency).
        default_currency: Currency to use if not specified in the value.

    Returns:
        Tuple of (price, currency). Price is None if the value cannot be parsed.
    """
    if value is None:
        return None, default_currency

    if isinstance(value, (int, float)):
        return float(value), default_currency

    if isinstance(value, dict):
        amount = value.get("amount") or value.get("value")
        currency = value.get("currency") or value.get("currency_code") or default_currency
        if isinstance(amount, (int, float)):
            return float(amount), currency
        if isinstance(amount, str):
            amt, _ = _parse_price(amount, default_currency=currency)
            return amt, currency
        return None, currency

    if isinstance(value, str):
        m = _NUMBER_RE.search(value.replace(",", ""))
        if not m:
            return None, default_currency
        try:
            return float(m.group(1)), default_currency
        except ValueError:
            return None, default_currency

    return None, default_currency


def _extract_airlines(flights: list[Any], entry: dict[str, Any]) -> str | None:
    """Extract a de-duplicated airline list.

    Args:
        flights: List of per-segment dicts.
        entry: The top-level offer entry (used as a fallback).

    Returns:
        A comma-separated airline string, or None if unavailable.
    """
    airlines: list[str] = []
    for f in flights:
        if not isinstance(f, dict):
            continue
        a = f.get("airline") or f.get("airline_name")
        if isinstance(a, str) and a.strip():
            airlines.append(a.strip())
    if not airlines:
        a = entry.get("airline")
        if isinstance(a, str) and a.strip():
            airlines.append(a.strip())
    if not airlines:
        return None
    # de-dup while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for a in airlines:
        if a in seen:
            continue
        seen.add(a)
        out.append(a)
    return ", ".join(out)


def _extract_times(flights: list[Any]) -> tuple[str | None, str | None]:
    """Extract the departure time of the first segment and arrival time of the last.

    Args:
        flights: List of per-segment dicts.

    Returns:
        Tuple of (depart_time, arrive_time) as strings, or (None, None) if unavailable.
    """
    if not flights:
        return None, None
    first = flights[0] if isinstance(flights[0], dict) else None
    last = flights[-1] if isinstance(flights[-1], dict) else None
    if not first or not last:
        return None, None

    dep = first.get("departure_airport")
    arr = last.get("arrival_airport")
    dep_time = dep.get("time") if isinstance(dep, dict) else None
    arr_time = arr.get("time") if isinstance(arr, dict) else None
    return (
        dep_time if isinstance(dep_time, str) else None,
        arr_time if isinstance(arr_time, str) else None,
    )


def _extract_duration_minutes(entry: dict[str, Any], flights: list[Any]) -> int | None:
    """Extract total duration in minutes.

    Args:
        entry: The top-level offer entry.
        flights: List of per-segment dicts.

    Returns:
        Duration in minutes, or None if unavailable.
    """
    d = entry.get("total_duration") or entry.get("duration")
    if isinstance(d, int):
        return d
    if isinstance(d, str):
        m = _NUMBER_RE.search(d)
        if m:
            try:
                return int(float(m.group(1)))
            except ValueError:
                return None

    durations: list[int] = []
    for f in flights:
        if not isinstance(f, dict):
            continue
        fd = f.get("duration")
        if isinstance(fd, int):
            durations.append(fd)
    return sum(durations) if durations else None


def _extract_stops(entry: dict[str, Any], flights: list[Any]) -> int | None:
    """Extract the number of stops.

    Args:
        entry: The top-level offer entry.
        flights: List of per-segment dicts.

    Returns:
        Stops count, or None if unavailable.
    """
    stops = entry.get("stops")
    if isinstance(stops, int):
        return stops
    if flights:
        return max(len(flights) - 1, 0)
    return None
