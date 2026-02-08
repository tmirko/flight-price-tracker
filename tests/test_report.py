"""Tests for Markdown report generation."""

from __future__ import annotations

from datetime import datetime, timezone

from flight_price_tracker.report import EvidenceRef, build_report_markdown


def test_report_includes_evidence_paths_and_hashes() -> None:
    """Report should include evidence paths, hashes, and currency information."""
    md = build_report_markdown(
        route="LHR-JFK",
        observed_at_utc=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
        currency="USD",
        rows=[{"outbound_date": "2026-03-10", "cheapest_price": 199.0}],
        evidence=[
            EvidenceRef(
                outbound_date="2026-03-10",
                json_path="evidence/route=LHR-JFK/run_date=2026-03-01/outbound_date=2026-03-10.json",
                sha256="deadbeef",
            )
        ],
        prev_prices={"2026-03-10": 210.0},
        top_k_deals=5,
    )

    assert "evidence/route=LHR-JFK" in md
    assert "sha256 `deadbeef`" in md
    assert "USD" in md
