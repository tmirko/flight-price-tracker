"""Main orchestration for a single tracking run."""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from flight_price_tracker.dlt_source import build_resources
from flight_price_tracker.normalize import cheapest_offer, extract_offers
from flight_price_tracker.report import EvidenceRef, build_report_markdown, load_previous_prices
from flight_price_tracker.serpapi import SerpApiError, search_google_flights
from flight_price_tracker.settings import AppConfig, EnvSettings, load_app_config

DATASET_NAME = "flight_price_tracker"


def run_once(*, config_path: Path) -> None:
    """Execute one tracking run.

    Fetches SerpApi Google Flights data for each outbound date in the configured window,
    writes evidence JSON+sha256, loads normalized tables to Parquet via dlt, and writes reports.

    Args:
        config_path: Path to the YAML configuration file.
    """
    config = load_app_config(config_path)
    env = EnvSettings()

    observed_at = datetime.now(timezone.utc)
    run_date = observed_at.date().isoformat()
    route = f"{config.route.origin}-{config.route.destination}"

    data_root = Path("data")
    evidence_root = Path("evidence")
    reports_root = Path("reports")

    prev_prices = load_previous_prices(
        data_root=data_root,
        dataset_name=DATASET_NAME,
        route=route,
        before_observed_at_utc=observed_at,
    )

    outbound_dates = _rolling_outbound_dates(
        start=observed_at.date() + timedelta(days=config.window.start_offset_days),
        days=config.window.window_days,
    )

    search_runs_rows: list[dict[str, Any]] = []
    offers_rows: list[dict[str, Any]] = []
    evidence_refs: list[EvidenceRef] = []

    for outbound_date in outbound_dates:
        params = _build_serpapi_params(config=config, outbound_date=outbound_date)

        try:
            resp, raw_json = search_google_flights(
                api_key=env.serpapi_api_key,
                params=params,
            )
        except SerpApiError as e:
            # Still record the run with missing price; evidence is not available.
            search_runs_rows.append(
                {
                    "run_date": run_date,
                    "observed_at_utc": observed_at,
                    "route": route,
                    "origin": config.route.origin,
                    "destination": config.route.destination,
                    "outbound_date": outbound_date,
                    "currency": config.serpapi.currency,
                    "cheapest_price": None,
                    "error": str(e),
                    "serpapi_params": json.dumps(params, sort_keys=True),
                }
            )
            continue

        evidence_json_path, evidence_sha = _write_evidence(
            evidence_root=evidence_root,
            route=route,
            run_date=run_date,
            outbound_date=outbound_date,
            raw_json=raw_json,
        )
        evidence_refs.append(
            EvidenceRef(
                outbound_date=outbound_date,
                json_path=evidence_json_path,
                sha256=evidence_sha,
            )
        )

        offers = extract_offers(
            resp,
            outbound_date=outbound_date,
            default_currency=config.serpapi.currency,
        )
        top_offers = offers[: config.serpapi.top_n_offers]
        cheapest = cheapest_offer(offers)

        search_runs_rows.append(
            {
                "run_date": run_date,
                "observed_at_utc": observed_at,
                "route": route,
                "origin": config.route.origin,
                "destination": config.route.destination,
                "outbound_date": outbound_date,
                "currency": config.serpapi.currency,
                "cheapest_price": None if cheapest is None else float(cheapest["price"]),
                "evidence_json_path": evidence_json_path,
                "evidence_sha256": evidence_sha,
                "serpapi_params": json.dumps(params, sort_keys=True),
                "serpapi_search_metadata_id": _get_search_metadata_id(resp),
            }
        )

        for i, o in enumerate(top_offers, start=1):
            offers_rows.append(
                {
                    "run_date": run_date,
                    "observed_at_utc": observed_at,
                    "route": route,
                    "outbound_date": outbound_date,
                    "rank": i,
                    "price": float(o["price"]),
                    "currency": o.get("currency") or config.serpapi.currency,
                    "bucket": o.get("bucket"),
                    "airlines": o.get("airlines"),
                    "depart_time": o.get("depart_time"),
                    "arrive_time": o.get("arrive_time"),
                    "duration_minutes": o.get("duration_minutes"),
                    "stops": o.get("stops"),
                }
            )

        if config.serpapi.rate_limit_seconds:
            time.sleep(config.serpapi.rate_limit_seconds)

    _load_with_dlt(data_root=data_root, search_runs_rows=search_runs_rows, offers_rows=offers_rows)

    report_rows = [
        r
        for r in search_runs_rows
        if r.get("cheapest_price") is not None and isinstance(r.get("outbound_date"), str)
    ]
    report_rows.sort(key=lambda r: str(r["outbound_date"]))

    md = build_report_markdown(
        route=route,
        observed_at_utc=observed_at,
        currency=config.serpapi.currency,
        rows=report_rows,
        evidence=evidence_refs,
        prev_prices=prev_prices,
        top_k_deals=config.reporting.top_k_deals,
    )

    reports_root.mkdir(parents=True, exist_ok=True)
    (reports_root / "latest.md").write_text(md, encoding="utf-8")
    if config.reporting.write_dated_report:
        (reports_root / f"{run_date}.md").write_text(md, encoding="utf-8")


def _load_with_dlt(
    *,
    data_root: Path,
    search_runs_rows: list[dict[str, Any]],
    offers_rows: list[dict[str, Any]],
) -> None:
    """Write normalized tables to Parquet via dlt.

    Args:
        data_root: Root folder where Parquet output will be written.
        search_runs_rows: Rows for the `search_runs` table.
        offers_rows: Rows for the `offers` table.
    """
    import dlt
    from dlt.destinations import filesystem

    data_root.mkdir(parents=True, exist_ok=True)

    destination = filesystem(
        bucket_url=str(data_root.resolve()),
        layout="{table_name}/run_date={YYYY}-{MM}-{DD}/{load_id}.{file_id}.{ext}",
    )

    pipeline = dlt.pipeline(
        pipeline_name=DATASET_NAME,
        destination=destination,
        dataset_name=DATASET_NAME,
    )

    pipeline.run(
        build_resources(search_runs_rows=search_runs_rows, offers_rows=offers_rows),
        loader_file_format="parquet",
    )


def _write_evidence(
    *,
    evidence_root: Path,
    route: str,
    run_date: str,
    outbound_date: str,
    raw_json: str,
) -> tuple[str, str]:
    """Persist the raw SerpApi JSON response and its SHA256.

    Args:
        evidence_root: Root evidence output folder.
        route: Route identifier in the form ORIGIN-DESTINATION.
        run_date: Run date (YYYY-MM-DD).
        outbound_date: Outbound date (YYYY-MM-DD) for the request.
        raw_json: Raw JSON string.

    Returns:
        Tuple of (relative_json_path, sha256_hex).
    """
    base = evidence_root / f"route={route}" / f"run_date={run_date}"
    base.mkdir(parents=True, exist_ok=True)

    json_rel = base / f"outbound_date={outbound_date}.json"
    json_rel.write_text(raw_json, encoding="utf-8")
    digest = sha256(raw_json.encode("utf-8")).hexdigest()
    (base / f"outbound_date={outbound_date}.sha256").write_text(digest + "\n", encoding="utf-8")

    return json_rel.as_posix(), digest


def _rolling_outbound_dates(*, start: date, days: int) -> list[str]:
    """Generate outbound dates for a rolling window.

    Args:
        start: First outbound date.
        days: Number of consecutive days.

    Returns:
        List of dates formatted as YYYY-MM-DD.
    """
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


def _get_search_metadata_id(resp: dict[str, Any]) -> str | None:
    """Extract SerpApi `search_metadata.id` from a response."""
    meta = resp.get("search_metadata")
    if isinstance(meta, dict):
        v = meta.get("id")
        if isinstance(v, str):
            return v
    return None


def _build_serpapi_params(*, config: AppConfig, outbound_date: str) -> dict[str, Any]:
    """Build SerpApi query parameters for a single outbound date.

    Args:
        config: Validated application config.
        outbound_date: Outbound date (YYYY-MM-DD).

    Returns:
        Mapping of query parameters for `search_google_flights`.
    """
    params: dict[str, Any] = {
        "type": "2",
        "departure_id": config.route.origin,
        "arrival_id": config.route.destination,
        "outbound_date": outbound_date,
        "hl": config.serpapi.hl,
        "gl": config.serpapi.gl,
        "currency": config.serpapi.currency,
        "adults": config.serpapi.adults,
        "travel_class": config.serpapi.travel_class,
        "deep_search": str(config.serpapi.deep_search).lower(),
    }

    if config.serpapi.include_airlines:
        params["include_airlines"] = ",".join(config.serpapi.include_airlines)
    if config.serpapi.exclude_airlines:
        params["exclude_airlines"] = ",".join(config.serpapi.exclude_airlines)

    return params
