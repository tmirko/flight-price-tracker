"""Report generation and historical comparisons."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds


@dataclass(frozen=True)
class EvidenceRef:
    """Reference to an evidence payload stored on disk.

    Attributes:
        outbound_date: Outbound date (YYYY-MM-DD) this evidence corresponds to.
        json_path: Relative path to the evidence JSON file.
        sha256: SHA256 of the evidence JSON content.
    """

    outbound_date: str
    json_path: str
    sha256: str


def build_report_markdown(
    *,
    route: str,
    observed_at_utc: datetime,
    currency: str,
    rows: list[dict[str, Any]],
    evidence: list[EvidenceRef],
    prev_prices: dict[str, float] | None,
    top_k_deals: int,
) -> str:
    """Build the Markdown report content.

    Args:
        route: Route identifier in the form ORIGIN-DESTINATION.
        observed_at_utc: Timestamp of observation in UTC.
        currency: Currency code used for display.
        rows: Rows from the `search_runs` table for the current run.
        evidence: Evidence references for outbound dates in the run.
        prev_prices: Prior run prices keyed by outbound date (for deltas).
        top_k_deals: Number of cheapest dates to include in the Top deals section.

    Returns:
        Markdown report body.
    """
    ev_by_date = {e.outbound_date: e for e in evidence}

    lines: list[str] = []
    lines.append("# Flight price tracker report")
    lines.append("")
    lines.append(f"- Route: `{route}`")
    lines.append(f"- Observed at (UTC): `{observed_at_utc.isoformat()}`")
    lines.append("")

    lines.append("## Cheapest by outbound date")
    lines.append("")
    lines.append("| Outbound date | Cheapest | Δ vs prev | Evidence |")
    lines.append("|---|---:|---:|---|")

    for r in rows:
        od = r["outbound_date"]
        price = float(r["cheapest_price"])
        delta = None
        if prev_prices and od in prev_prices:
            delta = price - float(prev_prices[od])
        delta_s = "" if delta is None else _fmt_delta(delta, currency)
        ev = ev_by_date.get(od)
        ev_s = "" if ev is None else f"`{ev.json_path}` (`{ev.sha256}`)"
        lines.append(f"| {od} | {_fmt_money(price, currency)} | {delta_s} | {ev_s} |")

    lines.append("")
    lines.append("## Top deals")
    lines.append("")
    top = sorted(rows, key=lambda r: float(r["cheapest_price"]))[:top_k_deals]
    for r in top:
        od = r["outbound_date"]
        cp = float(r["cheapest_price"])
        lines.append(f"- `{od}`: {_fmt_money(cp, currency)}")

    lines.append("")
    lines.append("## Evidence")
    lines.append("")
    for ev in evidence:
        lines.append(f"- `{ev.outbound_date}`: `{ev.json_path}` (sha256 `{ev.sha256}`)")

    lines.append("")
    return "\n".join(lines)


def load_previous_prices(
    *,
    data_root: Path,
    dataset_name: str,
    route: str,
    before_observed_at_utc: datetime,
) -> dict[str, float] | None:
    """Load the most recent prior run's prices for delta calculations.

    Args:
        data_root: Root folder where the tracker writes Parquet output.
        dataset_name: dlt dataset name.
        route: Route identifier in the form ORIGIN-DESTINATION.
        before_observed_at_utc: Only consider runs observed strictly before this timestamp.

    Returns:
        Mapping of outbound_date -> cheapest_price for the latest prior run, or None.
    """
    search_runs_dir = data_root / dataset_name / "search_runs"
    if not search_runs_dir.exists():
        return None

    dataset = ds.dataset(str(search_runs_dir), format="parquet")
    if "cheapest_price" not in dataset.schema.names:
        return None
    table = dataset.to_table(
        columns=["observed_at_utc", "route", "outbound_date", "cheapest_price"]
    )  # type: ignore[arg-type]

    if table.num_rows == 0:
        return None

    py = table.to_pylist()
    filtered: list[dict[str, Any]] = []
    for row in py:
        if row.get("route") != route:
            continue
        obs = row.get("observed_at_utc")
        obs_dt = _coerce_datetime(obs)
        if obs_dt is None:
            continue
        if obs_dt >= before_observed_at_utc:
            continue
        filtered.append({**row, "_obs": obs_dt})

    if not filtered:
        return None

    latest_obs = max(r["_obs"] for r in filtered)
    latest_rows = [r for r in filtered if r["_obs"] == latest_obs]

    out: dict[str, float] = {}
    for r in latest_rows:
        od = r.get("outbound_date")
        cp = r.get("cheapest_price")
        if isinstance(od, str) and isinstance(cp, (int, float)):
            out[od] = float(cp)
    return out or None


def _fmt_money(amount: float, currency: str) -> str:
    """Format a currency amount for display."""
    return f"{currency} {amount:.2f}".rstrip("0").rstrip(".")


def _fmt_delta(delta: float, currency: str) -> str:
    """Format a delta amount with a sign."""
    sign = "+" if delta > 0 else ""
    return f"{sign}{_fmt_money(delta, currency)}"


def _coerce_datetime(value: Any) -> datetime | None:
    """Coerce a value to a datetime if possible.

    Args:
        value: Value from a Parquet table cell.

    Returns:
        Parsed datetime, or None if the value cannot be converted.
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None
