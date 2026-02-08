"""DLT resources used for writing normalized tables."""

from __future__ import annotations

from typing import Any


def build_resources(
    *,
    search_runs_rows: list[dict[str, Any]],
    offers_rows: list[dict[str, Any]],
) -> list[Any]:
    """Create dlt resources for the output tables.

    Args:
        search_runs_rows: Rows for the `search_runs` table.
        offers_rows: Rows for the `offers` table.

    Returns:
        A list of configured dlt resources.
    """
    import dlt

    return [
        dlt.resource(
            search_runs_rows,
            name="search_runs",
            write_disposition="append",
            columns={
                "cheapest_price": {"data_type": "double"},
                "error": {"data_type": "text"},
                "serpapi_params": {"data_type": "text"},
                "evidence_json_path": {"data_type": "text"},
                "evidence_sha256": {"data_type": "text"},
            },
        ),
        dlt.resource(
            offers_rows,
            name="offers",
            write_disposition="append",
            columns={
                "price": {"data_type": "double"},
                "airlines": {"data_type": "text"},
                "depart_time": {"data_type": "text"},
                "arrive_time": {"data_type": "text"},
            },
        ),
    ]
