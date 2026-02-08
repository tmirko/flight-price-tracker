select
  run_date,
  observed_at_utc,
  route,
  origin,
  destination,
  outbound_date,
  currency,
  cheapest_price,
  evidence_json_path,
  evidence_sha256,
  error
from read_parquet('../data/flight_price_tracker/search_runs/run_date=*/*.parquet')
