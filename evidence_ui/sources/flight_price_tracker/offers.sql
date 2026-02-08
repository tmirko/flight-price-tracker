select
  run_date,
  observed_at_utc,
  route,
  outbound_date,
  currency,
  price,
  duration_minutes,
  stops,
  airlines,
  depart_time,
  arrive_time
from read_parquet('../data/flight_price_tracker/offers/run_date=*/*.parquet')
