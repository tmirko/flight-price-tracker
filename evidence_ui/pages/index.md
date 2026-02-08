---
title: Flight price tracker
---

```sql latest_search_runs
with latest_observed as (
  select max(observed_at_utc) as observed_at_utc
  from flight_price_tracker.search_runs
)
select
  outbound_date,
  currency,
  cheapest_price,
  error,
  ('/' || evidence_json_path) as evidence_url,
  evidence_sha256
from flight_price_tracker.search_runs
where observed_at_utc = (select observed_at_utc from latest_observed)
order by outbound_date
```

<DataTable data={latest_search_runs}>
  <Column id=outbound_date title="Outbound date" />
  <Column id=currency />
  <Column id=cheapest_price title="Cheapest" />
  <Column id=error />
  <Column id=evidence_url contentType=link linkLabel="Evidence JSON" openInNewTab=true />
  <Column id=evidence_sha256 title="sha256" />
</DataTable>
