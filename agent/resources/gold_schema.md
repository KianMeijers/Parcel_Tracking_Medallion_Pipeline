# Gold layer schema (read-only)

`query_gold` can run a single `SELECT` against exactly three views. Nothing
else is reachable - bronze and silver tables are never loaded into this
connection, and file/network access is disabled on the connection itself.
Results are capped at 200 rows; use `WHERE`/`GROUP BY`/aggregates to get
whole-dataset answers instead of relying on raw row dumps.

## `shipments` — one row per parcel

| column | type | nullable | meaning |
|---|---|---|---|
| parcel_id | int | no | internal parcel identifier |
| tracking_number | string | no | carrier's tracking number |
| organisation_id | int | no | shipping organisation (join to `dim_organisations`) |
| carrier_code | string | no | carrier (join to `dim_carriers`) |
| service_level | string | yes | carrier service tier |
| destination_country | string | yes | |
| destination_postcode | string | yes | |
| created_at | timestamp | no | parcel created in our system |
| handed_over_at | timestamp | yes | org handed the parcel to the carrier ("shipped") |
| accepted_at | timestamp | yes | carrier's first ACCEPTED scan ("accepted") |
| delivered_at | timestamp | yes | carrier's DELIVERED scan |
| latest_carrier_status | string | yes | most recent status seen from the carrier |
| weight_kg | double | yes | |
| facility_code | string | yes | facility of the latest carrier status |
| sla_hours | int | yes | carrier's SLA for this parcel, from `dim_carriers` |
| transit_hours | double | yes | `delivered_at - accepted_at`, in hours |
| is_delivered | bool | no | `delivered_at IS NOT NULL` |
| is_on_time | bool | yes | `transit_hours <= sla_hours` |
| is_lost | bool | yes | `latest_carrier_status = 'LOST'` |
| is_returned | bool | yes | `latest_carrier_status = 'RETURNED'` |
| _gold_loaded_at | timestamp | no | pipeline bookkeeping, not business data |

### Semantic caveats — read before writing transit-time or on-time queries

- **"Shipped" is not "accepted."** `handed_over_at` is the org's own dispatch
  record; `accepted_at` is the carrier's first scan. They are different
  moments and can be far apart. "How many parcels did org X ship" should
  count rows by `handed_over_at` (or just `created_at`/row count), not by
  `accepted_at`.
- **Transit time and on-time status are measured from carrier possession,
  not from hand-over.** `transit_hours` and `is_on_time` are computed as
  `accepted_at -> delivered_at`, matching the "transit time = from carrier
  possession to delivery" definition this project uses.
- **Nulls here are real, unresolved shipments, not bad data.** About 3,600
  of 500,000 parcels have a hand-over record but never appear in the
  carrier's own tracking feed (handed over, not yet or never scanned). For
  those rows `accepted_at`, `delivered_at`, `transit_hours`, and
  `is_on_time` are `NULL`. Still count them for shipment-volume questions;
  exclude/handle the nulls explicitly for on-time-rate questions (e.g.
  `WHERE is_on_time IS NOT NULL` when computing a rate, so unresolved
  parcels don't get miscounted as late or silently skew the denominator).
- **`is_on_time IS NULL` does not mean "no problem" — it also covers lost
  and returned parcels.** A parcel that's lost or returned never gets
  delivered, so it never resolves to on-time or late; filtering those rows
  out of an on-time-rate calculation (as the point above recommends) makes
  them invisible to that metric. For any "delivery performance" question,
  check `is_lost` / `is_returned` rates by carrier *in addition to*
  `is_on_time` — a carrier can have a flat on-time rate while its loss rate
  climbs, and only the latter would show it.

## `dim_carriers` — one row per carrier

| column | type | nullable | meaning |
|---|---|---|---|
| carrier_code | string | no | join key |
| carrier_name | string | no | |
| sla_hours | int | no | this carrier's SLA (already denormalized onto `shipments.sla_hours`) |
| countries_served | list\<string\> | yes | |

## `dim_organisations` — one row per organisation

| column | type | nullable | meaning |
|---|---|---|---|
| organisation_id | int | no | join key |
| name | string | yes | |
| country | string | yes | |
| plan | string | yes | subscription plan |
| created_at | timestamp | yes | |

## Join keys

- `shipments.carrier_code = dim_carriers.carrier_code`
- `shipments.organisation_id = dim_organisations.organisation_id`

## Example question shapes

- *How many parcels did organisation 4471 ship in July 2026, broken down by
  carrier?* → filter `shipments` on `organisation_id` and a `handed_over_at`
  (or `created_at`) month range, `GROUP BY carrier_code`.
- *Which carrier has the worst on-time delivery rate for parcels over 5
  kg?* → filter `shipments` on `weight_kg > 5 AND is_on_time IS NOT NULL`,
  `GROUP BY carrier_code`, compute `AVG(is_on_time::int)` or
  `SUM(is_on_time::int) / COUNT(*)`, order ascending.
- *Why did delivery performance drop in June?* → open-ended; compare
  `is_on_time` rates and `AVG(transit_hours)` month-over-month, broken down
  by `carrier_code`, `service_level`, and `destination_country`, to narrow
  down where the drop is concentrated. Also check `is_lost` and
  `is_returned` rates over the same breakdown — a rise in lost/returned
  parcels can depress delivery performance without moving `is_on_time` at
  all, since those parcels are excluded from that rate rather than counted
  against it.
