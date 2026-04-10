-- assert_no_revenue_on_entry_events.sql
-- Expects 0 rows.
-- Entry events are camera triggers — no payment is collected at entry.
-- amount_charged must always be NULL on entry rows.
-- A non-null value indicates upstream data corruption or a mis-tagged event.

select event_id
from {{ ref('stg_parking_events') }}
where event_type = 'entry'
  and amount_charged is not null
