-- assert_occupancy_not_exceeding_capacity.sql
-- Expects 0 rows.
-- Occupancy rate > 1.05 (105%) is physically impossible and signals
-- either a capacity data error or a session-matching bug in int_sessions.
-- A 5% tolerance is allowed for minor clock-sync edge cases.
-- Rows for lots with NULL capacity are excluded (no denominator).

select m.lot_id
from {{ ref('mart_lot_daily') }} m
join {{ ref('dim_lots') }} d
    on m.lot_id = d.lot_id
where m.occupancy_rate > 1.05
