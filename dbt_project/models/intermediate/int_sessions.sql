/*
  int_sessions.sql
  ----------------
  Core transformation: pairs ENTRY and EXIT events from stg_parking_events
  into one row per session.

  Matching strategy:
    1. Forward pass  — ASOF LEFT JOIN: each ENTRY gets the nearest EXIT
                       where exit_ts >= entry_ts (same lot + plate).
    2. Backward pass — for entries still unmatched after the forward pass,
                       look for an EXIT within 30 min BEFORE the entry
                       (clock-sync bug pattern). These produce negative durations.
    3. Orphaned exits — EXIT rows not consumed by either pass.

  All sessions are kept. The mart layer (fct_sessions) filters to valid ones.
*/

with events as (

    select * from {{ ref('stg_parking_events') }}

),

entries as (

    select
        event_id            as entry_event_id,
        lot_id,
        license_plate,
        event_timestamp     as entry_ts,
        payment_method,
        camera_id           as entry_camera_id
    from events
    where event_type = 'entry'

),

exits as (

    select
        event_id            as exit_event_id,
        lot_id,
        license_plate,
        event_timestamp     as exit_ts,
        amount_charged,
        payment_method,
        camera_id           as exit_camera_id
    from events
    where event_type = 'exit'

),

-- ── Step 1: forward pass ──────────────────────────────────────────────────
-- For each ENTRY, find the nearest EXIT where exit_ts >= entry_ts.
-- ASOF LEFT JOIN returns NULLs on the exit side when no forward exit exists.

forward_pairs as (

    select
        e.entry_event_id,
        x.exit_event_id,
        e.lot_id,
        e.license_plate,
        e.entry_ts,
        x.exit_ts,
        x.amount_charged,
        e.payment_method,
        e.entry_camera_id,
        x.exit_camera_id
    from entries e
    asof left join exits x
        on  e.lot_id        = x.lot_id
        and e.license_plate = x.license_plate
        and e.entry_ts     <= x.exit_ts

),

-- ── Step 2: backward pass (clock-sync bugs) ───────────────────────────────
-- Entries that had no forward exit — check for an EXIT just before them.

unmatched_entries as (

    select * from forward_pairs where exit_event_id is null

),

forward_used_exit_ids as (

    select exit_event_id from forward_pairs where exit_event_id is not null

),

backward_candidates as (

    select
        ue.entry_event_id,
        x.exit_event_id,
        ue.lot_id,
        ue.license_plate,
        ue.entry_ts,
        x.exit_ts,
        x.amount_charged,
        ue.payment_method,
        ue.entry_camera_id,
        x.exit_camera_id,
        row_number() over (
            partition by ue.entry_event_id
            order by datediff('second', x.exit_ts, ue.entry_ts) asc
        ) as rn
    from unmatched_entries ue
    join exits x
        on  ue.lot_id        = x.lot_id
        and ue.license_plate = x.license_plate
        and x.exit_ts        < ue.entry_ts
        and datediff('minute', x.exit_ts, ue.entry_ts) <= 30
        -- only use exits not already consumed in the forward pass
        and x.exit_event_id not in (select exit_event_id from forward_used_exit_ids)

),

backward_pairs as (

    select * exclude (rn) from backward_candidates where rn = 1

),

-- ── Step 3: orphaned exits ────────────────────────────────────────────────
-- EXIT rows not matched in either pass.

all_matched_exit_ids as (

    select exit_event_id from forward_used_exit_ids
    union all
    select exit_event_id from backward_pairs

),

orphaned_exits as (

    select
        null::varchar       as entry_event_id,
        x.exit_event_id,
        x.lot_id,
        x.license_plate,
        null::timestamp     as entry_ts,
        x.exit_ts,
        x.amount_charged,
        x.payment_method,
        null::varchar       as entry_camera_id,
        x.exit_camera_id
    from exits x
    where x.exit_event_id not in (select exit_event_id from all_matched_exit_ids)

),

-- ── Step 4: detect duplicate entries (camera misfires) ────────────────────
-- A second ENTRY for the same plate+lot within 120 seconds of a prior ENTRY.

entry_lag as (

    select
        entry_event_id,
        lot_id,
        license_plate,
        entry_ts,
        lag(entry_ts) over (
            partition by lot_id, license_plate
            order by entry_ts
        ) as prev_entry_ts
    from entries

),

duplicate_entry_ids as (

    select entry_event_id
    from entry_lag
    where prev_entry_ts is not null
      and datediff('second', prev_entry_ts, entry_ts) <= 120

),

-- ── Step 5: union all session types ──────────────────────────────────────

combined as (

    -- forward matched (and truly orphaned entries with no backward match)
    select * from forward_pairs
    where entry_event_id not in (select entry_event_id from backward_pairs)

    union all

    -- backward matched (clock-sync bugs → will have negative duration)
    select * from backward_pairs

    union all

    -- orphaned exits
    select * from orphaned_exits

),

-- ── Step 6: add duration ──────────────────────────────────────────────────

with_duration as (

    select
        *,
        case
            when entry_ts is not null and exit_ts is not null
            then datediff('minute', entry_ts, exit_ts)
        end as duration_minutes
    from combined

),

-- ── Step 7: final output with all flags ───────────────────────────────────

final as (

    select
        -- surrogate key: deterministic hash of the two event IDs
        md5(
            coalesce(entry_event_id, '') || '|' || coalesce(exit_event_id, '')
        )                                                               as session_id,

        entry_event_id,
        exit_event_id,
        lot_id,
        license_plate,
        entry_ts,
        exit_ts,
        duration_minutes,
        amount_charged,
        payment_method,
        entry_camera_id,
        exit_camera_id,

        -- data quality flags (keep all rows; mart layer filters)
        (entry_event_id is not null and exit_event_id is null)         as is_orphaned_entry,
        (entry_event_id is null     and exit_event_id is not null)     as is_orphaned_exit,
        (duration_minutes is not null and duration_minutes < 0)        as is_negative_duration,
        coalesce(
            entry_event_id in (select entry_event_id from duplicate_entry_ids),
            false
        )                                                               as is_duplicate_entry

    from with_duration

)

select * from final
