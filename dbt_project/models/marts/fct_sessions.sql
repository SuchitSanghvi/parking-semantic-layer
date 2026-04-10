/*
  fct_sessions.sql
  ----------------
  Valid parking sessions only. Filters out:
    - Orphaned entries  (no matching exit)
    - Orphaned exits    (no matching entry)
    - Negative-duration sessions (clock-sync bugs)
    - Duplicate entries (camera misfires)

  Adds time-bucketing and calendar dimensions for analysis.
*/

with sessions as (

    select * from {{ ref('int_sessions') }}

),

valid as (

    select * from sessions
    where is_orphaned_entry    = false
      and is_orphaned_exit     = false
      and is_negative_duration = false
      and is_duplicate_entry   = false

),

final as (

    select
        session_id,
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

        -- calendar dimensions
        cast(entry_ts as date)                              as session_date,
        dayofweek(entry_ts)                                 as day_of_week_num,
        dayname(entry_ts)                                   as day_of_week,
        dayofweek(entry_ts) in (0, 6)                      as is_weekend,

        -- time-of-day bucket based on entry hour
        case
            when hour(entry_ts) between 5  and 11 then 'morning'
            when hour(entry_ts) between 12 and 16 then 'afternoon'
            when hour(entry_ts) between 17 and 20 then 'evening'
            else 'overnight'
        end                                                 as time_of_day_bucket

    from valid

)

select * from final
