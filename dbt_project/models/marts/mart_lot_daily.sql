/*
  mart_lot_daily.sql
  ------------------
  Pre-aggregated: one row per lot × calendar day.

  Peak concurrent sessions (for occupancy_rate) uses a sweep-line:
    - Each session contributes a +1 event at entry_ts and a -1 at exit_ts.
    - A running sum over those events gives concurrent count at any point in time.
    - We take the max per lot × day as peak_concurrent_sessions.
*/

with sessions as (

    select * from {{ ref('fct_sessions') }}

),

lots as (

    select lot_id, capacity, city from {{ ref('dim_lots') }}

),

local_events as (

    select event_date, lower(city) as city from {{ ref('stg_local_events') }}

),

-- ── Sweep-line for peak concurrent sessions ───────────────────────────────

sweep_events as (

    select lot_id, session_date, entry_ts as ts, 1  as delta from sessions
    union all
    select lot_id, session_date, exit_ts  as ts, -1 as delta from sessions

),

running_concurrent as (

    select
        lot_id,
        session_date,
        sum(delta) over (
            partition by lot_id, session_date
            order by ts
            rows between unbounded preceding and current row
        ) as concurrent_sessions
    from sweep_events

),

peak_concurrent as (

    select
        lot_id,
        session_date,
        max(concurrent_sessions) as peak_concurrent_sessions
    from running_concurrent
    group by lot_id, session_date

),

-- ── Base session aggregates ───────────────────────────────────────────────

session_aggs as (

    select
        lot_id,
        session_date,
        count(*)                            as total_sessions,
        sum(coalesce(amount_charged, 0))    as total_revenue,
        avg(duration_minutes)               as avg_duration_minutes,
        avg(coalesce(amount_charged, 0))    as avg_price_per_session
    from sessions
    group by lot_id, session_date

),

-- ── Combine and enrich ────────────────────────────────────────────────────

final as (

    select
        sa.lot_id,
        sa.session_date,
        sa.total_sessions,
        sa.total_revenue,
        sa.avg_duration_minutes,
        sa.avg_price_per_session,
        pc.peak_concurrent_sessions,

        -- explicit double cast on division to prevent integer truncation
        case
            when l.capacity is not null and l.capacity > 0
            then round(pc.peak_concurrent_sessions::double / l.capacity, 4)
        end                                 as occupancy_rate,

        case
            when l.capacity is not null and l.capacity > 0
            then round(sa.total_sessions::double / l.capacity, 4)
        end                                 as turnover_rate,

        exists (
            select 1
            from local_events le
            where le.event_date = sa.session_date
              and le.city       = lower(l.city)
        )                                   as has_local_event

    from session_aggs sa
    left join peak_concurrent pc
        on  sa.lot_id       = pc.lot_id
        and sa.session_date = pc.session_date
    left join lots l
        on  sa.lot_id = l.lot_id

)

select * from final
