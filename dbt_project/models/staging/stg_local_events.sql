with source as (

    select * from {{ ref('raw_local_events') }}

),

-- deduplicate on (event_date, city, event_name) case-insensitively,
-- keeping the first occurrence per the spec
deduped as (

    select
        *,
        row_number() over (
            partition by event_date, lower(city), lower(event_name)
            order by event_date
        ) as _row_num

    from source

),

cleaned as (

    select
        cast(event_date           as date)                  as event_date,
        cast({{ initcap('city') }} as varchar)              as city,
        cast(event_name           as varchar)               as event_name,
        cast(lower(event_type)    as varchar)               as event_type,
        cast(expected_attendance  as integer)               as expected_attendance

    from deduped

    where _row_num = 1

)

select * from cleaned
