with source as (

    select * from {{ ref('raw_parking_events') }}

),

cleaned as (

    select
        cast(event_id         as varchar)                   as event_id,
        cast(lot_id           as varchar)                   as lot_id,
        cast(license_plate    as varchar)                   as license_plate,
        cast(lower(event_type) as varchar)                  as event_type,
        cast(event_timestamp  as timestamp)                 as event_timestamp,
        cast(amount_charged   as double)                    as amount_charged,
        cast(lower(nullif(payment_method, '')) as varchar)  as payment_method,
        cast(camera_id        as varchar)                   as camera_id,
        amount_charged is null                              as is_amount_missing

    from source

    -- drop any event_type that isn't entry or exit
    where upper(event_type) in ('ENTRY', 'EXIT')

)

select * from cleaned
