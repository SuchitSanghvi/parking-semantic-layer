with source as (

    select * from {{ ref('raw_parking_events') }}

),

cleaned as (

    select
        event_id,
        lot_id,
        license_plate,
        lower(event_type)                                   as event_type,
        cast(event_timestamp as timestamp)                  as event_timestamp,
        -- empty string → NULL, then cast to numeric
        cast(nullif(amount_charged, '') as double)          as amount_charged,
        lower(nullif(payment_method, ''))                   as payment_method,
        camera_id,
        case
            when nullif(amount_charged, '') is null then true
            else false
        end                                                 as is_amount_missing

    from source

    -- drop any event_type that isn't entry or exit
    where upper(event_type) in ('ENTRY', 'EXIT')

)

select * from cleaned
