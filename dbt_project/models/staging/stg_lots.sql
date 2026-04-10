with source as (

    select * from {{ ref('raw_lots') }}

),

cleaned as (

    select
        cast(lot_id           as varchar)                   as lot_id,
        cast(lot_name         as varchar)                   as lot_name,
        cast({{ initcap('city') }} as varchar)              as city,
        cast(upper(state)     as varchar)                   as state,
        cast(zip              as varchar)                   as zip,
        -- keep capacity as-is; NULL is preserved and flagged below
        cast(capacity         as integer)                   as capacity,
        capacity is null                                    as is_capacity_missing,
        -- standardize all market_type variants to snake_case
        cast(
            case lower(trim(market_type))
                when 'urban'     then 'urban'
                when 'suburban'  then 'suburban'
                when 'mixed-use' then 'mixed_use'
                when 'mixed use' then 'mixed_use'
                else lower(trim(market_type))
            end
        as varchar)                                         as market_type,
        -- handle two date formats: YYYY-MM-DD and MM/DD/YYYY
        cast(
            case
                when activation_date like '__/__/____'
                    then strptime(activation_date, '%m/%d/%Y')::date
                else cast(activation_date as date)
            end
        as date)                                            as activation_date,
        cast(owner_name       as varchar)                   as owner_name

    from source

)

select * from cleaned
