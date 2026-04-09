with source as (

    select * from {{ ref('raw_lots') }}

),

cleaned as (

    select
        lot_id,
        lot_name,
        {{ initcap('city') }}                               as city,
        upper(state)                                        as state,
        zip,
        -- keep capacity as-is; NULL is preserved and flagged below
        capacity,
        capacity is null                                    as is_capacity_missing,
        -- standardize all market_type variants to snake_case
        case lower(trim(market_type))
            when 'urban'     then 'urban'
            when 'suburban'  then 'suburban'
            when 'mixed-use' then 'mixed_use'
            when 'mixed use' then 'mixed_use'
            else lower(trim(market_type))
        end                                                 as market_type,
        -- handle two date formats: YYYY-MM-DD and MM/DD/YYYY
        case
            when activation_date like '__/__/____'
                then strptime(activation_date, '%m/%d/%Y')::date
            else cast(activation_date as date)
        end                                                 as activation_date,
        owner_name

    from source

)

select * from cleaned
