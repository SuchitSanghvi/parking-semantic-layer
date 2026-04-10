/*
  dim_lots.sql
  ------------
  Clean lot dimension — pass-through from stg_lots with two
  computed attributes: days_since_activation and lot_capacity_tier.
*/

with lots as (

    select * from {{ ref('stg_lots') }}

),

final as (

    select
        lot_id,
        lot_name,
        city,
        state,
        zip,
        capacity,
        is_capacity_missing,
        market_type,
        activation_date,
        owner_name,

        -- how long has this lot been in the system
        datediff('day', activation_date, current_date)      as days_since_activation,

        -- capacity tier for segmentation: small < 75, medium 75–149, large >= 150
        case
            when capacity is null then 'unknown'
            when capacity < 75    then 'small'
            when capacity < 150   then 'medium'
            else                       'large'
        end                                                 as lot_capacity_tier

    from lots

)

select * from final
