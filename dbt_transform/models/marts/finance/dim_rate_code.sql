{{ 
    config(
        materialized='table',
        tags=['dim', 'core']
    ) 
}}

with source_data as (
    select * from (
        values 
            (1, 'Standard rate'),
            (2, 'JFK'),
            (3, 'Newark'),
            (4, 'Nassau or Westchester'),
            (5, 'Negotiated fare'),
            (6, 'Group ride'),
            (-1, 'Invalid/Unknown')
    ) as t(rate_code_id, rate_code_name)
)

select
    cast(rate_code_id as int) as rate_code_id,
    cast(trim(rate_code_name) as string) as rate_code_name,
    case 
        when rate_code_id in (2, 3) then true
        else false
    end as is_airport_trip,
    case 
        when rate_code_id = -1 then true
        else false
    end as is_invalid_code
from source_data 