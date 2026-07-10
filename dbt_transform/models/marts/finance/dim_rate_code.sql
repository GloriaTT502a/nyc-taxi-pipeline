{{ 
    config(
        materialized='table',
        tags=['dim', 'core']
    ) 
}}

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
from {{ ref('seed_rate_code_mapping') }}