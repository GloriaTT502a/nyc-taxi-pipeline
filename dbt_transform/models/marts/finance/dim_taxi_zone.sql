{{ 
    config(
        materialized='table',
        tags=['dim', 'core', 'spatial']
    ) 
}}

with base_zones as (
    -- 业务属性：完全依赖 Seed
    select
        cast(LocationID as int) as location_id,
        cast(Borough as string) as borough,             
        cast(Zone as string) as zone_name,              
        cast(service_zone as string) as service_zone,   
        case 
            when service_zone = 'Airports' or Zone like '%Airport%' then true 
            else false 
        end as is_airport
    from {{ ref('seed_taxi_zone_lookup') }}
    where LocationID is not null
),

spatial_zones as (
    -- 空间属性：纯粹从 Silver 提取网格和边界，抛弃冗余的 borough/zone
    select 
        cast(LocationID as int) as location_id,
        raw_boundary_wkt,
        h3_cell
    from {{ source('databricks_ingest', 'taxi_zone_h3') }}
)

select
    b.location_id,
    b.borough,
    b.zone_name,
    b.service_zone,
    b.is_airport,
    s.raw_boundary_wkt,
    s.h3_cell
from base_zones b
left join spatial_zones s
    on b.location_id = s.location_id