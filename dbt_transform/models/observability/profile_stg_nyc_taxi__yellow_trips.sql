-- models/observability/profile_stg_nyc_taxi__yellow_trips.sql

{{ 
    config(
        materialized='incremental',
        unique_key=['column_name', 'profiled_date'],
        tags=['observability', 'dq_check']
    ) 
}}

with raw_profile as (
    -- 利用 dbt-profiler 生成元数据分布
    {{ dbt_profiler.get_profile(relation=ref('stg_nyc_taxi__yellow_trips')) }}
),

enriched_profile as (
    select
        *,
        -- 将探查时间截断为日期，作为增量加载的主键之一
        cast(current_timestamp() as date) as profiled_date,
        -- 添加运行元数据，方便追溯
        '{{ invocation_id }}' as meta_dbt_invocation_id
    from raw_profile
)

select * from enriched_profile 
