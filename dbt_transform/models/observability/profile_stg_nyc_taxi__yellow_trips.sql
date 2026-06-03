{{ 
    config(
        materialized='incremental',
        unique_key=['trip_id', 'profiled_date'], 
        tags=['observability', 'dq_check']
    ) 
}}

with raw_profile as (
    -- 1. 调用宏生成当前表的全量探查结果
    {{ dbt_profiler.get_profile(relation=ref('stg_nyc_taxi__yellow_trips')) }}
),

enriched_profile as (
    select
        *,
        cast(profiled_at as date) as profiled_date 
        
    from raw_profile
)

select * from enriched_profile

-- 【注意】：
-- 这里不需要像传统的业务增量模型那样写 {% if is_incremental() %} where ... {% endif %}
-- 因为 dbt-profiler 每次都会输出该表最新的几十行统计指标（每个字段一行）。
-- dbt 会自动将这几十行带时间戳的统计结果追加（或合并）到底层的物理表中。