{{ 
    config(
        materialized='table', 
        tags=['marts', 'finance', 'variance_analysis']
    )
}}

-- 1. 基础聚合：使用维度表关联，确保财务口径一致
with base_metrics as (
    select 
        date_trunc('month', f.pickup_at) as trip_month, 
        
        -- 通过关联维度表获取名称，彻底消除硬编码
        coalesce(d.rate_code_name, 'Unknown') as rate_code_name, 

        sum(f.total_amount) as segment_revenue,
        count(f.trip_id) as segment_trip_count
    from {{ ref('fct_nyc_taxi_trips') }} f
    left join {{ ref('dim_rate_code') }} d 
        on f.rate_code_id = d.rate_code_id
    where f.is_valid_financial_logic = true 
    group by 1, 2
), 

-- 2. 窗口计算：获取上月营收及当月总营收
window_metrics as (
    select 
        *,
        -- 获取上月同维度营收
        lag(segment_revenue) over (
            partition by rate_code_name 
            order by trip_month 
        ) as prev_month_revenue,
        
        -- 计算当月总营收（所有 rate_code 的总和，用于计算贡献度）
        sum(segment_revenue) over (
            partition by trip_month
        ) as total_current_month_revenue,

        -- 计算上月总营收（用于分母计算）
        sum(lag(segment_revenue) over (
            partition by rate_code_name 
            order by trip_month 
        )) over (
            partition by trip_month
        ) as total_prev_month_revenue
    from base_metrics 
), 

-- 3. 差异分析：计算方差、增长率及对整体的贡献
variance_calculation as (
    select 
        trip_month,
        rate_code_name,
        segment_revenue,
        prev_month_revenue,
        
        -- 绝对差异
        (segment_revenue - coalesce(prev_month_revenue, 0)) as revenue_variance_amount, 

        -- MoM 增长率
        case 
            when coalesce(prev_month_revenue, 0) = 0 then null 
            else (segment_revenue - prev_month_revenue) / prev_month_revenue 
        end as segment_mom_growth_rate, 

        -- 对整体增长的贡献度
        case 
            when coalesce(total_prev_month_revenue, 0) = 0 then null 
            else (segment_revenue - coalesce(prev_month_revenue, 0)) / total_prev_month_revenue 
        end as contribution_to_total_growth 
    from window_metrics
) 

-- 4. 最终输出
select * 
from variance_calculation 
-- 过滤掉没有对比基准的第一个月（即没有上月数据的月份）
where prev_month_revenue is not null 
order by trip_month desc, revenue_variance_amount desc
