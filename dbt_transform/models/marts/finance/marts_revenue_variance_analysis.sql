{{ 
    config(
        materialized='table', 
        tags=['marts', 'finance', 'variance_analysis']
    )
}}

-- 1. 极速预聚合 (Pre-aggregation)
with fact_agg as (
    select 
        -- 优化：直接使用物理层面的年月分区字段，避免对亿级时间戳进行 date_trunc 运算
        partition_year_month as trip_month,
        rate_code_id,
        sum(total_amount) as segment_revenue,
        count(trip_id) as segment_trip_count
    from {{ ref('fct_nyc_taxi_trips') }}
    where is_valid_financial_logic = true 
    group by 1, 2
), 

-- 2. 轻量级维度关联 (Join After Agg)
joined_dim as (
    select 
        f.trip_month,
        -- 我们在 dim 表中已经做好了 -1 兜底，这里的 rate_code_name 绝对安全
        d.rate_code_name, 
        f.segment_revenue,
        f.segment_trip_count
    from fact_agg f
    left join {{ ref('dim_rate_code') }} d 
        on f.rate_code_id = d.rate_code_id
),

-- 3. 拆解窗口函数 第一层：只计算 Lag (上月营收) 和 当月总计
step1_lag as (
    select 
        *,
        -- 获取上月同维度营收
        lag(segment_revenue) over (
            partition by rate_code_name 
            order by trip_month 
        ) as prev_month_revenue,
        
        -- 计算当月总营收
        sum(segment_revenue) over (
            partition by trip_month
        ) as total_current_month_revenue
    from joined_dim 
), 

-- 4. 拆解窗口函数 第二层：基于第一层算出的 prev_month 计算上月总计，完美避开嵌套报错
step2_totals as (
    select 
        *,
        sum(prev_month_revenue) over (
            partition by trip_month
        ) as total_prev_month_revenue
    from step1_lag
),

-- 5. 差异与贡献度分析
variance_calculation as (
    select 
        trip_month,
        rate_code_name,
        segment_revenue,
        prev_month_revenue,
        
        -- 绝对差异
        (segment_revenue - coalesce(prev_month_revenue, 0)) as revenue_variance_amount, 

        -- MoM 增长率 (防范除以 0)
        case 
            when coalesce(prev_month_revenue, 0) = 0 then null 
            else (segment_revenue - prev_month_revenue) / prev_month_revenue 
        end as segment_mom_growth_rate, 

        -- 对整体增长的贡献度
        case 
            when coalesce(total_prev_month_revenue, 0) = 0 then null 
            else (segment_revenue - coalesce(prev_month_revenue, 0)) / total_prev_month_revenue 
        end as contribution_to_total_growth 
    from step2_totals
) 

-- 6. 最终输出
select * 
from variance_calculation 
-- 过滤掉没有对比基准的第一个月（即没有上月数据的月份）
where prev_month_revenue is not null 
order by trip_month desc, revenue_variance_amount desc
