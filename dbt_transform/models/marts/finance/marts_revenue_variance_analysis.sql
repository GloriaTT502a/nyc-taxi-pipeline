{{
    config(
        materialized='table', 
        tags=['marts', 'finance', 'variance_analysis']
    )
}}

-- 1. basic merge: calculate trip, revenue by month
with monthly_segment_metrics as (
    select 
        data_trunc('month', pickup_at) as trip_month, 

        case rate_code_id
            when 1 then '1-Standard Rate'
            when 2 then '2-JFK Airport'
            when 3 then '3-Newark Airport'
            when 4 then '4-Nassau or Westchester'
            when 5 then '5-Negotiated Fare'
            when 6 then '6-Group Ride'
            else 'Unknown'
        end as rate_code_name, 

        sum(total_amount) as segment_revenue,
        count(trip_id) as segment_trip_count
    from {{ ref('int_nyc_taxi__yellow_trips_cleaned') }}
    where is_valid_financial_logic = true -- 🌟 只取财务上自洽的健康数据
    group by 1, 2
), 

-- 2. time window: use LAG function to get revenue from last month 
lagged_metrics as (
    select 
        trip_month, 
        rate_code_name, 
        segment_revenue, 
        segment_trip_count, 
        lag(segment_revenue) over (
            partition by rate_code_name 
            order by trip_month 
        ) as prev_month_segment_revenue 
    from monthly_segment_metrics 
), 

-- 3. Calculate the total monthly revenue
total_prev_month_metrics as (
    select 
        trip_month, 
        sum(prev_month_segment_revenue) as total_prev_month_revenue 
    from lagged_metrics 
    group by 1 
), 

-- 4. Calculate variance and contribution 
variance_calculation as (
    select 
        l.trip_month, 
        l.rate_code_name, 

        l.segment_revenue, 
        l.prev_month_segment_revenue, 
        (l.segment_revenue - coalesce(l.prev_month_segment_revenue, 0)) as revenue_variance_amount, 

        -- Segment Growth Rate 
        case 
            when l.prev_month_segment_revenue = 0 or l.prev_month_segment_revenue is null then null 
            else (l.segment_revenue - l.prev_month_segment_revenue) / l.prev_month_segment_revenue 
        end as segment_mom_growth_rate, 

        -- Contribution to Overall Growth 
        case 
            when t.total_prev_month_revenue = 0 or t.total_prev_month_revenue is null then null 
            else (l.segment_revenue - coalesce(l.prev_month_segment_revenue, 0)) / t.total_prev_month_revenue 
        end as contribution_to_total_growth 
    
    from lagged_metrics l 
    left join total_prev_month_metrics t 
        on l.trip_month = t.trip_month 
) 

select * from variance_calculation 
-- fill out the first month record due to without the previous month record 
where prev_month_segment_revenue is not null 
order by trip_month desc, revenue_variance_amount desc 
