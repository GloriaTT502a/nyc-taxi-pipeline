-- tests/assert_bridge_weight_is_one.sql
-- 财务权重守恒校验：任何一个 LocationID 下所有网格的 cell_weight 之和必须为 1.0 (允许极小的浮点误差)

with weight_summary as (
    select 
        LocationID,
        sum(cell_weight) as total_weight
    from {{ ref('bridge_taxi_zone_h3') }}
    group by LocationID
)

-- dbt test 的逻辑是：返回的行数代表“未通过测试的脏数据”。
-- 如果所有 LocationID 的权重和都是 1.0，这个查询将返回 0 行，测试通过。
select *
from weight_summary
where abs(total_weight - 1.0) > 0.000001