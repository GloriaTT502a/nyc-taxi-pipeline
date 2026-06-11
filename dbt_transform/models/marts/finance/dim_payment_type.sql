-- models/marts/core/dim_payment_type.sql
with source_data as (
    select * from {{ ref('seed_payment_mapping') }}
)

select
    payment_type as payment_type_id, -- 规范化后缀
    payment_type_name,               -- 例如: 'Credit Card', 'Cash'
    is_cash_payment                  -- boolean 标签，方便前端做切片器
from source_data