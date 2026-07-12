{{ 
    config(
        materialized='table',
        tags=['dim', 'core']
    ) 
}}

with source_data as (
    select * from (
        values 
            ('1', 'Creative Mobile Technologies, LLC'),
            ('2', 'Curb Mobility, LLC'),
            ('6', 'Myle Technologies Inc'),
            ('7', 'Helix'),
            ('CMT', 'Creative Mobile Technologies, LLC'),
            ('VTS', 'VeriFone Transportation Systems, Inc.'),
            ('DDS', 'Digital Dispatch Systems, Inc.'),
            ('99', 'Unknown')
    ) as t(vendor_id, vendor_name) 
),


clean_vendor as (
    select
        -- 1. 主键对齐：显式转换为 String，确保与中间层的数据类型绝对一致
        -- 去除可能存在的首尾空格，防止 JOIN 失败
        cast(trim(vendor_id) as string) as vendor_id,
        
        -- 2. 维度属性：去除首尾空格，规范化命名
        cast(trim(vendor_name) as string) as vendor_name

    from source_data
)

select
    vendor_id,
    vendor_name,
    
    -- 3. [可选] 派生维度：标记是否为传统三大供应商 (CMT / VTS / DDS) 及其对应的数字 ID
    case 
        when vendor_id in ('1', 'CMT', '2', 'VTS', 'DDS', '6') then true
        else false
    end as is_legacy_major_vendor

from clean_vendor

-- 4. 数据质量保护：剔除空主键
where vendor_id is not null
