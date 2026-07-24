{{ 

    config(

        materialized='table',

        tags=['dim', 'core', 'static']

    ) 

}}



with source_data as (

    select * from {{ ref('seed_vendor_mapping') }}

),


clean_vendor as (

    select

        -- 1. 主键对齐：显式转换为 String，确保与中间层的数据类型绝对一致

        -- 去除可能存在的首尾空格，防止 JOIN 失败

        cast(trim(vendor_id) as string) as vendor_id,

        

        -- 2. 维度属性：去除首尾空格，规范化命名

        cast(trim(vendor_name) as string) as vendor_name

    from source_data
    where vendor_id is not null 
    and trim(vendor_id) != '-1'

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