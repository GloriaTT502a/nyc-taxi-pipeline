{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {# 核心魔法：如果是生产环境，直接使用我们在 yml 里配置的银牌/金牌，不要拼接！ #}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}

    {# 如果是你本地开发，依然拼接，变成 dev_yourname_silver，保护生产数据不被你弄脏 #}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}