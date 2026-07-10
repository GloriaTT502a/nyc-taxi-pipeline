-- macros/data_quality/get_dq_rule.sql

{% macro get_dq_rule(rule_name) %}
    
    {# 
       从 dbt_project.yml (或等效的 yaml 环境配置) 中读取字典。
       如果找不到对应的规则，默认返回 '1=1' 以避免 SQL 编译报错。
    #}
    {% set dq_rules = var('dq_rules', {}) %}
    {% set rule_logic = dq_rules.get(rule_name, "1=1") %}
    
    {{ return(rule_logic) }}

{% endmacro %}