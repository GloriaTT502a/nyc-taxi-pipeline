{% macro map_payment_type(column_name) %} 
    case 
        when upper(trim(cast({{ column_name }} as string))) like 'CRE%' then 1
        when upper(trim(cast({{ column_name }} as string))) like 'CAS%' then 2
        when upper(trim(cast({{ column_name }} as string))) like 'NO%' then 3
        when upper(trim(cast({{ column_name }} as string))) like 'DIS%' then 4
        when upper(trim(cast({{ column_name }} as string))) like 'VOI%' then 6
        when cast({{ column_name }} as string) = '3' then 3
        else 5 -- Unknown
    end
{% endmacro %} 