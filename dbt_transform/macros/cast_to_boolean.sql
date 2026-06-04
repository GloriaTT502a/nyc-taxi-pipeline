{% macro cast_to_boolean(column_name) %}
    case 
        when upper(trim(cast({{ column_name }} as string))) in ('Y', '1', '1.0', 'TRUE', 'T') then true
        when upper(trim(cast({{ column_name }} as string))) in ('N', '0', '0.0', 'FALSE', 'F') then false
        else null 
    end
{% endmacro %}
