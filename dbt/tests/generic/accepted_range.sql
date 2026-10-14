{# Fails for rows outside [min_value, max_value] (inclusive). Nulls pass; test them with not_null. #}
{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
select {{ column_name }}
from {{ model }}
where
    {%- if min_value is not none %} {{ column_name }} < {{ min_value }}{% endif %}
    {%- if min_value is not none and max_value is not none %} or{% endif %}
    {%- if max_value is not none %} {{ column_name }} > {{ max_value }}{% endif %}
{% endtest %}
