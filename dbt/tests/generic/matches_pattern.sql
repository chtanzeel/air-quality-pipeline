{# Fails for non-null values that don't match a POSIX regular expression. #}
{% test matches_pattern(model, column_name, pattern) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} !~ '{{ pattern }}'
{% endtest %}
