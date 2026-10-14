{# Smog season label for a local date: 1 October to 31 January, named by its starting year
   ('2024-25' covers 2024-10-01 .. 2025-01-31). Null outside the season. #}
{% macro smog_season(date_expr) -%}
    case
        when extract(month from {{ date_expr }}) >= 10
            then extract(year from {{ date_expr }})::int::text
                 || '-' || lpad(((extract(year from {{ date_expr }})::int + 1) % 100)::text, 2, '0')
        when extract(month from {{ date_expr }}) = 1
            then (extract(year from {{ date_expr }})::int - 1)::text
                 || '-' || lpad((extract(year from {{ date_expr }})::int % 100)::text, 2, '0')
    end
{%- endmacro %}
