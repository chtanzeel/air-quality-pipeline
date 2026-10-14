{# US AQI category (EPA breakpoints). Returns null for a null AQI. #}
{% macro aqi_band(aqi) -%}
    case
        when {{ aqi }} is null then null
        when {{ aqi }} <= 50 then 'Good'
        when {{ aqi }} <= 100 then 'Moderate'
        when {{ aqi }} <= 150 then 'Unhealthy for Sensitive Groups'
        when {{ aqi }} <= 200 then 'Unhealthy'
        when {{ aqi }} <= 300 then 'Very Unhealthy'
        else 'Hazardous'
    end
{%- endmacro %}

{% macro aqi_band_order(aqi) -%}
    case
        when {{ aqi }} is null then null
        when {{ aqi }} <= 50 then 1
        when {{ aqi }} <= 100 then 2
        when {{ aqi }} <= 150 then 3
        when {{ aqi }} <= 200 then 4
        when {{ aqi }} <= 300 then 5
        else 6
    end
{%- endmacro %}
