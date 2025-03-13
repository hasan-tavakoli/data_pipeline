{% macro log_message(message, level='info') %}
    {%- if level == 'info' -%}
        {{ log(message, info=True) }}
    {%- elif level == 'warn' -%}
        {{ log("[WARNING] " ~ message, info=True) }}
    {%- elif level == 'error' -%}
        {{ log("[ERROR] " ~ message, info=True) }}
    {%- else -%}
        {{ log(message) }}
    {%- endif -%}
{% endmacro %}