{% macro cents_to_euro(column) %}
({{ column }} / 100.0)
{% endmacro %}
