{% macro cents_to_euro(cents_column) %}
    ({{ cents_column }} / 100.0)
{% endmacro %}
