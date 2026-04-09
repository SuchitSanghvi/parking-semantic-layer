{% macro initcap(column) %}
    array_to_string(
        list_transform(
            string_split(lower({{ column }}), ' '),
            x -> upper(left(x, 1)) || right(x, length(x) - 1)
        ),
        ' '
    )
{% endmacro %}
