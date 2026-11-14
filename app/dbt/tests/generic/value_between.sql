-- Fails on rows where the column is outside [min_value, max_value] (nulls are left to not_null).
{% test value_between(model, column_name, min_value, max_value) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}

{% endtest %}
