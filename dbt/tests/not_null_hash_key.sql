{#
  Custom test: ensure every Data Vault 2.0 hash key is non-null.
  Generic test — runs on any model that exposes a column ending in `_hk`.
#}

{% test not_null_hash_key(model) %}

select *
from {{ model }}
where {% for col in adapter.get_columns_in_relation(model) -%}
    {% if col.name | endswith('_hk') -%}
        {{ col.name }} is null
        {% if not loop.last %} or {% endif %}
    {% endif %}
{%- endfor %}

{% endtest %}
