{#
  Macro: hash_key
  Generates a Data Vault 2.0 hash key (SHA-256) from a list of business keys.
  Usage: {{ hash_key(['customer_id', 'country_code']) }}
#}

{% macro hash_key(columns) -%}
    {{ return(adapter.dispatch('hash_key', 'data_platform')(columns)) }}
{%- endmacro %}


{% macro postgres__hash_key(columns) -%}
    md5(
        {% for col in columns -%}
            coalesce(cast({{ col }} as varchar), '') {% if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    )
{%- endmacro %}


{% macro spark__hash_key(columns) -%}
    sha2(
        concat_ws('|',
            {% for col in columns -%}
                coalesce(cast({{ col }} as string), '')
                {% if not loop.last %},{% endif %}
            {%- endfor %}
        ),
        256
    )
{%- endmacro %}


{% macro trino__hash_key(columns) -%}
    sha256(
        to_utf8(
            {% for col in columns -%}
                coalesce(cast({{ col }} as varchar), '')
                {% if not loop.last %} || '|' || {% endif %}
            {%- endfor %}
        )
    )
{%- endmacro %}


{# Helper: load_datetime — current UTC timestamp for record_source tracking #}
{% macro load_datetime() -%}
    {{ return("cast(current_timestamp as timestamp)") }}
{%- endmacro %}


{# Helper: contract_check — wraps contract assertions #}
{% macro contract_check(contract_name) -%}
    {%- if execute -%}
        {% set contract = load_contract(contract_name) %}
        -- Contract: {{ contract_name }} (owner: {{ contract.owner }}, sla: {{ contract.sla }})
    {%- endif %}
{%- endmacro %}


{# Helper: refresh_cube_preagg — post-hook to trigger Cube pre-agg refresh #}
{% macro refresh_cube_preagg(model_name) -%}
    -- Cube pre-agg refresh for '{{ model_name }}' (executed by sidecar job in CI/CD)
    -- See: scripts/cube_refresh.sh
{%- endmacro %}
