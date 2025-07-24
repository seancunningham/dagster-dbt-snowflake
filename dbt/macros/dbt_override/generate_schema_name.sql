{% macro generate_schema_name(custom_schema_name=none, node=none) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {# {%- set schema_name = return(node.fqn[-2]) -%} #}
        {%- set schema_name = node.name.split("__", maxsplit=1)[0] -%}
        {%- set schema_name = schema_name.split("_", maxsplit=1)[-1] -%}

    {%- else -%}
        {%- set schema_name = custom_schema_name -%}
    {%- endif -%}
    {%- set schema_name = schema_name | trim -%}


    {% if target.name == "prod" %}
        {{ schema_name }}
    {%- else -%}
        {{ schema_name }}__{{ default_schema }}
    {%- endif -%}

{%- endmacro %}