{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {% if target.name == "prod" %}
        {%- if custom_schema_name is none -%}

            {% set parent_dir = node.fqn[-2] %}
            {{ parent_dir | trim }}

        {%- else -%}

            {{ custom_schema_name | trim }}

        {%- endif -%}

    {%- else -%}
        {%- if custom_schema_name is none -%}

            {% set parent_dir = node.fqn[-2] %}
            {{ parent_dir | trim }}__{{ default_schema }}

        {%- else -%}

            {{ custom_schema_name | trim }}__{{ default_schema }}

        {%- endif -%}
    {%- endif -%}

{%- endmacro %}