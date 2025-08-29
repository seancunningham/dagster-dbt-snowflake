{%- macro apply_data_mask(columns) -%}

    {%- set database = this.database -%}
    {%- set schema = this.schema -%}
    {%- set table = this.table -%}

    {%- set relation = load_relation(this) -%}
    {%- set table_type = "view" if relation.is_view else "table" -%}

    {%- set table_columns = adapter.get_columns_in_relation(relation) -%}

    {% for column_name in columns -%}
        {%- for column in table_columns -%}
            {%- if column.name | upper == column_name | upper -%}

                create masking policy if not exists {{ database }}.{{ schema }}.{{ table }}__{{ column_name }}__mask
                as (val {{ column.dtype }}) returns {{ column.dtype }} ->
                    case
                        when lower(current_role()) in ('service_account')
                        then val

                        when array_contains(
                            upper('{{ database }}__{{ schema }}__{{ table }}__{{ column_name }}__unmasked'),
                            parse_json(current_available_roles())::array(varchar)
                        )
                        then val

                        when array_contains(
                            upper('{{ database }}__{{ schema }}__{{ table }}__unmasked'),
                            parse_json(current_available_roles())::array(varchar)
                        )
                        then val

                        when array_contains(
                            upper('{{ database }}__{{ schema }}__unmasked'),
                            parse_json(current_available_roles())::array(varchar)
                        )
                        then val

                        when array_contains(
                            '{{ database }}__unmasked',
                            parse_json(current_available_roles())::array(varchar)
                        )
                        then val
                        
                        else
                        {% if column.is_string() -%}
                             md5(val)
                        {%- elif column.is_number() -%}
                            0
                        {%- endif %}
                    end
                ;

                alter {{ table_type }} if exists {{ database }}.{{ schema }}.{{ table }}
                modify column {{ column_name }} set masking policy {{ database }}.{{ schema }}.{{ table }}__{{ column_name }}__mask
                ;
            {%- endif -%}
        {%- endfor -%}
    {% endfor %}
{%- endmacro -%}
