{% macro to_usd(amount_col, currency_col) %}
    {#- Xây dựng SQL để lấy toàn bộ bảng currency_rates -#}
    {%- set sql -%}
        select currency, rate_to_usd
        from {{ ref('currency_rates') }}
    {%- endset -%}

    {#- Chạy query và lấy kết quả về dưới dạng `rows` -#}
    {%- set result = run_query(sql) -%}
        {%- if execute -%}
            {%- set rows = result.rows -%}
        {%- else -%}
            {%- set rows = [] -%}
        {%- endif -%}

{#- Sinh CASE WHEN dựa trên kết quả -#}
    case
    {% for row in rows %}
        when {{ currency_col }} = '{{ row[0] }}'
            then {{ amount_col }} * {{ row[1] }}
    {% endfor %}
    else null
end
{% endmacro %}