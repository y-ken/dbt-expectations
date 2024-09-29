{% test expect_row_values_to_have_recent_data(model,
                                                column_name,
                                                datepart,
                                                interval,
                                                ignore_time_component=False,
                                                row_condition=None) %}

 {{ adapter.dispatch('test_expect_row_values_to_have_recent_data', 'dbt_expectations') (model,
                                                                                        column_name,
                                                                                        datepart,
                                                                                        interval,
                                                                                        ignore_time_component,
                                                                                        row_condition) }}

{% endtest %}

{% macro default__test_expect_row_values_to_have_recent_data(model, column_name, datepart, interval, ignore_time_component, row_condition) %}
{%- set default_start_date = '1970-01-01' -%}
with max_recency as (

    select 
        {% if ignore_time_component %}
            max(cast({{ column_name }} as date)) as max_timestamp
        {% else %}
            max(cast({{ column_name }} as {{ dbt_expectations.type_timestamp() }})) as max_timestamp
        {% endif %}
    from
        {{ model }}
    where
        -- to exclude erroneous future dates
        {% if ignore_time_component %}
            cast({{ column_name }} as date) <= cast({{ dbt_date.now() }} as date)
        {% else %}
            cast({{ column_name }} as {{ dbt_expectations.type_timestamp() }}) <= {{ dbt_date.now() }}
        {% endif %}
        {% if row_condition %}
        and {{ row_condition }}
        {% endif %}
)
select
    *
from
    max_recency
where
    -- if the row_condition excludes all rows, we need to compare against a default date
    -- to avoid false negatives
    {% if ignore_time_component %}
        coalesce(max_timestamp, cast('{{ default_start_date }}' as date))
            <
        cast({{ dbt.dateadd(datepart, interval * -1, dbt_date.now()) }} as date)
    {% else %}
        coalesce(max_timestamp, cast('{{ default_start_date }}' as {{ dbt_expectations.type_timestamp() }}))
            <
        cast({{ dbt.dateadd(datepart, interval * -1, dbt_date.now()) }} as {{ dbt_expectations.type_timestamp() }})
    {% endif %}

{% endmacro %}
