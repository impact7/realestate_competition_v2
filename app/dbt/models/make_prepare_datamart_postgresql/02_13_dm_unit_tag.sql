{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='02_13_' ~ train_test ~ '_dm_unit_tag') }}

select
    id,
    cast(unnest(string_to_array(unit_tag_id, '/')) as bigint) as unit_tag_id
from
    {{ ref(table_name) }}
