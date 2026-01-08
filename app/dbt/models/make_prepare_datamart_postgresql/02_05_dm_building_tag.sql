{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='02_05_' ~ train_test ~ '_dm_building_tag') }}

select
    id,
    cast(unnest(string_to_array(building_tag_id, '/')) as bigint) as building_tag_id
from
    {{ ref(table_name) }}
