{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='02_08_' ~ train_test ~ '_dm_reform_wet_area') }}

select
    id,
    case when reform_wet_area is not null and strpos(reform_wet_area, '1') > 0 then 1 else 0 end as reform_wet_area_1,
    case when reform_wet_area is not null and strpos(reform_wet_area, '2') > 0 then 1 else 0 end as reform_wet_area_2,
    case when reform_wet_area is not null and strpos(reform_wet_area, '3') > 0 then 1 else 0 end as reform_wet_area_3,
    case when reform_wet_area is not null and strpos(reform_wet_area, '4') > 0 then 1 else 0 end as reform_wet_area_4,
    case when reform_wet_area is not null and strpos(reform_wet_area, '5') > 0 then 1 else 0 end as reform_wet_area_5,
    case when reform_wet_area is not null and strpos(reform_wet_area, '6') > 0 then 1 else 0 end as reform_wet_area_6
from
    {{ ref(table_name) }}
