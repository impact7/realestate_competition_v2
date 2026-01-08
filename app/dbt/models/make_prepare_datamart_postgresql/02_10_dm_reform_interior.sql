{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='02_10_' ~ train_test ~ '_dm_reform_interior') }}

select
    id,
    case when reform_interior is not null and strpos(reform_interior, '1') > 0 then 1 else 0 end as reform_interior_1,
    case when reform_interior is not null and strpos(reform_interior, '2') > 0 then 1 else 0 end as reform_interior_2,
    case when reform_interior is not null and strpos(reform_interior, '3') > 0 then 1 else 0 end as reform_interior_3,
    case when reform_interior is not null and strpos(reform_interior, '4') > 0 then 1 else 0 end as reform_interior_4,
    case when reform_interior is not null and strpos(reform_interior, '5') > 0 then 1 else 0 end as reform_interior_5,
    case when reform_interior is not null and strpos(reform_interior, '6') > 0 then 1 else 0 end as reform_interior_6
from
    {{ ref(table_name) }}
