{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='03_08_' ~ train_test ~ '_dm_mesh_1km') }}

with temp1 as (
select
    t1.id,
    t2.mesh_code_1km,
    t2.pt2020_1km,
    t2.pt2025_1km,
    t2.pt2030_1km,
    t2.pt2050_1km,
    t2.pt2070_1km,
    t2.fluctuate_2025_2020_1km,
    t2.fluctuate_2050_2020_1km,
    t2.fluctuate_2070_2020_1km
from
    {{ ref('03_05_dm_mesh_1km') }} as t1
    left join
    {{ ref('02_17_dm_mesh_1km') }} as t2
    on
        t1.mesh_code_1km = t2.mesh_code_1km
)
select
    *
from
    temp1
