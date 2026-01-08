{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='03_05_' ~ train_test ~ '_dm_mesh_temp2') }}

with temp1 as (
select
    t1.id,
    t2.mesh_code,
    t2.pt2020,
    t2.pt2025,
    t2.pt2030,
    t2.pt2050,
    t2.pt2070,
    t2.fluctuate_2025_2020,
    t2.fluctuate_2050_2020,
    t2.fluctuate_2070_2020
from
    {{ ref('03_05_dm_mesh_1km') }} as t1
    left join
    {{ ref('02_15_dm_mesh') }} as t2
    on
        t1.mesh_code_1km = cast(t2.mesh_code / 100 as bigint)
        and t1.lon >= t2.left_lon
        and t1.lat >= t2.bottom_lat
        and t1.lon < t2.right_lon
        and t1.lat < t2.top_lat
)
select
    *
from
    temp1
