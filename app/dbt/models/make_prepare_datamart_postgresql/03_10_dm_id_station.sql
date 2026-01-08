{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='03_10_' ~ train_test ~ '_dm_id_station') }}

with temp_500m as (
select
    t1.id,
    count(1) as cnt_station_500m,
    sum(t2.eki_population) as sum_population_500m,
    max(t2.eki_population) as max_poulation_500m
from
    {{ ref('03_05_dm_mesh_80km') }} as t1
    inner join
    {{ ref('03_09_dm_mesh_station') }} as t2
    on
        t1.mesh_code_80km = t2.mesh_code_80km
        and t1.lon - 0.5/111 <= t2.lon
        and t1.lat - 0.5/90 <= t2.lat
        and t1.lon + 0.5/111 > t2.lon
        and t1.lat + 0.5/90 > t2.lat
group by
    t1.id
), temp_1km as (
select
    t1.id,
    count(1) as cnt_station_1km,
    sum(t2.eki_population) as sum_population_1km,
    max(t2.eki_population) as max_poulation_1km
from
    {{ ref('03_05_dm_mesh_80km') }} as t1
    inner join
    {{ ref('03_09_dm_mesh_station') }} as t2
    on
        t1.mesh_code_80km = t2.mesh_code_80km
        and t1.lon - 1.0/111 <= t2.lon
        and t1.lat - 1.0/90 <= t2.lat
        and t1.lon + 1.0/111 > t2.lon
        and t1.lat + 1.0/90 > t2.lat
group by
    t1.id
), temp_3km as (
select
    t1.id,
    count(1) as cnt_station_3km,
    sum(t2.eki_population) as sum_population_3km,
    max(t2.eki_population) as max_poulation_3km
from
    {{ ref('03_05_dm_mesh_80km') }} as t1
    inner join
    {{ ref('03_09_dm_mesh_station') }} as t2
    on
        t1.mesh_code_80km = t2.mesh_code_80km
        and t1.lon - 3.0/111 <= t2.lon
        and t1.lat - 3.0/90 <= t2.lat
        and t1.lon + 3.0/111 > t2.lon
        and t1.lat + 3.0/90 > t2.lat
group by
    t1.id
), temp_10km as (
select
    t1.id,
    count(1) as cnt_station_10km,
    sum(t2.eki_population) as sum_population_10km,
    max(t2.eki_population) as max_poulation_10km
from
    {{ ref('03_05_dm_mesh_80km') }} as t1
    inner join
    {{ ref('03_09_dm_mesh_station') }} as t2
    on
        t1.mesh_code_80km = t2.mesh_code_80km
        and t1.lon - 10.0/111 <= t2.lon
        and t1.lat - 10.0/90 <= t2.lat
        and t1.lon + 10.0/111 > t2.lon
        and t1.lat + 10.0/90 > t2.lat
group by
    t1.id
), temp_order as (
select
    t1.id,
    t2.eki_population,
    round(cast(sqrt(power((t2.lon - t1.lon)*111, 2) + power((t2.lat - t1.lat)*90, 2)) as numeric), 4) as distance,
    row_number() over(partition by t1.id order by sqrt(power((t2.lon - t1.lon)*111, 2) + power((t2.lat - t1.lat)*90, 2))) as row_number
from
    {{ ref('03_05_dm_mesh_80km') }} as t1
    inner join
    {{ ref('03_09_dm_mesh_station') }} as t2
    on
        t1.mesh_code_80km = t2.mesh_code_80km
), temp_min as (
select
    id,
    eki_population as min_distance_eki_population,
    distance as min_distance_eki
from
    temp_order
where
    row_number = 1
)
select
    t1.id,
    t2.cnt_station_500m,
    t2.sum_population_500m,
    t2.max_poulation_500m,
    t3.cnt_station_1km,
    t3.sum_population_1km,
    t3.max_poulation_1km,
    t4.cnt_station_3km,
    t4.sum_population_3km,
    t4.max_poulation_3km,
    t5.cnt_station_10km,
    t5.sum_population_10km,
    t5.max_poulation_10km,
    t6.min_distance_eki_population,
    t6.min_distance_eki
from
    {{ ref(table_name) }} as t1
    left join
    temp_500m as t2
    on
        t1.id = t2.id
    left join
    temp_1km as t3
    on
        t1.id = t3.id
    left join
    temp_3km as t4
    on
        t1.id = t4.id
    left join
    temp_10km as t5
    on
        t1.id = t5.id
    left join
    temp_min as t6
    on
        t1.id = t6.id


