{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='03_03_' ~ train_test ~ '_dm_station_temp') }}

with temp11 as (
select
    t1.id,
    t2.eki_population,
    t2.lon,
    t2.lat,
    cast(sqrt(
            power((t2.lat - t1.lat) * 111320, 2) +
            power((t2.lon - t1.lon) * 111320, 2)
    ) as bigint ) as distance
from
    {{ ref(table_name) }} as t1
    left join
    {{ ref('02_14_dm_station') }} as t2
    on
        t1.eki_name1 = t2.eki_name
), temp12 as (
select
    *,
    row_number() over(partition by id order by distance) as distance_order
from
    temp11
), temp13 as (
select
    id,
    eki_population as eki_population1,
    lon as lon1,
    lat as lat1,
    distance as distance1
from
    temp12
where
    distance_order = 1
), temp21 as (
select
    t1.id,
    t2.eki_population,
    t2.lon,
    t2.lat,
    cast(sqrt(
            power((t2.lat - t1.lat) * 111320, 2) +
            power((t2.lon - t1.lon) * 111320, 2)
    ) as bigint ) as distance
from
    {{ ref(table_name) }} as t1
    left join
    {{ ref('02_14_dm_station') }} as t2
    on
        t1.eki_name2 = t2.eki_name
), temp22 as (
select
    *,
    row_number() over(partition by id order by distance) as distance_order
from
    temp21
), temp23 as (
select
    id,
    eki_population as eki_population2,
    lon as lon2,
    lat as lat2,
    distance as distance2
from
    temp22
where
    distance_order = 1
), temp31 as (
select
    t1.id,
    t2.eki_population1,
    t2.lon1,
    t2.lat1,
    t2.distance1,
    t3.eki_population2,
    t3.lon2,
    t3.lat2,
    t3.distance2
from
    {{ ref(table_name) }} as t1
    left join
    temp13 as t2
    on
        t1.id = t2.id
    left join
    temp23 as t3
    on
        t1.id = t3.id
)
select
    *
from
    temp31
