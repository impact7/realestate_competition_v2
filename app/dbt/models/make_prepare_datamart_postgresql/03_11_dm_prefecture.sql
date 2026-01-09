{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='03_11_' ~ train_test ~ '_dm_prefecture') }}

with temp1 as (
select
    floor(addr / 1000) as prefecture,
    target_price,
    lon,
    lat,
    row_number() over(partition by floor(addr / 1000) order by target_price desc) as row_number
from
    {{ ref('02_16_dm_price') }}
), temp2 as (
select
    *
from
    temp1
where
    row_number = 1
order by
    target_price desc
), temp3 as (
select
    t1.id,
    t2.prefecture,
    round(cast(
                  sqrt(power((t1.lon - t2.lon) * 111, 2) + power((t1.lat - t2.lat) * 90, 2))
           as numeric), 3) as distance
from
    {{ ref(table_name) }} as t1
    inner join
    temp2 as t2
    on
        1 = 1
), temp4 as (
select
    id,
    max(case when prefecture = 1  then distance else null end) as distance_01,
    max(case when prefecture = 2  then distance else null end) as distance_02,
    max(case when prefecture = 3  then distance else null end) as distance_03,
    max(case when prefecture = 4  then distance else null end) as distance_04,
    max(case when prefecture = 5  then distance else null end) as distance_05,
    max(case when prefecture = 6  then distance else null end) as distance_06,
    max(case when prefecture = 7  then distance else null end) as distance_07,
    max(case when prefecture = 8  then distance else null end) as distance_08,
    max(case when prefecture = 9  then distance else null end) as distance_09,
    max(case when prefecture = 10 then distance else null end) as distance_10,
    max(case when prefecture = 11 then distance else null end) as distance_11,
    max(case when prefecture = 12 then distance else null end) as distance_12,
    max(case when prefecture = 13 then distance else null end) as distance_13,
    max(case when prefecture = 14 then distance else null end) as distance_14,
    max(case when prefecture = 15 then distance else null end) as distance_15,
    max(case when prefecture = 16 then distance else null end) as distance_16,
    max(case when prefecture = 17 then distance else null end) as distance_17,
    max(case when prefecture = 18 then distance else null end) as distance_18,
    max(case when prefecture = 19 then distance else null end) as distance_19,
    max(case when prefecture = 20 then distance else null end) as distance_20,
    max(case when prefecture = 21 then distance else null end) as distance_21,
    max(case when prefecture = 22 then distance else null end) as distance_22,
    max(case when prefecture = 23 then distance else null end) as distance_23,
    max(case when prefecture = 24 then distance else null end) as distance_24,
    max(case when prefecture = 25 then distance else null end) as distance_25,
    max(case when prefecture = 26 then distance else null end) as distance_26,
    max(case when prefecture = 27 then distance else null end) as distance_27,
    max(case when prefecture = 28 then distance else null end) as distance_28,
    max(case when prefecture = 29 then distance else null end) as distance_29,
    max(case when prefecture = 30 then distance else null end) as distance_30,
    max(case when prefecture = 31 then distance else null end) as distance_31,
    max(case when prefecture = 32 then distance else null end) as distance_32,
    max(case when prefecture = 33 then distance else null end) as distance_33,
    max(case when prefecture = 34 then distance else null end) as distance_34,
    max(case when prefecture = 35 then distance else null end) as distance_35,
    max(case when prefecture = 36 then distance else null end) as distance_36,
    max(case when prefecture = 37 then distance else null end) as distance_37,
    max(case when prefecture = 38 then distance else null end) as distance_38,
    max(case when prefecture = 39 then distance else null end) as distance_39,
    max(case when prefecture = 40 then distance else null end) as distance_40,
    max(case when prefecture = 41 then distance else null end) as distance_41,
    max(case when prefecture = 42 then distance else null end) as distance_42,
    max(case when prefecture = 43 then distance else null end) as distance_43,
    max(case when prefecture = 44 then distance else null end) as distance_44,
    max(case when prefecture = 45 then distance else null end) as distance_45,
    max(case when prefecture = 46 then distance else null end) as distance_46,
    max(case when prefecture = 47 then distance else null end) as distance_47
from
    temp3
group by
    id
)
select
    *
from
    temp4
