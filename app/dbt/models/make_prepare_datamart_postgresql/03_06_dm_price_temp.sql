{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='03_06_' ~ train_test ~ '_dm_price_temp') }}

with temp01 as (
select
    t1.id,
    t1.addr,
    percentile_cont(0.5) within group (order by target_price) as med_target_price,
    avg(target_price) as avg_target_price,
    max(target_price) as max_target_price,
    percentile_cont(0.5) within group (order by price) as med_price,
    avg(price) as avg_price,
    max(price) as max_price,
    count(1) as cnt_price
from
    {{ ref(table_name) }} as t1
    inner join
    {{ ref('02_16_dm_price') }} as t2
    on
        t1.target_ym = t2.target_ym
        and t1.addr = cast(t2.addr as int)
        and t1.lon - 0.5/111 <= t2.lon
        and t1.lat - 0.5/90 <= t2.lat
        and t1.lon + 0.5/111 >= t2.lon
        and t1.lat + 0.5/90 >= t2.lat
group by
    t1.addr,
    t1.id
), temp02 as (
select
    t1.id,
    t1.addr,
    percentile_cont(0.5) within group (order by target_price) as med_target_price,
    avg(target_price) as avg_target_price,
    max(target_price) as max_target_price,
    percentile_cont(0.5) within group (order by price) as med_price,
    avg(price) as avg_price,
    max(price) as max_price,
    count(1) as cnt_price
from
    {{ ref(table_name) }} as t1
    inner join
    {{ ref('02_16_dm_price') }} as t2
    on
        t1.target_ym = t2.target_ym
        and t1.addr = cast(t2.addr as int)
        and t1.lon - 1.0/111 <= t2.lon
        and t1.lat - 1.0/90 <= t2.lat
        and t1.lon + 1.0/111 >= t2.lon
        and t1.lat + 1.0/90 >= t2.lat
group by
    t1.addr,
    t1.id
), temp03 as (
select
    t1.id,
    t1.addr,
    percentile_cont(0.5) within group (order by target_price) as med_target_price,
    avg(target_price) as avg_target_price,
    max(target_price) as max_target_price,
    percentile_cont(0.5) within group (order by price) as med_price,
    avg(price) as avg_price,
    max(price) as max_price,
    count(1) as cnt_price
from
    {{ ref(table_name) }} as t1
    inner join
    {{ ref('02_16_dm_price') }} as t2
    on
        t1.target_ym = t2.target_ym
        and t1.addr = cast(t2.addr as int)
        and t1.lon - 3.0/111 <= t2.lon
        and t1.lat - 3.0/90 <= t2.lat
        and t1.lon + 3.0/111 >= t2.lon
        and t1.lat + 3.0/90 >= t2.lat
group by
    t1.addr,
    t1.id
), temp04 as (
select
    t1.id,
    t1.addr,
    percentile_cont(0.5) within group (order by target_price) as med_target_price,
    avg(target_price) as avg_target_price,
    max(target_price) as max_target_price,
    percentile_cont(0.5) within group (order by price) as med_price,
    avg(price) as avg_price,
    max(price) as max_price,
    count(1) as cnt_price
from
    {{ ref(table_name) }} as t1
    inner join
    {{ ref('02_16_dm_price') }} as t2
    on
        t1.target_ym = t2.target_ym
        and t1.addr = cast(t2.addr as int)
group by
    t1.addr,
    t1.id
), temp99 as (
select
    t1.id,

    t2.med_target_price as med_target_price_500m,
    cast(t2.avg_target_price as bigint) as avg_target_price_500m,
    t2.max_target_price as max_target_price_500m,
    t2.med_price as med_price_500m,
    cast(t2.avg_price as bigint) as avg_price_500m,
    t2.max_price as max_price_500m,
    t2.cnt_price as cnt_price_500m,

    t3.med_target_price as med_target_price_1km,
    cast(t3.avg_target_price as bigint) as avg_target_price_1km,
    t3.max_target_price as max_target_price_1km,
    t3.med_price as med_price_1km,
    cast(t3.avg_price as bigint) as avg_price_1km,
    t3.max_price as max_price_1km,
    t3.cnt_price as cnt_price_1km,

    t4.med_target_price as med_target_price_3km,
    cast(t4.avg_target_price as bigint) as avg_target_price_3km,
    t4.max_target_price as max_target_price_3km,
    t4.med_price as med_price_3km,
    cast(t4.avg_price as bigint) as avg_price_3km,
    t4.max_price as max_price_3km,
    t4.cnt_price as cnt_price_3km,

    t5.med_target_price as med_target_price_city,
    cast(t5.avg_target_price as bigint) as avg_target_price_city,
    t5.max_target_price as max_target_price_city,
    t5.med_price as med_price_city,
    cast(t5.avg_price as bigint) as avg_price_city,
    t5.max_price as max_price_city,
    t5.cnt_price as cnt_price_city
from
    {{ ref(table_name) }} as t1
    left join
    temp01 as t2
    on
        t1.id = t2.id
    left join
    temp02 as t3
    on
        t1.id = t3.id
    left join
    temp03 as t4
    on
        t1.id = t4.id
    left join
    temp04 as t5
    on
        t1.id = t5.id
)
select
    *
from
    temp99
