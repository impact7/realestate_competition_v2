with temp1 as (
select
    cast(mesh_code / 1000000 as bigint) as mesh_code_80km,
    min(left_lon) as left_lon,
    min(bottom_lat) as bottom_lat,
    max(right_lon) as right_lon,
    max(top_lat) as top_lat
from
    {{ ref('02_15_dm_mesh') }} as t1
group by
    cast(mesh_code / 1000000 as bigint)
), temp2 as (
select
    t2.mesh_code_80km,
    t1.lon,
    t1.lat,
    t1.eki_name,
    t1.eki_population
from
    {{ ref('02_14_dm_station') }} as t1
    inner join
    temp1 as t2
    on
        t1.lon >= t2.left_lon
        and t1.lat >= t2.bottom_lat
        and t1.lon < t2.right_lon
        and t1.lat < t2.top_lat
)
select
    *
from
    temp2
