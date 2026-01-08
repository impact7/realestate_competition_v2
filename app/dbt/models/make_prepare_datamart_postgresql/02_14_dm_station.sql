with temp1 as (
select
    s12_001g as eki_code,
    case when strpos(s12_001, '（') > 0 then left(s12_001, strpos(s12_001, '（') - 1) else s12_001 end as eki_name,
    s12_057 as eki_population,
    s12_003 as rosen_name,
    lat,
    lon
from
    raw_data.station_master
), temp2 as (
select
    t1.eki_code,
    t1.eki_name,
    sum(t2.eki_population) as eki_population,
    avg(t2.lon) as lon,
    avg(t2.lat) as lat
from
    temp1 as t1
    inner join
    temp1 as t2
    on
        t1.eki_name = t2.eki_name
        and t1.eki_code = t2.eki_code
where
    t1.lon <= t2.lon
    and t1.lat <= t2.lat
group by
    t1.eki_code,
    t1.eki_name
having
    sum(t2.eki_population) > 0
)
select
    *
from
    temp2
