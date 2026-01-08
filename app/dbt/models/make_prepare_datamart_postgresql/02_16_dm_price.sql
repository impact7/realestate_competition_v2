with temp01 as (
select
    addr,
    kubun,
    number,
    address,
    201901 as target_ym,
    (price2023+price2024)/2 as target_price,
    price2019 as price,
    round(1.0*price2023/price2019-1, 3) as ratio_202301,
    round(1.0*(price2023+price2024)/2/price2019-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_country_data
where
    price2018 > 0
), temp02 as (
select
    addr,
    kubun,
    number,
    address,
    201907 as target_ym,
    (price2023+price2024)/2 as target_price,
    (price2019+price2020)/2 as price,
    round(1.0*price2023/(price2019+price2020)*2-1, 3) as ratio_202301,
    round(1.0*(price2023+price2024)/(price2019+price2020)-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_country_data
where
    price2018 > 0
), temp03 as (
select
    addr,
    kubun,
    number,
    address,
    202001 as target_ym,
    (price2023+price2024)/2 as target_price,
    price2020 as price,
    round(1.0*price2023/price2020-1, 3) as ratio_202301,
    round(1.0*(price2023+price2024)/2/price2020-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_country_data
where
    price2018 > 0
), temp04 as (
select
    addr,
    kubun,
    number,
    address,
    202007 as target_ym,
    (price2023+price2024)/2 as target_price,
    (price2020+price2021)/2 as price,
    round(1.0*price2023/(price2020+price2021)*2-1, 3) as ratio_202301,
    round(1.0*(price2023+price2024)/(price2020+price2021)-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_country_data
where
    price2018 > 0
), temp05 as (
select
    addr,
    kubun,
    number,
    address,
    202001 as target_ym,
    (price2023+price2024)/2 as target_price,
    price2021 as price,
    round(1.0*price2023/price2021-1, 3) as ratio_202301,
    round(1.0*(price2023+price2024)/2/price2021-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_country_data
where
    price2018 > 0
), temp06 as (
select
    addr,
    kubun,
    number,
    address,
    202107 as target_ym,
    (price2023+price2024)/2 as target_price,
    (price2021+price2022)/2 as price,
    round(1.0*price2023/(price2021+price2022)*2-1, 3) as ratio_202301,
    round(1.0*(price2023+price2024)/(price2021+price2022)-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_country_data
where
    price2018 > 0
), temp07 as (
select
    addr,
    kubun,
    number,
    address,
    202201 as target_ym,
    (price2023+price2024)/2 as target_price,
    price2022 as price,
    round(1.0*price2023/price2022-1, 3) as ratio_202301,
    round(1.0*(price2023+price2024)/2/price2022-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_country_data
where
    price2018 > 0
), temp08 as (
select
    addr,
    kubun,
    number,
    address,
    202207 as target_ym,
    (price2023+price2024)/2 as target_price,
    (price2022+price2023)/2 as price,
    round(1.0*price2023/(price2022+price2023)*2-1, 3) as ratio_202301,
    round(1.0*(price2023+price2024)/(price2022+price2023)-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_country_data
where
    price2018 > 0
), temp09 as (
select
    addr,
    kubun,
    number,
    address,
    202301 as target_ym,
    (price2023+price2024)/2 as target_price,
    (price2022+price2023)/2 as price,
    round(1.0*price2023/(price2022+price2023)*2-1, 3) as ratio_202301,
    round(1.0*(price2023+price2024)/(price2022+price2023)-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_country_data
where
    price2018 > 0
), temp10 as (
select
    addr,
    kubun,
    number,
    address,
    202307 as target_ym,
    (price2023+price2024)/2 as target_price,
    (price2022+price2023)/2 as price,
    round(1.0*price2023/(price2022+price2023)*2-1, 3) as ratio_202301,
    round(1.0*(price2023+price2024)/(price2022+price2023)-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_country_data
where
    price2018 > 0
), temp11 as (
select
    addr,
    kubun,
    number,
    address,
    201901 as target_ym,
    price2023 as target_price,
    (price2018+price2019)/2 as price,
    round(1.0*(price2022+price2023)/(price2018+price2019)-1, 3) as ratio_202301,
    round(1.0*price2023/(price2018+price2019)*2-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_prefecture_data
where
    price2018 > 0
), temp12 as (
select
    addr,
    kubun,
    number,
    address,
    201907 as target_ym,
    price2023 as target_price,
    price2019 as price,
    round(1.0*(price2022+price2023)/2/price2019-1, 3) as ratio_202301,
    round(1.0*price2023/price2019-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_prefecture_data
where
    price2018 > 0
), temp13 as (
select
    addr,
    kubun,
    number,
    address,
    202001 as target_ym,
    price2023 as target_price,
    (price2019+price2020)/2 as price,
    round(1.0*(price2022+price2023)/(price2019+price2020)-1, 3) as ratio_202301,
    round(1.0*price2023/(price2019+price2020)*2-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_prefecture_data
where
    price2018 > 0
), temp14 as (
select
    addr,
    kubun,
    number,
    address,
    202007 as target_ym,
    price2023 as target_price,
    price2020 as price,
    round(1.0*(price2022+price2023)/2/price2020-1, 3) as ratio_202301,
    round(1.0*price2023/price2020-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_prefecture_data
where
    price2018 > 0
), temp15 as (
select
    addr,
    kubun,
    number,
    address,
    202101 as target_ym,
    price2023 as target_price,
    (price2020+price2021)/2 as price,
    round(1.0*(price2022+price2023)/(price2020+price2021)-1, 3) as ratio_202301,
    round(1.0*price2023/(price2020+price2021)*2-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_prefecture_data
where
    price2018 > 0
), temp16 as (
select
    addr,
    kubun,
    number,
    address,
    202107 as target_ym,
    price2023 as target_price,
    price2021 as price,
    round(1.0*(price2022+price2023)/2/price2021-1, 3) as ratio_202301,
    round(1.0*price2023/price2021-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_prefecture_data
where
    price2018 > 0
), temp17 as (
select
    addr,
    kubun,
    number,
    address,
    202201 as target_ym,
    price2023 as target_price,
    (price2021+price2022)/2 as price,
    round(1.0*(price2022+price2023)/(price2021+price2022)-1, 3) as ratio_202301,
    round(1.0*price2023/(price2021+price2022)*2-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_prefecture_data
where
    price2018 > 0
), temp18 as (
select
    addr,
    kubun,
    number,
    address,
    202207 as target_ym,
    price2023 as target_price,
    price2022 as price,
    round(1.0*(price2022+price2023)/2/price2022-1, 3) as ratio_202301,
    round(1.0*price2023/price2022-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_prefecture_data
where
    price2018 > 0
), temp19 as (
select
    addr,
    kubun,
    number,
    address,
    202301 as target_ym,
    price2023 as target_price,
    price2022 as price,
    round(1.0*(price2022+price2023)/2/price2022-1, 3) as ratio_202301,
    round(1.0*price2023/price2022-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_prefecture_data
where
    price2018 > 0
), temp20 as (
select
    addr,
    kubun,
    number,
    address,
    202307 as target_ym,
    price2023 as target_price,
    price2022 as price,
    round(1.0*(price2022+price2023)/2/price2022-1, 3) as ratio_202301,
    round(1.0*price2023/price2022-1, 3) as ratio_202307,
    lat,
    lon
from
    raw_data.price_prefecture_data
where
    price2018 > 0
), temp98 as (
select
    *
from
    temp01
union all
select
    *
from
    temp02
union all
select
    *
from
    temp03
union all
select
    *
from
    temp04
union all
select
    *
from
    temp05
union all
select
    *
from
    temp06
union all
select
    *
from
    temp07
union all
select
    *
from
    temp08
union all
select
    *
from
    temp09
union all
select
    *
from
    temp10
union all
select
    *
from
    temp11
union all
select
    *
from
    temp12
union all
select
    *
from
    temp13
union all
select
    *
from
    temp14
union all
select
    *
from
    temp15
union all
select
    *
from
    temp16
union all
select
    *
from
    temp17
union all
select
    *
from
    temp18
union all
select
    *
from
    temp19
union all
select
    *
from
    temp20
)
select
    cast(addr as bigint) as addr,
    kubun,
    number,
    address,
    target_ym,
    target_price,
    price,
    ratio_202301,
    ratio_202307,
    lat,
    lon
from
    temp98
where
    kubun in ('000', '005')
