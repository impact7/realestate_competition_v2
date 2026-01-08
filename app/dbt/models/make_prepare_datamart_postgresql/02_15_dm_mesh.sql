with temp1 as (
select
    cast(mesh_code as bigint) as mesh_code,
    sum(pt2020) as pt2020,
    sum(pt2025) as pt2025,
    sum(pt2030) as pt2030,
    sum(pt2050) as pt2050,
    sum(pt2070) as pt2070,
    min(left_lon) as left_lon,
    min(bottom_lat) as bottom_lat,
    max(right_lon) as right_lon,
    max(top_lat) as top_lat
from
    raw_data.mesh_data
group by
    mesh_code
)
select
    mesh_code,
    left_lon,
    bottom_lat,
    right_lon,
    top_lat,
    pt2020,
    pt2025,
    pt2030,
    pt2050,
    pt2070,
    pt2025 / pt2020 as fluctuate_2025_2020,
    pt2030 / pt2020 as fluctuate_2030_2020,
    pt2050 / pt2020 as fluctuate_2050_2020,
    pt2070 / pt2020 as fluctuate_2070_2020
from
    temp1
