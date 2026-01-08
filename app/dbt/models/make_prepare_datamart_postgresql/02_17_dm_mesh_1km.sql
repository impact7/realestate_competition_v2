with temp1 as (
select
    cast(cast(mesh_code as bigint) /100 as bigint) as mesh_code_1km,
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
    cast(cast(mesh_code as bigint) /100 as bigint)
)
select
    mesh_code_1km,
    pt2020 as pt2020_1km,
    pt2025 as pt2025_1km,
    pt2030 as pt2030_1km,
    pt2030 as pt2040_1km,
    pt2050 as pt2050_1km,
    pt2070 as pt2070_1km,
    pt2025 / pt2020 as fluctuate_2025_2020_1km,
    pt2030 / pt2020 as fluctuate_2030_2020_1km,
    pt2050 / pt2020 as fluctuate_2050_2020_1km,
    pt2070 / pt2020 as fluctuate_2070_2020_1km
from
    temp1
