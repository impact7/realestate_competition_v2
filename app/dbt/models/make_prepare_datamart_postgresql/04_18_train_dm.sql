{{ config(
    enabled = (var('train_test') == 'train')
) }}

select
    t1.id,
    t1.target_ym,
    t1.building_id,
    t1.building_status,
    t1.building_create_date,
    t1.building_modify_date,
    t1.building_type,
    t1.unit_count,
    t1.lon,
    t1.lat,
    t1.building_structure,
    t1.total_floor_area,
    t1.building_area,
    t1.floor_count,
    t1.basement_floor_count,
    t1.month_built,
    t1.building_land_area,
    t1.land_area_all,
    t1.unit_area_min,
    t1.unit_area_max,
    t1.building_land_chimoku,
    t1.land_youto,
    t1.land_toshi,
    t1.land_chisei,
    t1.land_area_kind,
    t1.land_setback_flg,
    t1.land_setback,
    t1.land_kenpei,
    t1.land_youseki,
    t1.land_road_cond,
    t1.building_area_kind,
    t1.management_association_flg,
    t1.reform_exterior,
    t1.reform_exterior_month,
    t1.unit_id,
    t1.room_floor,
    t1.rate_room_floor,
    t1.balcony_area,
    t1.dwelling_unit_window_angle,
    t1.room_count,
    t1.floor_plan_code,
    t1.reform_wet_area_month,
    t1.reform_interior_month,
    t1.bukken_id,
    t1.snapshot_create_month,
    t1.new_month,
    t1.snapshot_modify_month,
    t1.timelimit_month,
    t1.bukken_type,
    t1.flg_investment,
    t1.empty_number,
    t1.post1,
    t1.post2,
    t1.post,
    t1.addr1_1,
    t1.addr1_2,
    t1.addr,
    t1.nl,
    t1.el,
    t1.bus_time1,
    t1.walk_distance1,
    t1.walk_distance2,
    t1.snapshot_land_area,
    t1.snapshot_land_shidou,
    t1.land_shidou_a,
    t1.land_shidou_b,
    t1.land_mochibun_a,
    t1.land_mochibun_b,
    t1.house_area,
    t1.flg_new,
    t1.house_kanrinin,
    t1.room_kaisuu,
    t1.snapshot_window_angle,
    t1.madori_number_all,
    t1.madori_kind_all,
    t1.money_kyoueki,
    t1.money_kyoueki_tax,
    t1.money_rimawari_now,
    t1.money_shuuzen,
    t1.parking_money,
    t1.parking_money_tax,
    t1.parking_kubun,
    t1.parking_distance,
    t1.parking_number,
    t1.genkyo_code,
    t1.usable_status,
    t1.usable_month,
    t1.renovation_month,

    t2.brand_name,

    t8.reform_wet_area_1,
    t8.reform_wet_area_2,
    t8.reform_wet_area_3,
    t8.reform_wet_area_4,
    t8.reform_wet_area_6,
    t10.reform_interior_1,
    t10.reform_interior_2,
    t10.reform_interior_5,
    t11.reform_interior_other_category,
    t13.building_tag_210101,
    t13.building_tag_210102,
    t13.building_tag_210201,
    t13.building_tag_210202,
    t13.building_tag_210299,
    t13.building_tag_210301,
    t13.building_tag_210302,
    t13.building_tag_210303,
    t13.building_tag_210399,
    t13.building_tag_210401,
    t13.building_tag_294201,
    t13.building_tag_310101,
    t13.building_tag_310201,
    t13.building_tag_320101,
    t13.building_tag_320201,
    t13.building_tag_320901,
    t13.building_tag_321001,
    t13.building_tag_321101,
    t13.building_tag_330101,
    t13.building_tag_330501,
    t13.building_tag_330601,
    t13.building_tag_334001,
    t13.building_tag_334101,
    t13.building_tag_334201,
    t13.building_tag_340301,
    t13.building_tag_423201,
    t13.building_tag_433301,
    t13.building_tag_110102,
    t13.building_tag_335401,
    t14.unit_tag_210101,
    t14.unit_tag_210301,
    t14.unit_tag_210302,
    t14.unit_tag_220101,
    t14.unit_tag_220201,
    t14.unit_tag_220301,
    t14.unit_tag_220401,
    t14.unit_tag_220501,
    t14.unit_tag_220601,
    t14.unit_tag_220701,
    t14.unit_tag_220801,
    t14.unit_tag_223101,
    t14.unit_tag_223201,
    t14.unit_tag_223401,
    t14.unit_tag_223501,
    t14.unit_tag_230101,
    t14.unit_tag_230103,
    t14.unit_tag_230202,
    t14.unit_tag_230203,
    t14.unit_tag_230204,
    t14.unit_tag_230401,
    t14.unit_tag_230501,
    t14.unit_tag_230601,
    t14.unit_tag_230701,
    t14.unit_tag_230801,
    t14.unit_tag_240104,
    t14.unit_tag_240201,
    t14.unit_tag_250201,
    t14.unit_tag_250301,
    t14.unit_tag_253401,
    t14.unit_tag_253501,
    t14.unit_tag_253601,
    t14.unit_tag_253701,
    t14.unit_tag_260301,
    t14.unit_tag_260501,
    t14.unit_tag_260503,
    t14.unit_tag_283401,
    t14.unit_tag_290101,
    t14.unit_tag_290201,
    t14.unit_tag_290301,
    t14.unit_tag_290401,
    t14.unit_tag_290501,
    t14.unit_tag_290601,
    t14.unit_tag_290801,
    t14.unit_tag_290901,
    t14.unit_tag_290902,
    t14.unit_tag_293101,
    t14.unit_tag_294301,
    t14.unit_tag_310501,
    t14.unit_tag_331001,
    t14.unit_tag_340101,
    t14.unit_tag_340102,
    t14.unit_tag_340201,
    t14.unit_tag_340401,
    t15.eki_population1,
    t15.lon1,
    t15.lat1,
    t15.distance1,
    t15.eki_population2,
    t15.lon2,
    t15.lat2,
    t15.distance2,

    t16.mesh_code,
    t18.pt2020_1km,
    t18.pt2025_1km,
    t18.pt2050_1km,
    t18.pt2070_1km,
    t18.fluctuate_2025_2020_1km,
    t18.fluctuate_2050_2020_1km,
    t18.fluctuate_2070_2020_1km,

    t17.med_price_500m,
    t17.avg_price_500m,
    t17.med_price_1km,
    t17.avg_price_1km,
    t17.med_price_3km,
    t17.avg_price_3km,
    t17.med_price_city,
    t17.avg_price_city,

    t19.cnt_station_500m,
    t19.sum_population_500m,
    t19.cnt_station_1km,
    t19.sum_population_1km,
    t19.cnt_station_3km,
    t19.sum_population_3km,
    t19.cnt_station_10km,
    t19.sum_population_10km,
    t19.min_distance_eki_population,
    t19.min_distance_eki,

    t1.target,
    t1.money_room,
    t1.weight,
    t1.weight2,
    t1.weight3,
    t1.log_weight,
    t1.fold_no
from
    {{ ref('01_01_train_dm_base') }} as t1
    left join
    {{ ref('02_18_brand_name') }} as t2
    on
        t1.id = t2.id
    left join
    {{ ref('02_08_dm_reform_wet_area') }} as t8
    on
        t1.id = t8.id
    left join
    {{ ref('02_09_dm_reform_wet_area_other') }} as t9
    on
        t1.id = t9.id
    left join
    {{ ref('02_10_dm_reform_interior') }} as t10
    on
        t1.id = t10.id
    left join
    {{ ref('02_11_dm_interior_other') }} as t11
    on
        t1.id = t11.id
    left join
    {{ ref('02_12_dm_reform_etc') }} as t12
    on
        t1.id = t12.id
    left join
    {{ ref('03_01_dm_building_tag_temp') }} as t13
    on
        t1.id = t13.id
    left join
    {{ ref('03_02_dm_union_tag_temp') }} as t14
    on
        t1.id = t14.id
    left join
    {{ ref('03_03_dm_station_temp') }} as t15
    on
        t1.id = t15.id
    left join
    {{ ref('03_05_dm_mesh_temp2') }} as t16
    on
        t1.id = t16.id
    left join
    {{ ref('03_06_dm_price_temp') }} as t17
    on
        t1.id = t17.id
    left join
    {{ ref('03_08_dm_mesh_temp2_1km') }} as t18
    on
        t1.id = t18.id
    left join
    {{ ref('03_10_dm_id_station') }} as t19
    on
        t1.id = t19.id
order by
    t1.id
