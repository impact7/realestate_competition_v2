{{ config(
    enabled = (var('train_test') == 'train')
) }}

with temp1 as (
select
    row_number() over(order by money_room / house_area) as id,
    target_ym,
    building_id,
    building_status,
    cast(replace(left(building_create_date, 10), '-', '') as bigint) as building_create_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100)
        - cast(cast(replace(left(building_create_date, 7), '-', '') as bigint) / 100 as int) * 12
        - mod(cast(replace(left(building_create_date, 7), '-', '') as bigint), 100) as building_create_month,
    cast(replace(left(building_modify_date, 10), '-', '') as bigint) as building_modify_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100)
        - cast(cast(replace(left(building_modify_date, 7), '-', '') as bigint) / 100 as int) * 12
        - mod(cast(replace(left(building_modify_date, 7), '-', '') as bigint), 100) as building_modify_month,
    building_type,
    unit_count,
    lon,
    lat,
    building_structure,
    total_floor_area,
    building_area,
    floor_count,
    basement_floor_count,
    year_built,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100) - cast(year_built / 100 as int) * 12 - mod(year_built, 100) as month_built,
    building_land_area,
    land_area_all,
    unit_area_min,
    unit_area_max,
    building_land_chimoku,
    land_youto,
    land_toshi,
    land_chisei,
    land_area_kind,
    land_setback_flg,
    land_setback,
    land_kenpei,
    land_youseki,
    land_road_cond,
    building_area_kind,
    management_association_flg,
    case
        when reform_exterior is null then 0
        when reform_exterior='1' or reform_exterior='1 ' then 1
        when reform_exterior='1/2' then 2
        when reform_exterior='2' then 3
        when reform_exterior='2/' or reform_exterior='2/1' then 4
        else 5
    end reform_exterior,
    reform_exterior_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100) - cast(reform_exterior_date / 100 as int) * 12 - mod(reform_exterior_date, 100) as reform_exterior_month,
    unit_id,
    room_floor,
    case when total_floor_area = 0 then 0 else room_floor / total_floor_area end as rate_room_floor,
    balcony_area,
    dwelling_unit_window_angle,
    room_count,
    case when unit_area is null then 0 else 1 end as unit_area_exist_flg,
    floor_plan_code,
    reform_date,
    reform_wet_area_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100) - cast(reform_wet_area_date / 100 as int) * 12 - mod(reform_wet_area_date, 100) as reform_wet_area_month,
    reform_interior_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100) - cast(reform_interior_date / 100 as int) * 12 - mod(reform_interior_date, 100) as reform_interior_month,
    bukken_id,
    cast(replace(left(snapshot_create_date, 10), '-', '') as bigint) as snapshot_create_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100)
        - cast(cast(replace(left(snapshot_create_date, 7), '-', '') as bigint) / 100 as int) * 12
        - mod(cast(replace(left(snapshot_create_date, 7), '-', '') as bigint), 100) as snapshot_create_month,
    cast(replace(left(new_date, 10), '-', '') as bigint) as new_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100)
        - cast(cast(replace(left(new_date, 7), '-', '') as bigint) / 100 as int) * 12
        - mod(cast(replace(left(new_date, 7), '-', '') as bigint), 100) as new_month,
    cast(replace(left(snapshot_modify_date, 10), '-', '') as bigint) as snapshot_modify_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100)
        - cast(cast(replace(left(snapshot_modify_date, 7), '-', '') as bigint) / 100 as int) * 12
        - mod(cast(replace(left(snapshot_modify_date, 7), '-', '') as bigint), 100) as snapshot_modify_month,
    cast(replace(left(timelimit_date, 10), '-', '') as bigint) as timelimit_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100)
        - cast(cast(replace(left(timelimit_date, 7), '-', '') as bigint) / 100 as int) * 12
        - mod(cast(replace(left(timelimit_date, 7), '-', '') as bigint), 100) as timelimit_month,
    bukken_type,
    flg_investment,
    empty_number,
    post1,
    post2,
    post1 * 10000 + post2 as post,
    addr1_1,
    addr1_2,
    addr1_1*1000+addr1_2 as addr,
    nl,
    el,
    bus_time1,
    walk_distance1,
    walk_distance2,
    traffic_car,
    snapshot_land_area,
    snapshot_land_shidou,
    land_shidou_a,
    land_shidou_b,
    land_mochibun_a,
    land_mochibun_b,
    case
        when house_area <= 15 and building_type = 4 and greatest(house_area, unit_area, total_floor_area) <= 15 then greatest(house_area, unit_area, total_floor_area, snapshot_land_area)
        when house_area <= 15 and building_type = 4 and greatest(house_area, unit_area, total_floor_area) > 15 then greatest(house_area, unit_area, total_floor_area)
        when house_area <= 15 and building_type = 1 and unit_area is not null then greatest(house_area, unit_area)
        when house_area <= 15 and building_type = 1 and unit_area is null then greatest(house_area, unit_area, total_floor_area)
        when house_area >= 1000 and least(house_area, unit_area, total_floor_area) < 1000 then least(house_area, unit_area, total_floor_area)
        when house_area >= 2500 and building_type<>4 then round(cast(house_area*1.0 as numeric) / 100, 2)
        when house_area >= 1000 and building_type<>4 then round(cast(house_area*1.0 as numeric) / 10, 2)
        when house_area >= 7000 and building_type=4 then round(cast(house_area*1.0 as numeric) / 100, 2)
        when house_area >= 1500 and building_type=4 then round(cast(house_area*1.0 as numeric) / 10, 2)
        when house_area >= 500 then least(house_area, unit_area)
        when floor(house_area) = floor(unit_area) then unit_area
        when floor(house_area) = floor(total_floor_area) then total_floor_area
        else house_area
    end as house_area,
    flg_new,
    house_kanrinin,
    room_kaisuu,
    snapshot_window_angle,
    madori_number_all,
    madori_kind_all,
    money_kyoueki,
    money_kyoueki_tax,
    money_rimawari_now,
    money_shuuzen,
    money_shuuzenkikin,
    money_sonota1 + money_sonota2 + money_sonota3 as money_sonota,
    parking_money,
    parking_money_tax,
    parking_kubun,
    parking_distance,
    parking_number,
    genkyo_code,
    usable_status,
    usable_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100) - cast(usable_date / 100 as int) * 12 - mod(usable_date, 100) as usable_month,
    case
        when renovation_date is null then null
        else cast(replace(renovation_date, '-', '') as bigint)
    end as renovation_date,
    cast(target_ym / 100 as int) * 12 + mod(target_ym, 100)
        - cast(cast(replace(left(renovation_date, 7), '-', '') as bigint) / 100 as int) * 12
        - mod(cast(replace(left(renovation_date, 7), '-', '') as bigint), 100) as renovation_month,
    money_room,

    homes_building_name,
    reform_place,
    building_tag_id,
    building_name,
    reform_common_area,
    reform_place_other,
    reform_wet_area,
    reform_exterior_other,
    reform_wet_area_other,
    reform_interior,
    reform_interior_other,
    unit_tag_id,
    reform_etc,
    replace(replace(replace(replace(replace(replace(replace(replace(
        case when strpos(eki_name1, '(') > 0 then left(eki_name1, strpos(eki_name1, '(') - 1) else eki_name1 end,
            'ケ', 'ヶ'), '笹塚', '笹塚'), 'なんば', '難波'), 'あびこ', '我孫子'),
            '京成町屋', '町屋'), 'なかもず', '中百舌鳥'), '泉ヶ丘', '泉ケ丘'), '西鉄福岡', '西鉄福岡（天神）'
    ) as eki_name1,
    replace(replace(replace(replace(replace(replace(replace(replace(
        case when strpos(eki_name2, '(') > 0 then left(eki_name1, strpos(eki_name2, '(') - 1) else eki_name2 end,
            'ケ', 'ヶ'), '笹塚', '笹塚'), 'なんば', '難波'), 'あびこ', '我孫子'),
            '京成町屋', '町屋'), 'なかもず', '中百舌鳥'), '泉ヶ丘', '泉ケ丘'), '西鉄福岡', '西鉄福岡（天神）'
    ) as eki_name2,
    full_address,
    rosen_name1,
    land_seigen,
    unit_area
from
    "raw_data"."train"
), temp2 as (
select
    *,
    round(cast(money_room / house_area as numeric), 4) as target
from
    temp1
)
select
    *,
    case
        when target >= 1000000 then 1
        when ln(1000000 / target)+1 <= 10 then round(cast(ln(1000000 / target)+1 as numeric), 2)
        else 10
    end as weight,
    case
        when 1000000 / target >= 50 then 50
        when 1000000 / target <= 1 then 1
        else round(cast(1000000 / target as numeric), 2)
    end as weight2,
    case
        when target_ym = 202201 then 1.5
        when target_ym = 202207 then 2
        else 1
    end as weight3,
    case
        when target >= 1000000 then 1
        when ln(1000000 / target)*2+1 <= 10 then round(cast(ln(1000000 / target)*2+1 as numeric), 2)
        else 10
    end as log_weight,
    mod(id, 5) as fold_no
from
    temp2
