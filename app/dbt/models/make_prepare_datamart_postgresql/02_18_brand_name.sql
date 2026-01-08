{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='02_18_' ~ train_test ~ '_brand_name') }}

with temp1 as (
select
    id,
    case
        when strpos(homes_building_name, 'ライオンズ') > 0 then 1
        when strpos(homes_building_name, 'ダイアパレス') > 0 then 2
        when strpos(homes_building_name, 'コスモ') > 0 then 3
        when strpos(homes_building_name, 'サーパス') > 0 then 4
        when strpos(homes_building_name, '朝日プラザ') > 0 then 5
        when strpos(homes_building_name, 'レーベン') > 0 then 6
        when strpos(homes_building_name, 'エスリード') > 0 then 7
        when strpos(homes_building_name, 'ファミール') > 0 then 8
        when strpos(homes_building_name, 'ルネ') > 0 then 9
        when strpos(homes_building_name, 'クリオ') > 0 then 10
        when strpos(homes_building_name, 'プレサンス') > 0 then 11
        when strpos(homes_building_name, 'レクセル') > 0 then 12
        when strpos(homes_building_name, 'エステム') > 0 then 13
        when strpos(homes_building_name, 'ワコーレ') > 0 then 14
        when strpos(homes_building_name, 'ローレル') > 0 then 15
        when strpos(homes_building_name, 'イトーピア') > 0 then 16
        when strpos(homes_building_name, '日商岩井') > 0 then 17
        when strpos(homes_building_name, 'ネオハイツ') > 0 then 18
        when strpos(homes_building_name, 'ジオ') > 0 then 19
        when strpos(homes_building_name, 'シティタワー') > 0 then 20
        when strpos(homes_building_name, 'パークハウス') > 0 then 21
        when strpos(homes_building_name, 'パークシティ') > 0 then 22
        when strpos(homes_building_name, 'パークホームズ') > 0 then 23
        when strpos(homes_building_name, 'チサンマンション') > 0 then 24
        when strpos(homes_building_name, 'シャンボール') > 0 then 25
        when strpos(homes_building_name, 'アルファ') > 0 then 26
        when strpos(homes_building_name, 'グランドメゾン') > 0 then 27
        when strpos(homes_building_name, 'グランシティ') > 0 then 28
        when strpos(homes_building_name, 'サンクレイドル') > 0 then 29
        when strpos(homes_building_name, 'ブリリア') > 0 then 30
        when strpos(homes_building_name, 'プラウド') > 0 then 31
        when strpos(homes_building_name, 'シティハウス') > 0 then 32
        when strpos(homes_building_name, '藤和') > 0 then 33
        when strpos(homes_building_name, 'クレスト') > 0 then 34
        when strpos(homes_building_name, 'デュオ') > 0 then 35
        when strpos(homes_building_name, 'コープ野村') > 0 then 36
        when strpos(homes_building_name, '日神') > 0 then 37
        when strpos(homes_building_name, 'オーベル') > 0 then 38
        when strpos(homes_building_name, 'セザール') > 0 then 39
        when strpos(homes_building_name, 'エクレール') > 0 then 40
        when strpos(homes_building_name, 'メイツ') > 0 then 41
        when strpos(homes_building_name, 'エンゼル') > 0 then 42
        when strpos(homes_building_name, 'プレミスト') > 0 then 43
        when strpos(homes_building_name, 'ナイス') > 0 then 44
        when strpos(homes_building_name, 'ポレスター') > 0 then 45
        when strpos(homes_building_name, 'ユニーブル') > 0 then 46
        when strpos(homes_building_name, 'リビオ') > 0 then 47
        when strpos(homes_building_name, 'ヴェレーナ') > 0 then 48
        when strpos(homes_building_name, 'シティテラス') > 0 then 49
        when strpos(homes_building_name, 'グランドパレス') > 0 then 50
        else 99
    end as brand_name
from
    {{ ref(table_name) }}
)
select
    *
from
    temp1