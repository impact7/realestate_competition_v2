{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='02_09_' ~ train_test ~ '_dm_reform_wet_area_other') }}

select
    id,
    CASE
      WHEN reform_wet_area_other IS NULL THEN 99999

      /* --- 給湯・熱源（15xxx） --- */
      WHEN strpos(reform_wet_area_other, 'エコキュート') > 0 THEN 15110
      WHEN strpos(reform_wet_area_other, 'エネファーム') > 0 THEN 15120
      WHEN strpos(reform_wet_area_other, 'エコジョーズ') > 0 THEN 15130
      WHEN strpos(reform_wet_area_other, 'TES') > 0 THEN 15140
      WHEN strpos(reform_wet_area_other, '給湯器') > 0 THEN 15100
      WHEN strpos(reform_wet_area_other, '電気温水器') > 0 THEN 15150

      /* --- キッチン（11xxx） --- */
      WHEN strpos(reform_wet_area_other, 'システムキッチン') > 0 THEN 11000
      WHEN strpos(reform_wet_area_other, 'キッチン交換') > 0 THEN 11010
      WHEN strpos(reform_wet_area_other, 'キッチン新調') > 0 THEN 11011
      WHEN strpos(reform_wet_area_other, '食器洗浄乾燥機') > 0 OR strpos(reform_wet_area_other, '食洗機') > 0 THEN 11050
      WHEN strpos(reform_wet_area_other, 'レンジフード') > 0 THEN 11200
      WHEN strpos(reform_wet_area_other, '水栓') > 0 OR strpos(reform_wet_area_other, 'カラン') > 0 OR strpos(reform_wet_area_other, '混合栓') > 0 THEN 11300

      /* コンロ系（111xx） */
      WHEN strpos(reform_wet_area_other, 'ビルトインコンロ') > 0 THEN 11110
      WHEN strpos(reform_wet_area_other, 'ガラストップコンロ') > 0 THEN 11121
      WHEN strpos(reform_wet_area_other, 'ガスコンロ') > 0 OR strpos(reform_wet_area_other, 'ガスレンジ') > 0 THEN 11120
      WHEN strpos(reform_wet_area_other, 'IHクッキングヒーター') > 0 OR strpos(reform_wet_area_other, 'IHコンロ') > 0 OR strpos(reform_wet_area_other, 'IH') > 0 THEN 11130

      /* --- 浴室（12xxx） --- */
      WHEN strpos(reform_wet_area_other, 'ユニットバス') > 0 OR strpos(reform_wet_area_other, 'UB') > 0 THEN 12000
      WHEN strpos(reform_wet_area_other, '浴室乾燥') > 0 OR strpos(reform_wet_area_other, '浴室暖房') > 0 OR strpos(reform_wet_area_other, '換気乾燥') > 0 THEN 12100
      WHEN strpos(reform_wet_area_other, '浴室') > 0 AND (strpos(reform_wet_area_other, '鏡') > 0 OR strpos(reform_wet_area_other, 'シャワー') > 0) THEN 12200
      WHEN strpos(reform_wet_area_other, 'コーティング') > 0 AND strpos(reform_wet_area_other, '浴室') > 0 THEN 12210

      /* --- トイレ（13xxx） --- */
      WHEN strpos(reform_wet_area_other, 'タンクレス') > 0 AND strpos(reform_wet_area_other, 'トイレ') > 0 THEN 13020
      WHEN strpos(reform_wet_area_other, '便器') > 0 OR strpos(reform_wet_area_other, 'トイレ交換') > 0 OR strpos(reform_wet_area_other, 'トイレ新規交換') > 0 THEN 13000
      WHEN strpos(reform_wet_area_other, 'ウォシュレット') > 0 OR strpos(reform_wet_area_other, 'ウォッシュレット') > 0 OR strpos(reform_wet_area_other, '洗浄便座') > 0 OR strpos(reform_wet_area_other, '便座') > 0 THEN 13100

      /* --- 洗面・ランドリー（14xxx） --- */
      WHEN strpos(reform_wet_area_other, '洗面化粧台') > 0 OR strpos(reform_wet_area_other, '洗面台') > 0 OR strpos(reform_wet_area_other, 'シャンプードレッサー') > 0 THEN 14100
      WHEN strpos(reform_wet_area_other, '洗濯パン') > 0 OR strpos(reform_wet_area_other, '防水パン') > 0 THEN 14200
      WHEN strpos(reform_wet_area_other, '洗濯水栓') > 0 OR strpos(reform_wet_area_other, '洗濯機水栓') > 0 THEN 14210

      /* --- その他（2xxxx） --- */
      WHEN strpos(reform_wet_area_other, 'クロス') > 0 THEN 20100
      WHEN strpos(reform_wet_area_other, 'フローリング') > 0 OR strpos(reform_wet_area_other, 'フロアタイル') > 0 THEN 20110
      WHEN strpos(reform_wet_area_other, 'CF') > 0 OR strpos(reform_wet_area_other, 'クッションフロア') > 0 THEN 20120
      WHEN strpos(reform_wet_area_other, '建具') > 0 OR strpos(reform_wet_area_other, 'ドア') > 0 THEN 20130
      WHEN strpos(reform_wet_area_other, 'クリーニング') > 0 THEN 20200
      WHEN strpos(reform_wet_area_other, 'エアコン') > 0 THEN 20300
      WHEN strpos(reform_wet_area_other, 'TV') > 0 AND strpos(reform_wet_area_other, 'インターホン') > 0 THEN 20400
      WHEN strpos(reform_wet_area_other, '照明') > 0 OR strpos(reform_wet_area_other, 'LED') > 0 THEN 20500

      ELSE 90000
    END as reform_wet_area_other_category
from
    {{ ref(table_name) }}
