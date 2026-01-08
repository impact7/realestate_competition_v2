{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='02_12_' ~ train_test ~ '_dm_reform_etc') }}

select
    id,
    case
      when reform_etc is null then 0
      /* 90000: 大規模（フル/全面/リノベ等） */
      when strpos(reform_etc, 'スケルトン') > 0 then 90010
      when strpos(reform_etc, 'フルリノベ') > 0 then 90020
      when strpos(reform_etc, 'フルリフォーム') > 0 then 90030
      when strpos(reform_etc, '全面') > 0 and (strpos(reform_etc, 'リフォーム') > 0 or strpos(reform_etc, '改装') > 0 or strpos(reform_etc, '改修') > 0) then 90040
      when strpos(reform_etc, 'リノベーション') > 0 then 90050
      when strpos(reform_etc, 'リフォーム') > 0 then 90060
      when strpos(reform_etc, '内装') > 0 then 90070

      /* 91000: 外装（外壁・屋根・防水） */
      when strpos(reform_etc, '外壁') > 0 or strpos(reform_etc, '屋根') > 0 or strpos(reform_etc, '外装') > 0 then 91010
      when strpos(reform_etc, '防水') > 0 then 91020
      when strpos(reform_etc, '防蟻') > 0 then 91030

      /* 11000: 内装（クロス/床/畳/建具/美装まとめ） */
      when strpos(reform_etc, 'クロス') > 0 or strpos(reform_etc, '壁紙') > 0 then 11010
      when strpos(reform_etc, 'フローリング') > 0 or strpos(reform_etc, 'ＣＦ') > 0 or strpos(reform_etc, 'クッションフロア') > 0
        or strpos(reform_etc, 'フロアタイル') > 0 or strpos(reform_etc, 'カーペット') > 0 then 11020
      when strpos(reform_etc, '畳') > 0 or strpos(reform_etc, '襖') > 0 or strpos(reform_etc, '障子') > 0 then 11030
      when strpos(reform_etc, '建具') > 0 or strpos(reform_etc, '玄関') > 0 or strpos(reform_etc, 'ドア') > 0 then 11040
      when strpos(reform_etc, 'クリーニング') > 0 then 11050
      when strpos(reform_etc, '和室') > 0 and (strpos(reform_etc, '洋室') > 0 or strpos(reform_etc, '洋室に変更') > 0) then 11060

      /* 20000: 水回り（キッチン/浴室/トイレ/洗面） */
      when strpos(reform_etc, 'キッチン') > 0 or strpos(reform_etc, 'システムキッチン') > 0 then 20010
      when strpos(reform_etc, 'ユニットバス') > 0 or strpos(reform_etc, '浴室') > 0 or strpos(reform_etc, '浴槽') > 0 then 20020
      when strpos(reform_etc, 'トイレ') > 0 or strpos(reform_etc, 'ウォシュレット') > 0 or strpos(reform_etc, '温水洗浄') > 0 then 20030
      when strpos(reform_etc, '洗面') > 0 or strpos(reform_etc, 'シャンプードレッサー') > 0 then 20040

      /* 21000: 設備（給湯器/エアコン/電化/モニターホン等） */
      when strpos(reform_etc, '給湯器') > 0 or strpos(reform_etc, 'エコキュート') > 0 then 21010
      when (strpos(reform_etc, 'ＩＨ') > 0 or strpos(reform_etc, 'IH') > 0 or strpos(reform_etc, 'オール電化') > 0) then 21020
      when strpos(reform_etc, 'コンロ') > 0 then 21030
      when strpos(reform_etc, 'エアコン') > 0 then 21040
      when strpos(reform_etc, 'インターホン') > 0 or strpos(reform_etc, 'ドアホン') > 0 or strpos(reform_etc, 'モニター') > 0 then 21050

      else 90000
    end as reform_etc_category
from
    {{ ref(table_name) }}
