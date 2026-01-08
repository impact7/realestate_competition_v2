{% set train_test = var('train_test') %}
{% set table_name = '01_01_' ~ train_test ~ '_dm_base' %}
{{ config(alias='02_11_' ~ train_test ~ '_dm_interior_other') }}

select
    id,
    case
      when reform_interior_other is null then 99999
      /* 90000台：大規模・概況（最優先） */
      when strpos(reform_interior_other, 'スケルトン') > 0 then 90010
      when strpos(reform_interior_other, 'フルリノベ') > 0 then 90020
      when strpos(reform_interior_other, 'フルリフォーム') > 0 then 90030
      when strpos(reform_interior_other, '全面リフォーム') > 0 then 90040
      when strpos(reform_interior_other, '大規模改修') > 0 then 90050
      when strpos(reform_interior_other, 'リノベーション') > 0 then 90060
      when strpos(reform_interior_other, '間取り変更') > 0 then 90070
      /* 11000台：壁・天井（クロス/塗装/漆喰） */
      when strpos(reform_interior_other, '全室クロス') > 0 then 11010
      when strpos(reform_interior_other, 'クロス全面') > 0 then 11011
      when strpos(reform_interior_other, '壁紙') > 0 and strpos(reform_interior_other, '張替') > 0 then 11012
      when strpos(reform_interior_other, 'クロス張替') > 0 then 11013
      when strpos(reform_interior_other, 'アクセントクロス') > 0 then 11020
      when strpos(reform_interior_other, '天井') > 0 and (strpos(reform_interior_other, '張替') > 0 or strpos(reform_interior_other, '貼替') > 0) then 11030
      when strpos(reform_interior_other, '塗装') > 0 then 11040
      when strpos(reform_interior_other, '漆喰') > 0 then 11050

      /* 12000台：床（フローリング/フロアタイル/CF/カーペット） */
      when strpos(reform_interior_other, 'フローリング全面') > 0 then 12010
      when strpos(reform_interior_other, 'フローリング張替') > 0 then 12011
      when strpos(reform_interior_other, '床張り替え') > 0 then 12012
      when strpos(reform_interior_other, '床貼替') > 0 then 12013
      when strpos(reform_interior_other, 'フロアタイル') > 0 and (strpos(reform_interior_other, '張替') > 0 or strpos(reform_interior_other, '貼替') > 0) then 12020
      when strpos(reform_interior_other, 'フロアタイル') > 0 then 12021
      when strpos(reform_interior_other, 'クッションフロア') > 0 and (strpos(reform_interior_other, '張替') > 0 or strpos(reform_interior_other, '貼替') > 0) then 12030
      when strpos(reform_interior_other, 'CF') > 0 and (strpos(reform_interior_other, '張替') > 0 or strpos(reform_interior_other, '貼替') > 0) then 12031
      when strpos(reform_interior_other, 'CF') > 0 then 12032
      when strpos(reform_interior_other, 'カーペット') > 0 and strpos(reform_interior_other, '張替') > 0 then 12040
      when strpos(reform_interior_other, '絨毯') > 0 and strpos(reform_interior_other, '張替') > 0 then 12041
      when strpos(reform_interior_other, '巾木') > 0 then 12090

      /* 13000台：和室まわり（畳/襖/障子/和→洋） */
      when strpos(reform_interior_other, '和室') > 0 and (strpos(reform_interior_other, '→洋室') > 0 or strpos(reform_interior_other, '洋室に変更') > 0) then 13010
      when strpos(reform_interior_other, '畳新調') > 0 or strpos(reform_interior_other, '畳交換') > 0 then 13020
      when strpos(reform_interior_other, '畳表替') > 0 then 13021
      when strpos(reform_interior_other, '琉球畳') > 0 then 13022
      when strpos(reform_interior_other, '襖') > 0 and (strpos(reform_interior_other, '張替') > 0 or strpos(reform_interior_other, '交換') > 0) then 13030
      when strpos(reform_interior_other, '障子') > 0 and (strpos(reform_interior_other, '張替') > 0 or strpos(reform_interior_other, '貼替') > 0 or strpos(reform_interior_other, '交換') > 0) then 13040
      when strpos(reform_interior_other, '掘りごたつ') > 0 then 13090

      /* 14000台：建具・室内ドア・鍵 */
      when strpos(reform_interior_other, '建具新調') > 0 or strpos(reform_interior_other, '建具交換') > 0 then 14010
      when strpos(reform_interior_other, '室内ドア') > 0 then 14011
      when strpos(reform_interior_other, 'ドア') > 0 and strpos(reform_interior_other, '交換') > 0 then 14012
      when strpos(reform_interior_other, 'レバーハンドル') > 0 then 14020
      when strpos(reform_interior_other, '鍵交換') > 0 then 14030

      /* 15000台：収納（クローゼット/下駄箱/棚） */
      when strpos(reform_interior_other, 'ウォークイン') > 0 then 15010
      when strpos(reform_interior_other, 'ウォークスルー') > 0 then 15011
      when strpos(reform_interior_other, 'クローゼット') > 0 and (strpos(reform_interior_other, '新設') > 0 or strpos(reform_interior_other, '新調') > 0 or strpos(reform_interior_other, '造作') > 0) then 15020
      when strpos(reform_interior_other, 'クローゼット') > 0 then 15021
      when strpos(reform_interior_other, '下駄箱') > 0 or strpos(reform_interior_other, 'シューズ') > 0 then 15030
      when strpos(reform_interior_other, '収納') > 0 then 15040
      when strpos(reform_interior_other, '棚') > 0 then 15050

      /* 16000台：窓・サッシ・二重窓（インプラス/内窓/ペアガラス） */
      when strpos(reform_interior_other, 'インプラス') > 0 then 16010
      when strpos(reform_interior_other, '内窓') > 0 then 16011
      when strpos(reform_interior_other, '二重サッシ') > 0 or strpos(reform_interior_other, '2重サッシ') > 0 then 16012
      when strpos(reform_interior_other, 'ペアガラス') > 0 then 16020
      when strpos(reform_interior_other, 'サッシ') > 0 and strpos(reform_interior_other, '交換') > 0 then 16030
      when strpos(reform_interior_other, '窓') > 0 and strpos(reform_interior_other, '交換') > 0 then 16031
      when strpos(reform_interior_other, '網戸') > 0 and (strpos(reform_interior_other, '張替') > 0 or strpos(reform_interior_other, '交換') > 0) then 16040

      /* 17000台：電気・照明・スイッチ類（内装寄り） */
      when strpos(reform_interior_other, 'ダウンライト') > 0 and (strpos(reform_interior_other, '新設') > 0 or strpos(reform_interior_other, '増設') > 0) then 17010
      when strpos(reform_interior_other, 'ダウンライト') > 0 then 17011
      when strpos(reform_interior_other, 'LED') > 0 and strpos(reform_interior_other, '照明') > 0 then 17020
      when strpos(reform_interior_other, '照明') > 0 then 17021
      when strpos(reform_interior_other, 'シーリング') > 0 then 17022
      when strpos(reform_interior_other, 'スイッチ') > 0 or strpos(reform_interior_other, 'コンセント') > 0 then 17030
      when strpos(reform_interior_other, '分電盤') > 0 then 17040

      /* 18000台：設備“軽微”系（内装文脈に出がち：エアコン/インターホン/カーテンレール等） */
      when strpos(reform_interior_other, 'エアコン') > 0 and (strpos(reform_interior_other, '新設') > 0 or strpos(reform_interior_other, '設置') > 0) then 18010
      when strpos(reform_interior_other, 'エアコン') > 0 and strpos(reform_interior_other, '交換') > 0 then 18011
      when strpos(reform_interior_other, 'エアコン') > 0 then 18012
      when strpos(reform_interior_other, 'TVモニター') > 0 or strpos(reform_interior_other, 'ＴＶモニター') > 0
        or strpos(reform_interior_other, 'TVドアホン') > 0 or strpos(reform_interior_other, 'インターホン') > 0 then 18020
      when strpos(reform_interior_other, 'カーテンレール') > 0 then 18030
      when strpos(reform_interior_other, 'ピクチャーレール') > 0 then 18031
      when strpos(reform_interior_other, '火災警報器') > 0 then 18040

      /* 19000台：清掃・美装（最後寄り） */
      when strpos(reform_interior_other, 'ハウスクリーニング') > 0 then 19010
      when strpos(reform_interior_other, 'クリーニング') > 0 then 19011
      when strpos(reform_interior_other, '美装') > 0 then 19020

      /* 該当なし */
      else 99000
    end as reform_interior_other_category
from
    {{ ref(table_name) }}
