select
    lpad(cast(t1.id as text), 6, '0') as id,
    least(cast(t2.y_pred * t1.house_area as bigint), cast(t3.y_pred as bigint)) as predict
from
    "prepare_data"."04_15_test_dm" as t1
    inner join
    "output"."test_predict" as t2
    on
        t1.id = t2.id
    inner join
    "output"."test_predict2" as t3
    on
        t1.id = t3.id
order by
    t1.id
