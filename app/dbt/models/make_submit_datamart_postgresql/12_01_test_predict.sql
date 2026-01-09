select
    lpad(cast(t1.id as text), 6, '0') as id,
    cast(t2.y_pred as bigint) as predict
from
    "prepare_data"."04_15_test_dm" as t1
    inner join
    "output"."test_predict2" as t2
    on
        t1.id = t2.id
order by
    t1.id
