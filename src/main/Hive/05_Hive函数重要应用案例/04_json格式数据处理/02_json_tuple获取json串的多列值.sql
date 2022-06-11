--todo json_tuple
--功能：用于实现JSON字符串的解析，可以通过指定多个参数来解析JSON返回多列的值
--用法：json_tuple(json字符串, 要返回的字段1, 要返回的字段2, ..., 要返回的字段n)
--特点：功能类似于get_json_object，但是可以调用一次返回多列的值，属于UDTF类型函数，todo 一般搭配lateral view使用
--     返回的每一列都是字符串类型

--单独使用
use db_df2;
select *
from tb_json_test1;

select
    --解析所有字段
    json_tuple(json, "device", "deviceType", "signal", "time") as (device, deviceType, signal, stime)
from tb_json_test1;

select get_json_object(json, '$.device')--get_josn_object调用json中数据时用的是$.column
       --json_tuple调用json数据时用的是column
from tb_json_test1;

--搭配侧视图使用
select json,
       device,
       deviceType,
       signal,
       stime
from tb_json_test1
         lateral view json_tuple(json, "device", "deviceType", "signal", "time") b
         as device, deviceType, signal, stime;