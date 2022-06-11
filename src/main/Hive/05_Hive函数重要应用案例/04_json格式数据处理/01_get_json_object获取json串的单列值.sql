--todo get_json_object  UDF函数
--功能：用于解析JSON字符串，可以从JSON字符串中返回指定的某个对象列的值
--用法：get_json_object(json字符串, 要返回的字段) todo 一般用$.字段名来返回字段的值
--缺点：每次只能返回JSON对象中一列的值


--创建表
use db_df2;
create table tb_json_test1
(
    json string
);

--加载数据
load data local inpath '/root/hivedata/device.json' into table tb_json_test1;

select *
from tb_json_test1;


select
    --获取设备名称
    get_json_object(json, "$.device")     as device,
    --获取设备类型
    get_json_object(json, "$.deviceType") as deviceType,
    --获取设备信号强度
    get_json_object(json, "$.signal")     as signal,
    --获取时间
    get_json_object(json, "$.time") as time1
from tb_json_test1;