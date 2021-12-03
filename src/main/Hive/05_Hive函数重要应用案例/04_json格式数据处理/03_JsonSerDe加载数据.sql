--todo JsonSerDe
-- 包路径： org.apache.hive.hcatalog.data.JsonSerDe
--创建表
create table tb_json_test2
(
    device     string,
    deviceType string,
    signal     double,
    `time`     string
)
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    STORED AS TEXTFILE;

load data local inpath '/root/hivedata/device.json' into table tb_json_test2;

select *
from tb_json_test2;
