--一行数据是一个日期，也不进行分区处理。
--数据装载 并不是来自业务系统 ，而是需要自己手动写入，一般一次性导入一年的数据。
--建表语句
DROP TABLE IF EXISTS dim_date_info;
CREATE EXTERNAL TABLE dim_date_info
(
    `date_id`    STRING COMMENT '日',
    `week_id`    STRING COMMENT '周ID',
    `week_day`   STRING COMMENT '周几',
    `day`        STRING COMMENT '每月的第几天',
    `month`      STRING COMMENT '第几月',
    `quarter`    STRING COMMENT '第几季度',
    `year`       STRING COMMENT '年',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '时间维度表'
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dim/dim_date_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

--创建一个临时表先将时间维度的文件装载到临时表中（使用web页面上传的方式）然后在insert+select到时间维度表中
DROP TABLE IF EXISTS tmp_dim_date_info;
CREATE EXTERNAL TABLE tmp_dim_date_info
(
    `date_id`    STRING COMMENT '日',
    `week_id`    STRING COMMENT '周ID',
    `week_day`   STRING COMMENT '周几',
    `day`        STRING COMMENT '每月的第几天',
    `month`      STRING COMMENT '第几月',
    `quarter`    STRING COMMENT '第几季度',
    `year`       STRING COMMENT '年',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '时间维度表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/tmp/tmp_dim_date_info/';

--将临时表中文件插入到时间维度表中
insert overwrite table dim_date_info
select *
from tmp_dim_date_info;


select *
from dim_date_info;



