--查询ODS层两张活动相关的表进行join之后进行装载，以时间进行分区。
--建表语句
DROP TABLE IF EXISTS dim_activity_rule_info;
CREATE EXTERNAL TABLE dim_activity_rule_info
(
    `activity_rule_id` STRING COMMENT '活动规则ID',
    `activity_id`      STRING COMMENT '活动ID',
    `activity_name`    STRING COMMENT '活动名称',
    `activity_type`    STRING COMMENT '活动类型',
    `start_time`       STRING COMMENT '开始时间',
    `end_time`         STRING COMMENT '结束时间',
    `create_time`      STRING COMMENT '创建时间',
    `condition_amount` DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`    BIGINT COMMENT '满减件数',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_level`    STRING COMMENT '优惠级别'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dim/dim_activity_rule_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


-------装载语句
insert overwrite table dim_activity_rule_info partition (dt = '2021-10-13')
select ar.id,
       ar.activity_id,
       ai.activity_name,
       ar.activity_type,
       ai.start_time,
       ai.end_time,
       ai.create_time,
       ar.condition_amount,
       ar.condition_num,
       ar.benefit_amount,
       ar.benefit_discount,
       ar.benefit_level
from (
         select id,
                activity_id,
                activity_type,
                condition_amount,
                condition_num,
                benefit_amount,
                benefit_discount,
                benefit_level
         from ods_activity_rule
         where dt = '2021-10-13'
     ) ar
         left join
     (
         select id,
                activity_name,
                start_time,
                end_time,
                create_time
         from ods_activity_info
         where dt = '2021-10-13'
     ) ai
     on ar.activity_id = ai.id;

