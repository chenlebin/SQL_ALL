--退单事实表（事务型事实表）
--每行数据代表一次退单记录，采用增量同步方式，按天进行分区，每个分区中存储的是每日新增的退单数据。
--关联的维度有时间、用户、地区、商品四个维度，度量值有件数/金额两个，如下是退单事实表的建表语句。

DROP TABLE IF EXISTS dwd_order_refund_info;
CREATE EXTERNAL TABLE dwd_order_refund_info
(
    `id`                 STRING COMMENT '编号',
    `user_id`            STRING COMMENT '用户ID',
    `order_id`           STRING COMMENT '订单ID',
    `sku_id`             STRING COMMENT '商品ID',
    `province_id`        STRING COMMENT '地区ID',
    --以上均是维度外键
    `refund_type`        STRING COMMENT '退单类型',
    `refund_num`         BIGINT COMMENT '退单件数',
    `refund_amount`      DECIMAL(16, 2) COMMENT '退单金额',
    `refund_reason_type` STRING COMMENT '退单原因类型',
    --以退单件数和金额均是度量值，退单类型和退单原因类型均是维度字段使用维度退化
    --下面的退单时间其实就是时间维度的外键
    `create_time`        STRING COMMENT '退单时间'
) COMMENT '退单事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_order_refund_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

--首日装载
insert overwrite table dwd_order_refund_info partition (dt)
select ri.id,
       ri.user_id,
       ri.order_id,
       ri.sku_id,
       oi.province_id,
       ri.refund_type,
       ri.refund_num,
       ri.refund_amount,
       ri.refund_reason_type,
       ri.create_time,
       date_format(ri.create_time, 'yyyy-MM-dd')
from (
         select *
         from ods_order_refund_info
         where dt = '2021-10-13'
     ) ri
         left join
     (
         select id, province_id
         from ods_order_info
         where dt = '2021-10-13'
     ) oi
     on ri.order_id = oi.id;
--每日装载
insert overwrite table dwd_order_refund_info partition (dt = '2021-10-14')
select ri.id,
       ri.user_id,
       ri.order_id,
       ri.sku_id,
       oi.province_id,
       ri.refund_type,
       ri.refund_num,
       ri.refund_amount,
       ri.refund_reason_type,
       ri.create_time
from (
         select *
         from ods_order_refund_info
         where dt = '2021-10-14'
     ) ri
         left join
     (
         select id, province_id
         from ods_order_info
         where dt = '2021-10-14'
     ) oi
     on ri.order_id = oi.id;
