--订单事实表（累积型快照事实表）
--每行数据代表一次订单记录，相关的维度有时间、用户、地区维度，度量值有运费/优惠金额/原始金额/最终金额。

--建表语句
DROP TABLE IF EXISTS dwd_order_info;
CREATE EXTERNAL TABLE dwd_order_info
(
    `id`                     STRING COMMENT '编号',
    `order_status`           STRING COMMENT '订单状态',
    `user_id`                STRING COMMENT '用户ID',
    `province_id`            STRING COMMENT '地区ID',
    --用户和地区维度外键
    `payment_way`            STRING COMMENT '支付方式',
    `delivery_address`       STRING COMMENT '邮寄地址',
    `out_trade_no`           STRING COMMENT '对外交易编号',
    `tracking_no`            STRING COMMENT '物流单号',
    --以上是维度字段，进行了维度退化
    `create_time`            STRING COMMENT '创建时间(未支付状态)',
    `payment_time`           STRING COMMENT '支付时间(已支付状态)',
    `cancel_time`            STRING COMMENT '取消时间(已取消状态)',
    `finish_time`            STRING COMMENT '完成时间(已完成状态)',
    `refund_time`            STRING COMMENT '退款时间(退款中状态)',
    `refund_finish_time`     STRING COMMENT '退款完成时间(退款完成状态)',
    `expire_time`            STRING COMMENT '过期时间',
    --以上时间字段均是时间维度外键
    `feight_fee`             DECIMAL(16, 2) COMMENT '运费',
    `feight_fee_reduce`      DECIMAL(16, 2) COMMENT '运费减免',
    `activity_reduce_amount` DECIMAL(16, 2) COMMENT '活动减免',
    `coupon_reduce_amount`   DECIMAL(16, 2) COMMENT '优惠券减免',
    `original_amount`        DECIMAL(16, 2) COMMENT '订单原始价格',
    `final_amount`           DECIMAL(16, 2) COMMENT '订单最终价格'
    --以上均是度量值字段
) COMMENT '订单事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_order_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


---分区情况及数据走向和数据装载
--分区规划类似拉链表，有一个dt=9999-99-99分区用于存放未完成状态的数据，即，其他分区为dt=2021-10-13
--这种具体时间，其中存放数据为已经完成的数据即取消订单、退款成功、确认收货七天之后、订单超时或者过期，是不再发生变化的数据，
--将可能发生变化的数据和不可能发生变化的数据放在两个不同的分区，方便进行数据的更新操作。


--首日装载
insert overwrite table dwd_order_info partition (dt)
select oi.id,
       oi.order_status,
       oi.user_id,
       oi.province_id,
       oi.payment_way,
       oi.delivery_address,
       oi.out_trade_no,
       oi.tracking_no,
       oi.create_time,
       times.ts['1002'] payment_time,
       times.ts['1003'] cancel_time,
       times.ts['1004'] finish_time,
       times.ts['1005'] refund_time,
       times.ts['1006'] refund_finish_time,
       --获取字典中的时间作为时间外键的数据
       oi.expire_time,
       feight_fee,
       feight_fee_reduce,
       activity_reduce_amount,
       coupon_reduce_amount,
       original_amount,
       final_amount,
       case
           when times.ts['1003'] is not null then date_format(times.ts['1003'], 'yyyy-MM-dd')
           when times.ts['1004'] is not null and
                date_add(date_format(times.ts['1004'], 'yyyy-MM-dd'), 7) <= '2020-06-14' and times.ts['1005'] is null
               then date_add(date_format(times.ts['1004'], 'yyyy-MM-dd'), 7)
           when times.ts['1006'] is not null then date_format(times.ts['1006'], 'yyyy-MM-dd')
           when oi.expire_time is not null then date_format(oi.expire_time, 'yyyy-MM-dd')
           else '9999-99-99'
           end
       --状态判断when代表是完成的状态放到完成的分区汇总，如果不是上面四种完成状态就都是未完成状态放入dt=9999-99-99分区
from (
         select *
         from ods_order_info
         where dt = '2020-06-14'
     ) oi
         left join
     (
         select order_id,
                str_to_map(concat_ws(',', collect_set(concat(order_status, '=', operate_time))), ',', '=') ts
         from ods_order_status_log
         where dt = '2020-06-14'
         group by order_id
     ) times
     on oi.id = times.order_id;


--每日装载
insert overwrite table dwd_order_info partition (dt)
select nvl(new.id, old.id),
       nvl(new.order_status, old.order_status),
       nvl(new.user_id, old.user_id),
       nvl(new.province_id, old.province_id),
       nvl(new.payment_way, old.payment_way),
       nvl(new.delivery_address, old.delivery_address),
       nvl(new.out_trade_no, old.out_trade_no),
       nvl(new.tracking_no, old.tracking_no),
       nvl(new.create_time, old.create_time),
       nvl(new.payment_time, old.payment_time),
       nvl(new.cancel_time, old.cancel_time),
       nvl(new.finish_time, old.finish_time),
       nvl(new.refund_time, old.refund_time),
       nvl(new.refund_finish_time, old.refund_finish_time),
       nvl(new.expire_time, old.expire_time),
       nvl(new.feight_fee, old.feight_fee),
       nvl(new.feight_fee_reduce, old.feight_fee_reduce),
       nvl(new.activity_reduce_amount, old.activity_reduce_amount),
       nvl(new.coupon_reduce_amount, old.coupon_reduce_amount),
       nvl(new.original_amount, old.original_amount),
       nvl(new.final_amount, old.final_amount),
       case
           when new.cancel_time is not null then date_format(new.cancel_time, 'yyyy-MM-dd')
           when new.finish_time is not null and
                date_add(date_format(new.finish_time, 'yyyy-MM-dd'), 7) = '2020-06-15' and new.refund_time is null
               then '2020-06-15'
           when new.refund_finish_time is not null then date_format(new.refund_finish_time, 'yyyy-MM-dd')
           when new.expire_time is not null then date_format(new.expire_time, 'yyyy-MM-dd')
           else '9999-99-99'
           end
from (
         select id,
                order_status,
                user_id,
                province_id,
                payment_way,
                delivery_address,
                out_trade_no,
                tracking_no,
                create_time,
                payment_time,
                cancel_time,
                finish_time,
                refund_time,
                refund_finish_time,
                expire_time,
                feight_fee,
                feight_fee_reduce,
                activity_reduce_amount,
                coupon_reduce_amount,
                original_amount,
                final_amount
         from dwd_order_info
         where dt = '9999-99-99'
     ) old
         --dt=9999-99-99分区中的未完成数据
         full outer join
     (
         select oi.id,
                oi.order_status,
                oi.user_id,
                oi.province_id,
                oi.payment_way,
                oi.delivery_address,
                oi.out_trade_no,
                oi.tracking_no,
                oi.create_time,
                times.ts['1002'] payment_time,
                times.ts['1003'] cancel_time,
                times.ts['1004'] finish_time,
                times.ts['1005'] refund_time,
                times.ts['1006'] refund_finish_time,
                oi.expire_time,
                feight_fee,
                feight_fee_reduce,
                activity_reduce_amount,
                coupon_reduce_amount,
                original_amount,
                final_amount
         from (
                  select *
                  from ods_order_info
                  where dt = '2020-06-15'
              ) oi
                  left join
              (
                  select order_id,
                         str_to_map(concat_ws(',', collect_set(concat(order_status, '=', operate_time))), ',', '=') ts
                  from ods_order_status_log
                  where dt = '2020-06-15'
                  group by order_id
              ) times
              on oi.id = times.order_id
     ) new
         --新增及变化数据
     on old.id = new.id;
