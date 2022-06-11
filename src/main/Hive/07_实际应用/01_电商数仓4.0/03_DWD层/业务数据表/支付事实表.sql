--支付型事实表（累积型快照事实表）
--每行数据是一条支付记录，相关的维度有时间、用户、地区维度，度量值有支付金额。

--建表语句
DROP TABLE IF EXISTS dwd_payment_info;
CREATE EXTERNAL TABLE dwd_payment_info
(
    `id`             STRING COMMENT '编号',
    `order_id`       STRING COMMENT '订单编号',
    `user_id`        STRING COMMENT '用户编号',
    `province_id`    STRING COMMENT '地区ID',
    --以上都是用户、地区维度外键
    `trade_no`       STRING COMMENT '交易编号',
    `out_trade_no`   STRING COMMENT '对外交易编号',
    `payment_type`   STRING COMMENT '支付类型',
    `payment_amount` DECIMAL(16, 2) COMMENT '支付金额',
    `payment_status` STRING COMMENT '支付状态',
    --以上字段除支付金额是度量值外其他字段均是维度字段，使用维度退化
    `create_time`    STRING COMMENT '创建时间',--调用第三方支付接口的时间
    `callback_time`  STRING COMMENT '完成时间'--支付完成时间，即支付成功回调时间
    --这两个时间均是时间维度外键
) COMMENT '支付事实表表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_payment_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


---分区情况及数据走向和数据装载
--分区规划类似拉链表，有一个dt=9999-99-99分区用于存放未完成状态的数据，也就是创建但未支付的数据，其他分区为dt=2021-10-13这种具体时间
--其中存放数据为已近完成的数据，即创建并完成支付的数据，是不再发生变化的数据，将可能发生变化的数据和不可能发生变化的数据放
--在两个不同的分区，方便进行数据的更新操作。

--首日装载
insert overwrite table dwd_payment_info partition (dt)
select pi.id,
       pi.order_id,
       pi.user_id,
       oi.province_id,
       pi.trade_no,
       pi.out_trade_no,
       pi.payment_type,
       pi.payment_amount,
       pi.payment_status,
       pi.create_time,
       pi.callback_time,
       nvl(date_format(pi.callback_time, 'yyyy-MM-dd'), '9999-99-99')
from (
         select *
         from ods_payment_info
         where dt = '2021-10-13'
     ) pi
         left join
     (
         select id, province_id
         from ods_order_info
         where dt = '2021-10-13'
     ) oi
     on pi.order_id = oi.id;


--每日装载
insert overwrite table dwd_payment_info partition (dt)
select nvl(new.id, old.id),
       nvl(new.order_id, old.order_id),
       nvl(new.user_id, old.user_id),
       nvl(new.province_id, old.province_id),
       nvl(new.trade_no, old.trade_no),
       nvl(new.out_trade_no, old.out_trade_no),
       nvl(new.payment_type, old.payment_type),
       nvl(new.payment_amount, old.payment_amount),
       nvl(new.payment_status, old.payment_status),
       nvl(new.create_time, old.create_time),
       nvl(new.callback_time, old.callback_time),
       nvl(date_format(nvl(new.callback_time, old.callback_time), 'yyyy-MM-dd'), '9999-99-99')
from (
         select id,
                order_id,
                user_id,
                province_id,
                trade_no,
                out_trade_no,
                payment_type,
                payment_amount,
                payment_status,
                create_time,
                callback_time
         from dwd_payment_info
         where dt = '9999-99-99'
     ) old
         full outer join
     (
         select pi.id,
                pi.out_trade_no,
                pi.order_id,
                pi.user_id,
                oi.province_id,
                pi.payment_type,
                pi.trade_no,
                pi.payment_amount,
                pi.payment_status,
                pi.create_time,
                pi.callback_time
         from (
                  select *
                  from ods_payment_info
                  where dt = '2021-10-14'
              ) pi
                  left join
              (
                  select id, province_id
                  from ods_order_info
                  where dt = '2021-10-14'
              ) oi
              on pi.order_id = oi.id
     ) new
     on old.id = new.id;



