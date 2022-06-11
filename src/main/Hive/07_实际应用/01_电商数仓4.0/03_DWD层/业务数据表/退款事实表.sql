1.2.8 退款事实表（累积型快照事实表）
每行数据代表一次退款记录 ，相关维度有时间、用户、地区、商品维度，度量值有件数和金额。

--建表语句
DROP TABLE IF EXISTS dwd_refund_payment;
CREATE EXTERNAL TABLE dwd_refund_payment
(
    `id`            STRING COMMENT '编号',
    `user_id`       STRING COMMENT '用户ID',
    `order_id`      STRING COMMENT '订单编号',
    `sku_id`        STRING COMMENT 'SKU编号',
    `province_id`   STRING COMMENT '地区ID',
    --以上是维度外键，包括用户、地区、商品维度
    `trade_no`      STRING COMMENT '交易编号',
    `out_trade_no`  STRING COMMENT '对外交易编号',
    `payment_type`  STRING COMMENT '支付类型',
    `refund_amount` DECIMAL(16, 2) COMMENT '退款金额',
    `refund_status` STRING COMMENT '退款状态',
    --以上除了退款金额是度量值外，其他均是维度字段
    `create_time`   STRING COMMENT '创建时间',--调用第三方支付接口的时间
    `callback_time` STRING COMMENT '回调时间'--支付接口回调时间，即支付成功时间
    --以上两个字段均是时间维度外键
) COMMENT '退款事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_refund_payment/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

---分区情况及数据走向和数据装载
分区规划类似拉链表，有一个dt=9999-99-99分区用于存放未完成状态的数据，也就是创建但未回调（退款）的数据，
其他分区为dt=2021-10-13这种具体时间，其中存放数据为已经完成的数据，即创建并退款的数据，是不再发生变化的数据，
将可能发生变化的数据和不可能发生变化的数据放在两个不同的分区，方便进行数据的更新操作。

--首日装载
insert overwrite table dwd_refund_payment partition (dt)
select rp.id,
       user_id,
       order_id,
       sku_id,
       province_id,
       trade_no,
       out_trade_no,
       payment_type,
       refund_amount,
       refund_status,
       create_time,
       callback_time,
       nvl(date_format(callback_time, 'yyyy-MM-dd'), '9999-99-99')
from (
         select id,
                out_trade_no,
                order_id,
                sku_id,
                payment_type,
                trade_no,
                refund_amount,
                refund_status,
                create_time,
                callback_time
         from ods_refund_payment
         where dt = '2021-10-13'
     ) rp
         left join
     (
         select id,
                user_id,
                province_id
         from ods_order_info
         where dt = '2021-10-13'
     ) oi
     on rp.order_id = oi.id;


--每日装载
insert overwrite table dwd_refund_payment partition (dt)
select nvl(new.id, old.id),
       nvl(new.user_id, old.user_id),
       nvl(new.order_id, old.order_id),
       nvl(new.sku_id, old.sku_id),
       nvl(new.province_id, old.province_id),
       nvl(new.trade_no, old.trade_no),
       nvl(new.out_trade_no, old.out_trade_no),
       nvl(new.payment_type, old.payment_type),
       nvl(new.refund_amount, old.refund_amount),
       nvl(new.refund_status, old.refund_status),
       nvl(new.create_time, old.create_time),
       nvl(new.callback_time, old.callback_time),
       nvl(date_format(nvl(new.callback_time, old.callback_time), 'yyyy-MM-dd'), '9999-99-99')
from (
         select id,
                user_id,
                order_id,
                sku_id,
                province_id,
                trade_no,
                out_trade_no,
                payment_type,
                refund_amount,
                refund_status,
                create_time,
                callback_time
         from dwd_refund_payment
         where dt = '9999-99-99'
     ) old
         full outer join
     (
         select rp.id,
                user_id,
                order_id,
                sku_id,
                province_id,
                trade_no,
                out_trade_no,
                payment_type,
                refund_amount,
                refund_status,
                create_time,
                callback_time
         from (
                  select id,
                         out_trade_no,
                         order_id,
                         sku_id,
                         payment_type,
                         trade_no,
                         refund_amount,
                         refund_status,
                         create_time,
                         callback_time
                  from ods_refund_payment
                  where dt = '2021-10-14'
              ) rp
                  left join
              (
                  select id,
                         user_id,
                         province_id
                  from ods_order_info
                  where dt = '2021-10-14'
              ) oi
              on rp.order_id = oi.id
     ) new
     on old.id = new.id;


