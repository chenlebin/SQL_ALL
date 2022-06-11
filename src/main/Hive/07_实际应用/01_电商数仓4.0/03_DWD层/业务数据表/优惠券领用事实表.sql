--优惠券领用事实表（累积型快照事实表）
--一行数据代表一次优惠券记录，相关的维度有：时间、用户、优惠券维度，度量值是次数，

DROP TABLE IF EXISTS dwd_coupon_use;
CREATE EXTERNAL TABLE dwd_coupon_use
(
    `id`            STRING COMMENT '编号',
    `coupon_id`     STRING COMMENT '优惠券ID',
    `user_id`       STRING COMMENT 'userid',
    `order_id`      STRING COMMENT '订单id',
    --以上是维度外键
    `coupon_status` STRING COMMENT '优惠券状态',
    --这是维度字段
    `get_time`      STRING COMMENT '领取时间',
    `using_time`    STRING COMMENT '使用时间(下单)',
    `used_time`     STRING COMMENT '使用时间(支付)',
    `expire_time`   STRING COMMENT '过期时间'
    --以上是时间维度的外键，累积型事实表的特点就是有多个时间维度外键
) COMMENT '优惠券领用事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_coupon_use/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


---分区情况及数据走向和数据装载
--分区规划类似拉链表，有一个dt=9999-99-99分区用于存放未使用优惠券数据并且优惠券未过期，其他分区为dt=2021-10-13这种具体时间
--其中存放数据为已经使用了优惠券的数据或者是优惠券已经过期了的数据，是不在发生变化的数据，将可能发生变化的数据和不可能发生变化的数据放
--在两个不同的分区，方便进行数据的更新操作。

----数据装载
--首日装载
insert overwrite table dwd_coupon_use partition (dt)
select id,
       coupon_id,
       user_id,
       order_id,
       coupon_status,
       get_time,
       using_time,
       used_time,
       expire_time,
       coalesce(date_format(used_time, 'yyyy-MM-dd'), date_format(expire_time, 'yyyy-MM-dd'), '9999-99-99')
       --这句话代表，如果used_time不为null则说明已经使用了为已经完成了的数据放在已完成的分区中，
       --如果used_time为null但expire_time不为null则说明优惠券已过期，也是已经完成了的数据放在已完成的分区中，
from ods_coupon_use
where dt = '2021-10-13';


--每日装载
insert overwrite table dwd_coupon_use partition (dt)
select nvl(new.id, old.id),
       nvl(new.coupon_id, old.coupon_id),
       nvl(new.user_id, old.user_id),
       nvl(new.order_id, old.order_id),
       nvl(new.coupon_status, old.coupon_status),
       nvl(new.get_time, old.get_time),
       nvl(new.using_time, old.using_time),
       nvl(new.used_time, old.used_time),
       nvl(new.expire_time, old.expire_time),
       --上面为获取最新的数据，如果右边存在说明数据为新增或更新数据，如果左边存在则说明为旧数据未发生变化
       coalesce(date_format(nvl(new.used_time, old.used_time), 'yyyy-MM-dd'),
                date_format(nvl(new.expire_time, old.expire_time), 'yyyy-MM-dd'), '9999-99-99')
       --coalesce相当于nvl的加强版，nvl只能有两个参数用于判断是否为空，为空则取后面的值，coalesce可以有n个参数，但效果和nvl一致。
from (
         select id,
                coupon_id,
                user_id,
                order_id,
                coupon_status,
                get_time,
                using_time,
                used_time,
                expire_time
         from dwd_coupon_use
         where dt = '9999-99-99'
     ) old
         full outer join
     (
         select id,
                coupon_id,
                user_id,
                order_id,
                coupon_status,
                get_time,
                using_time,
                used_time,
                expire_time
         from ods_coupon_use
         where dt = '2021-10-14'
     ) new
     on old.id = new.id;


