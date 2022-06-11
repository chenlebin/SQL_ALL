--建表语句
DROP TABLE IF EXISTS dwt_coupon_topic;
CREATE EXTERNAL TABLE dwt_coupon_topic
(
    `coupon_id`                      STRING COMMENT '优惠券ID',
    `get_last_1d_count`              BIGINT COMMENT '最近1日领取次数',
    `get_last_7d_count`              BIGINT COMMENT '最近7日领取次数',
    `get_last_30d_count`             BIGINT COMMENT '最近30日领取次数',
    `get_count`                      BIGINT COMMENT '累积领取次数',
    `order_last_1d_count`            BIGINT COMMENT '最近1日使用某券下单次数',
    `order_last_1d_reduce_amount`    DECIMAL(16, 2) COMMENT '最近1日使用某券下单优惠金额',
    `order_last_1d_original_amount`  DECIMAL(16, 2) COMMENT '最近1日使用某券下单原始金额',
    `order_last_1d_final_amount`     DECIMAL(16, 2) COMMENT '最近1日使用某券下单最终金额',
    `order_last_7d_count`            BIGINT COMMENT '最近7日使用某券下单次数',
    `order_last_7d_reduce_amount`    DECIMAL(16, 2) COMMENT '最近7日使用某券下单优惠金额',
    `order_last_7d_original_amount`  DECIMAL(16, 2) COMMENT '最近7日使用某券下单原始金额',
    `order_last_7d_final_amount`     DECIMAL(16, 2) COMMENT '最近7日使用某券下单最终金额',
    `order_last_30d_count`           BIGINT COMMENT '最近30日使用某券下单次数',
    `order_last_30d_reduce_amount`   DECIMAL(16, 2) COMMENT '最近30日使用某券下单优惠金额',
    `order_last_30d_original_amount` DECIMAL(16, 2) COMMENT '最近30日使用某券下单原始金额',
    `order_last_30d_final_amount`    DECIMAL(16, 2) COMMENT '最近30日使用某券下单最终金额',
    `order_count`                    BIGINT COMMENT '累积使用(下单)次数',
    `order_reduce_amount`            DECIMAL(16, 2) COMMENT '使用某券累积下单优惠金额',
    `order_original_amount`          DECIMAL(16, 2) COMMENT '使用某券累积下单原始金额',
    `order_final_amount`             DECIMAL(16, 2) COMMENT '使用某券累积下单最终金额',
    `payment_last_1d_count`          BIGINT COMMENT '最近1日使用某券支付次数',
    `payment_last_1d_reduce_amount`  DECIMAL(16, 2) COMMENT '最近1日使用某券优惠金额',
    `payment_last_1d_amount`         DECIMAL(16, 2) COMMENT '最近1日使用某券支付金额',
    `payment_last_7d_count`          BIGINT COMMENT '最近7日使用某券支付次数',
    `payment_last_7d_reduce_amount`  DECIMAL(16, 2) COMMENT '最近7日使用某券优惠金额',
    `payment_last_7d_amount`         DECIMAL(16, 2) COMMENT '最近7日使用某券支付金额',
    `payment_last_30d_count`         BIGINT COMMENT '最近30日使用某券支付次数',
    `payment_last_30d_reduce_amount` DECIMAL(16, 2) COMMENT '最近30日使用某券优惠金额',
    `payment_last_30d_amount`        DECIMAL(16, 2) COMMENT '最近30日使用某券支付金额',
    `payment_count`                  BIGINT COMMENT '累积使用(支付)次数',
    `payment_reduce_amount`          DECIMAL(16, 2) COMMENT '使用某券累积优惠金额',
    `payment_amount`                 DECIMAL(16, 2) COMMENT '使用某券累积支付金额',
    `expire_last_1d_count`           BIGINT COMMENT '最近1日过期次数',
    `expire_last_7d_count`           BIGINT COMMENT '最近7日过期次数',
    `expire_last_30d_count`          BIGINT COMMENT '最近30日过期次数',
    `expire_count`                   BIGINT COMMENT '累积过期次数'
) comment '优惠券主题表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwt/dwt_coupon_topic/'
    TBLPROPERTIES ("parquet.compression" = "lzo");



----数据装载
--首日装载
insert overwrite table dwt_coupon_topic partition (dt = '2021-10-13')
select id,
       nvl(get_last_1d_count, 0),
       nvl(get_last_7d_count, 0),
       nvl(get_last_30d_count, 0),
       nvl(get_count, 0),
       nvl(order_last_1d_count, 0),
       nvl(order_last_1d_reduce_amount, 0),
       nvl(order_last_1d_original_amount, 0),
       nvl(order_last_1d_final_amount, 0),
       nvl(order_last_7d_count, 0),
       nvl(order_last_7d_reduce_amount, 0),
       nvl(order_last_7d_original_amount, 0),
       nvl(order_last_7d_final_amount, 0),
       nvl(order_last_30d_count, 0),
       nvl(order_last_30d_reduce_amount, 0),
       nvl(order_last_30d_original_amount, 0),
       nvl(order_last_30d_final_amount, 0),
       nvl(order_count, 0),
       nvl(order_reduce_amount, 0),
       nvl(order_original_amount, 0),
       nvl(order_final_amount, 0),
       nvl(payment_last_1d_count, 0),
       nvl(payment_last_1d_reduce_amount, 0),
       nvl(payment_last_1d_amount, 0),
       nvl(payment_last_7d_count, 0),
       nvl(payment_last_7d_reduce_amount, 0),
       nvl(payment_last_7d_amount, 0),
       nvl(payment_last_30d_count, 0),
       nvl(payment_last_30d_reduce_amount, 0),
       nvl(payment_last_30d_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_reduce_amount, 0),
       nvl(payment_amount, 0),
       nvl(expire_last_1d_count, 0),
       nvl(expire_last_7d_count, 0),
       nvl(expire_last_30d_count, 0),
       nvl(expire_count, 0)
from (
         select id
         from dim_coupon_info
         where dt = '2021-10-13'
     ) t1
         left join
     (
         select coupon_id                                                            coupon_id,
                sum(if(dt = '2021-10-13', get_count, 0))                             get_last_1d_count,
                sum(if(dt >= date_add('2021-10-13', -6), get_count, 0))              get_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -29), get_count, 0))             get_last_30d_count,
                sum(get_count)                                                       get_count,
                sum(if(dt = '2021-10-13', order_count, 0))                           order_last_1d_count,
                sum(if(dt = '2021-10-13', order_reduce_amount, 0))                   order_last_1d_reduce_amount,
                sum(if(dt = '2021-10-13', order_original_amount, 0))                 order_last_1d_original_amount,
                sum(if(dt = '2021-10-13', order_final_amount, 0))                    order_last_1d_final_amount,
                sum(if(dt >= date_add('2021-10-13', -6), order_count, 0))            order_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), order_reduce_amount, 0))    order_last_7d_reduce_amount,
                sum(if(dt >= date_add('2021-10-13', -6), order_original_amount, 0))  order_last_7d_original_amount,
                sum(if(dt >= date_add('2021-10-13', -6), order_final_amount, 0))     order_last_7d_final_amount,
                sum(if(dt >= date_add('2021-10-13', -29), order_count, 0))           order_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), order_reduce_amount, 0))   order_last_30d_reduce_amount,
                sum(if(dt >= date_add('2021-10-13', -29), order_original_amount, 0)) order_last_30d_original_amount,
                sum(if(dt >= date_add('2021-10-13', -29), order_final_amount, 0))    order_last_30d_final_amount,
                sum(order_count)                                                     order_count,
                sum(order_reduce_amount)                                             order_reduce_amount,
                sum(order_original_amount)                                           order_original_amount,
                sum(order_final_amount)                                              order_final_amount,
                sum(if(dt = '2021-10-13', payment_count, 0))                         payment_last_1d_count,
                sum(if(dt = '2021-10-13', payment_reduce_amount, 0))                 payment_last_1d_reduce_amount,
                sum(if(dt = '2021-10-13', payment_amount, 0))                        payment_last_1d_amount,
                sum(if(dt >= date_add('2021-10-13', -6), payment_count, 0))          payment_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), payment_reduce_amount, 0))  payment_last_7d_reduce_amount,
                sum(if(dt >= date_add('2021-10-13', -6), payment_amount, 0))         payment_last_7d_amount,
                sum(if(dt >= date_add('2021-10-13', -29), payment_count, 0))         payment_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), payment_reduce_amount, 0)) payment_last_30d_reduce_amount,
                sum(if(dt >= date_add('2021-10-13', -29), payment_amount, 0))        payment_last_30d_amount,
                sum(payment_count)                                                   payment_count,
                sum(payment_reduce_amount)                                           payment_reduce_amount,
                sum(payment_amount)                                                  payment_amount,
                sum(if(dt = '2021-10-13', expire_count, 0))                          expire_last_1d_count,
                sum(if(dt >= date_add('2021-10-13', -6), expire_count, 0))           expire_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -29), expire_count, 0))          expire_last_30d_count,
                sum(expire_count)                                                    expire_count
         from dws_coupon_info_daycount
         group by coupon_id
     ) t2
     on t1.id = t2.coupon_id;


--每日装载
insert overwrite table dwt_coupon_topic partition (dt = '2021-10-14')
select nvl(1d_ago.coupon_id, old.coupon_id),
       nvl(1d_ago.get_count, 0),
       nvl(old.get_last_7d_count, 0) + nvl(1d_ago.get_count, 0) - nvl(7d_ago.get_count, 0),
       nvl(old.get_last_30d_count, 0) + nvl(1d_ago.get_count, 0) - nvl(30d_ago.get_count, 0),
       nvl(old.get_count, 0) + nvl(1d_ago.get_count, 0),
       nvl(1d_ago.order_count, 0),
       nvl(1d_ago.order_reduce_amount, 0.0),
       nvl(1d_ago.order_original_amount, 0.0),
       nvl(1d_ago.order_final_amount, 0.0),
       nvl(old.order_last_7d_count, 0) + nvl(1d_ago.order_count, 0) - nvl(7d_ago.order_count, 0),
       nvl(old.order_last_7d_reduce_amount, 0.0) + nvl(1d_ago.order_reduce_amount, 0.0) -
       nvl(7d_ago.order_reduce_amount, 0.0),
       nvl(old.order_last_7d_original_amount, 0.0) + nvl(1d_ago.order_original_amount, 0.0) -
       nvl(7d_ago.order_original_amount, 0.0),
       nvl(old.order_last_7d_final_amount, 0.0) + nvl(1d_ago.order_final_amount, 0.0) -
       nvl(7d_ago.order_final_amount, 0.0),
       nvl(old.order_last_30d_count, 0) + nvl(1d_ago.order_count, 0) - nvl(30d_ago.order_count, 0),
       nvl(old.order_last_30d_reduce_amount, 0.0) + nvl(1d_ago.order_reduce_amount, 0.0) -
       nvl(30d_ago.order_reduce_amount, 0.0),
       nvl(old.order_last_30d_original_amount, 0.0) + nvl(1d_ago.order_original_amount, 0.0) -
       nvl(30d_ago.order_original_amount, 0.0),
       nvl(old.order_last_30d_final_amount, 0.0) + nvl(1d_ago.order_final_amount, 0.0) -
       nvl(30d_ago.order_final_amount, 0.0),
       nvl(old.order_count, 0) + nvl(1d_ago.order_count, 0),
       nvl(old.order_reduce_amount, 0.0) + nvl(1d_ago.order_reduce_amount, 0.0),
       nvl(old.order_original_amount, 0.0) + nvl(1d_ago.order_original_amount, 0.0),
       nvl(old.order_final_amount, 0.0) + nvl(1d_ago.order_final_amount, 0.0),
       nvl(old.payment_last_1d_count, 0) + nvl(1d_ago.payment_count, 0) - nvl(1d_ago.payment_count, 0),
       nvl(old.payment_last_1d_reduce_amount, 0.0) + nvl(1d_ago.payment_reduce_amount, 0.0) -
       nvl(1d_ago.payment_reduce_amount, 0.0),
       nvl(old.payment_last_1d_amount, 0.0) + nvl(1d_ago.payment_amount, 0.0) - nvl(1d_ago.payment_amount, 0.0),
       nvl(old.payment_last_7d_count, 0) + nvl(1d_ago.payment_count, 0) - nvl(7d_ago.payment_count, 0),
       nvl(old.payment_last_7d_reduce_amount, 0.0) + nvl(1d_ago.payment_reduce_amount, 0.0) -
       nvl(7d_ago.payment_reduce_amount, 0.0),
       nvl(old.payment_last_7d_amount, 0.0) + nvl(1d_ago.payment_amount, 0.0) - nvl(7d_ago.payment_amount, 0.0),
       nvl(old.payment_last_30d_count, 0) + nvl(1d_ago.payment_count, 0) - nvl(30d_ago.payment_count, 0),
       nvl(old.payment_last_30d_reduce_amount, 0.0) + nvl(1d_ago.payment_reduce_amount, 0.0) -
       nvl(30d_ago.payment_reduce_amount, 0.0),
       nvl(old.payment_last_30d_amount, 0.0) + nvl(1d_ago.payment_amount, 0.0) - nvl(30d_ago.payment_amount, 0.0),
       nvl(old.payment_count, 0) + nvl(1d_ago.payment_count, 0),
       nvl(old.payment_reduce_amount, 0.0) + nvl(1d_ago.payment_reduce_amount, 0.0),
       nvl(old.payment_amount, 0.0) + nvl(1d_ago.payment_amount, 0.0),
       nvl(1d_ago.expire_count, 0),
       nvl(old.expire_last_7d_count, 0) + nvl(1d_ago.expire_count, 0) - nvl(7d_ago.expire_count, 0),
       nvl(old.expire_last_30d_count, 0) + nvl(1d_ago.expire_count, 0) - nvl(30d_ago.expire_count, 0),
       nvl(old.expire_count, 0) + nvl(1d_ago.expire_count, 0)
from (
         select coupon_id,
                get_last_1d_count,
                get_last_7d_count,
                get_last_30d_count,
                get_count,
                order_last_1d_count,
                order_last_1d_reduce_amount,
                order_last_1d_original_amount,
                order_last_1d_final_amount,
                order_last_7d_count,
                order_last_7d_reduce_amount,
                order_last_7d_original_amount,
                order_last_7d_final_amount,
                order_last_30d_count,
                order_last_30d_reduce_amount,
                order_last_30d_original_amount,
                order_last_30d_final_amount,
                order_count,
                order_reduce_amount,
                order_original_amount,
                order_final_amount,
                payment_last_1d_count,
                payment_last_1d_reduce_amount,
                payment_last_1d_amount,
                payment_last_7d_count,
                payment_last_7d_reduce_amount,
                payment_last_7d_amount,
                payment_last_30d_count,
                payment_last_30d_reduce_amount,
                payment_last_30d_amount,
                payment_count,
                payment_reduce_amount,
                payment_amount,
                expire_last_1d_count,
                expire_last_7d_count,
                expire_last_30d_count,
                expire_count
         from dwt_coupon_topic
         where dt = date_add('2021-10-14', -1)
     ) old
         full outer join
     (
         select coupon_id,
                get_count,
                order_count,
                order_reduce_amount,
                order_original_amount,
                order_final_amount,
                payment_count,
                payment_reduce_amount,
                payment_amount,
                expire_count
         from dws_coupon_info_daycount
         where dt = '2021-10-14'
     ) 1d_ago
     on old.coupon_id = 1d_ago.coupon_id
         left join
     (
         select coupon_id,
                get_count,
                order_count,
                order_reduce_amount,
                order_original_amount,
                order_final_amount,
                payment_count,
                payment_reduce_amount,
                payment_amount,
                expire_count
         from dws_coupon_info_daycount
         where dt = date_add('2021-10-14', -7)
     ) 7d_ago
     on old.coupon_id = 7d_ago.coupon_id
         left join
     (
         select coupon_id,
                get_count,
                order_count,
                order_reduce_amount,
                order_original_amount,
                order_final_amount,
                payment_count,
                payment_reduce_amount,
                payment_amount,
                expire_count
         from dws_coupon_info_daycount
         where dt = date_add('2021-10-14', -30)
     ) 30d_ago
     on old.coupon_id = 30d_ago.coupon_id;
