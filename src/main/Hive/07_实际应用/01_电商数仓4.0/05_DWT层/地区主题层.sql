--建表语句
DROP TABLE IF EXISTS dwt_area_topic;
CREATE EXTERNAL TABLE dwt_area_topic
(
    `province_id`                    STRING COMMENT '编号',
    `visit_last_1d_count`            BIGINT COMMENT '最近1日访客访问次数',
    `login_last_1d_count`            BIGINT COMMENT '最近1日用户访问次数',
    `visit_last_7d_count`            BIGINT COMMENT '最近7访客访问次数',
    `login_last_7d_count`            BIGINT COMMENT '最近7日用户访问次数',
    `visit_last_30d_count`           BIGINT COMMENT '最近30日访客访问次数',
    `login_last_30d_count`           BIGINT COMMENT '最近30日用户访问次数',
    `visit_count`                    BIGINT COMMENT '累积访客访问次数',
    `login_count`                    BIGINT COMMENT '累积用户访问次数',
    `order_last_1d_count`            BIGINT COMMENT '最近1天下单次数',
    `order_last_1d_original_amount`  DECIMAL(16, 2) COMMENT '最近1天下单原始金额',
    `order_last_1d_final_amount`     DECIMAL(16, 2) COMMENT '最近1天下单最终金额',
    `order_last_7d_count`            BIGINT COMMENT '最近7天下单次数',
    `order_last_7d_original_amount`  DECIMAL(16, 2) COMMENT '最近7天下单原始金额',
    `order_last_7d_final_amount`     DECIMAL(16, 2) COMMENT '最近7天下单最终金额',
    `order_last_30d_count`           BIGINT COMMENT '最近30天下单次数',
    `order_last_30d_original_amount` DECIMAL(16, 2) COMMENT '最近30天下单原始金额',
    `order_last_30d_final_amount`    DECIMAL(16, 2) COMMENT '最近30天下单最终金额',
    `order_count`                    BIGINT COMMENT '累积下单次数',
    `order_original_amount`          DECIMAL(16, 2) COMMENT '累积下单原始金额',
    `order_final_amount`             DECIMAL(16, 2) COMMENT '累积下单最终金额',
    `payment_last_1d_count`          BIGINT COMMENT '最近1天支付次数',
    `payment_last_1d_amount`         DECIMAL(16, 2) COMMENT '最近1天支付金额',
    `payment_last_7d_count`          BIGINT COMMENT '最近7天支付次数',
    `payment_last_7d_amount`         DECIMAL(16, 2) COMMENT '最近7天支付金额',
    `payment_last_30d_count`         BIGINT COMMENT '最近30天支付次数',
    `payment_last_30d_amount`        DECIMAL(16, 2) COMMENT '最近30天支付金额',
    `payment_count`                  BIGINT COMMENT '累积支付次数',
    `payment_amount`                 DECIMAL(16, 2) COMMENT '累积支付金额',
    `refund_order_last_1d_count`     BIGINT COMMENT '最近1天退单次数',
    `refund_order_last_1d_amount`    DECIMAL(16, 2) COMMENT '最近1天退单金额',
    `refund_order_last_7d_count`     BIGINT COMMENT '最近7天退单次数',
    `refund_order_last_7d_amount`    DECIMAL(16, 2) COMMENT '最近7天退单金额',
    `refund_order_last_30d_count`    BIGINT COMMENT '最近30天退单次数',
    `refund_order_last_30d_amount`   DECIMAL(16, 2) COMMENT '最近30天退单金额',
    `refund_order_count`             BIGINT COMMENT '累积退单次数',
    `refund_order_amount`            DECIMAL(16, 2) COMMENT '累积退单金额',
    `refund_payment_last_1d_count`   BIGINT COMMENT '最近1天退款次数',
    `refund_payment_last_1d_amount`  DECIMAL(16, 2) COMMENT '最近1天退款金额',
    `refund_payment_last_7d_count`   BIGINT COMMENT '最近7天退款次数',
    `refund_payment_last_7d_amount`  DECIMAL(16, 2) COMMENT '最近7天退款金额',
    `refund_payment_last_30d_count`  BIGINT COMMENT '最近30天退款次数',
    `refund_payment_last_30d_amount` DECIMAL(16, 2) COMMENT '最近30天退款金额',
    `refund_payment_count`           BIGINT COMMENT '累积退款次数',
    `refund_payment_amount`          DECIMAL(16, 2) COMMENT '累积退款金额'
) COMMENT '地区主题宽表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwt/dwt_area_topic/'
    TBLPROPERTIES ("parquet.compression" = "lzo");



----数据装载
--首日装载
insert overwrite table dwt_area_topic partition (dt = '2021-10-13')
select id,
       nvl(visit_last_1d_count, 0),
       nvl(login_last_1d_count, 0),
       nvl(visit_last_7d_count, 0),
       nvl(login_last_7d_count, 0),
       nvl(visit_last_30d_count, 0),
       nvl(login_last_30d_count, 0),
       nvl(visit_count, 0),
       nvl(login_count, 0),
       nvl(order_last_1d_count, 0),
       nvl(order_last_1d_original_amount, 0),
       nvl(order_last_1d_final_amount, 0),
       nvl(order_last_7d_count, 0),
       nvl(order_last_7d_original_amount, 0),
       nvl(order_last_7d_final_amount, 0),
       nvl(order_last_30d_count, 0),
       nvl(order_last_30d_original_amount, 0),
       nvl(order_last_30d_final_amount, 0),
       nvl(order_count, 0),
       nvl(order_original_amount, 0),
       nvl(order_final_amount, 0),
       nvl(payment_last_1d_count, 0),
       nvl(payment_last_1d_amount, 0),
       nvl(payment_last_7d_count, 0),
       nvl(payment_last_7d_amount, 0),
       nvl(payment_last_30d_count, 0),
       nvl(payment_last_30d_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_amount, 0),
       nvl(refund_order_last_1d_count, 0),
       nvl(refund_order_last_1d_amount, 0),
       nvl(refund_order_last_7d_count, 0),
       nvl(refund_order_last_7d_amount, 0),
       nvl(refund_order_last_30d_count, 0),
       nvl(refund_order_last_30d_amount, 0),
       nvl(refund_order_count, 0),
       nvl(refund_order_amount, 0),
       nvl(refund_payment_last_1d_count, 0),
       nvl(refund_payment_last_1d_amount, 0),
       nvl(refund_payment_last_7d_count, 0),
       nvl(refund_payment_last_7d_amount, 0),
       nvl(refund_payment_last_30d_count, 0),
       nvl(refund_payment_last_30d_amount, 0),
       nvl(refund_payment_count, 0),
       nvl(refund_payment_amount, 0)
from (
         select id
         from dim_base_province
     ) t1
         left join
     (
         select province_id                                                          province_id,
                sum(if(dt = '2021-10-13', visit_count, 0))                           visit_last_1d_count,
                sum(if(dt = '2021-10-13', login_count, 0))                           login_last_1d_count,
                sum(if(dt >= date_add('2021-10-13', -6), visit_count, 0))            visit_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), login_count, 0))            login_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -29), visit_count, 0))           visit_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), login_count, 0))           login_last_30d_count,
                sum(visit_count)                                                     visit_count,
                sum(login_count)                                                     login_count,
                sum(if(dt = '2021-10-13', order_count, 0))                           order_last_1d_count,
                sum(if(dt = '2021-10-13', order_original_amount, 0))                 order_last_1d_original_amount,
                sum(if(dt = '2021-10-13', order_final_amount, 0))                    order_last_1d_final_amount,
                sum(if(dt >= date_add('2021-10-13', -6), order_count, 0))            order_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), order_original_amount, 0))  order_last_7d_original_amount,
                sum(if(dt >= date_add('2021-10-13', -6), order_final_amount, 0))     order_last_7d_final_amount,
                sum(if(dt >= date_add('2021-10-13', -29), order_count, 0))           order_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), order_original_amount, 0)) order_last_30d_original_amount,
                sum(if(dt >= date_add('2021-10-13', -29), order_final_amount, 0))    order_last_30d_final_amount,
                sum(order_count)                                                     order_count,
                sum(order_original_amount)                                           order_original_amount,
                sum(order_final_amount)                                              order_final_amount,
                sum(if(dt = '2021-10-13', payment_count, 0))                         payment_last_1d_count,
                sum(if(dt = '2021-10-13', payment_amount, 0))                        payment_last_1d_amount,
                sum(if(dt >= date_add('2021-10-13', -6), payment_count, 0))          payment_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), payment_amount, 0))         payment_last_7d_amount,
                sum(if(dt >= date_add('2021-10-13', -29), payment_count, 0))         payment_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), payment_amount, 0))        payment_last_30d_amount,
                sum(payment_count)                                                   payment_count,
                sum(payment_amount)                                                  payment_amount,
                sum(if(dt = '2021-10-13', refund_order_count, 0))                    refund_order_last_1d_count,
                sum(if(dt = '2021-10-13', refund_order_amount, 0))                   refund_order_last_1d_amount,
                sum(if(dt >= date_add('2021-10-13', -6), refund_order_count, 0))     refund_order_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), refund_order_amount, 0))    refund_order_last_7d_amount,
                sum(if(dt >= date_add('2021-10-13', -29), refund_order_count, 0))    refund_order_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), refund_order_amount, 0))   refund_order_last_30d_amount,
                sum(refund_order_count)                                              refund_order_count,
                sum(refund_order_amount)                                             refund_order_amount,
                sum(if(dt = '2021-10-13', refund_payment_count, 0))                  refund_payment_last_1d_count,
                sum(if(dt = '2021-10-13', refund_payment_amount, 0))                 refund_payment_last_1d_amount,
                sum(if(dt >= date_add('2021-10-13', -6), refund_payment_count, 0))   refund_payment_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), refund_payment_amount, 0))  refund_payment_last_7d_amount,
                sum(if(dt >= date_add('2021-10-13', -29), refund_payment_count, 0))  refund_payment_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), refund_payment_amount, 0)) refund_payment_last_30d_amount,
                sum(refund_payment_count)                                            refund_payment_count,
                sum(refund_payment_amount)                                           refund_payment_amount
         from dws_area_stats_daycount
         group by province_id
     ) t2
     on t1.id = t2.province_id;


--每日装载
insert overwrite table dwt_area_topic partition (dt = '2021-10-14')
select nvl(old.province_id, 1d_ago.province_id),
       nvl(1d_ago.visit_count, 0),
       nvl(1d_ago.login_count, 0),
       nvl(old.visit_last_7d_count, 0) + nvl(1d_ago.visit_count, 0) - nvl(7d_ago.visit_count, 0),
       nvl(old.login_last_7d_count, 0) + nvl(1d_ago.login_count, 0) - nvl(7d_ago.login_count, 0),
       nvl(old.visit_last_30d_count, 0) + nvl(1d_ago.visit_count, 0) - nvl(30d_ago.visit_count, 0),
       nvl(old.login_last_30d_count, 0) + nvl(1d_ago.login_count, 0) - nvl(30d_ago.login_count, 0),
       nvl(old.visit_count, 0) + nvl(1d_ago.visit_count, 0),
       nvl(old.login_count, 0) + nvl(1d_ago.login_count, 0),
       nvl(1d_ago.order_count, 0),
       nvl(1d_ago.order_original_amount, 0.0),
       nvl(1d_ago.order_final_amount, 0.0),
       nvl(old.order_last_7d_count, 0) + nvl(1d_ago.order_count, 0) - nvl(7d_ago.order_count, 0),
       nvl(old.order_last_7d_original_amount, 0.0) + nvl(1d_ago.order_original_amount, 0.0) -
       nvl(7d_ago.order_original_amount, 0.0),
       nvl(old.order_last_7d_final_amount, 0.0) + nvl(1d_ago.order_final_amount, 0.0) -
       nvl(7d_ago.order_final_amount, 0.0),
       nvl(old.order_last_30d_count, 0) + nvl(1d_ago.order_count, 0) - nvl(30d_ago.order_count, 0),
       nvl(old.order_last_30d_original_amount, 0.0) + nvl(1d_ago.order_original_amount, 0.0) -
       nvl(30d_ago.order_original_amount, 0.0),
       nvl(old.order_last_30d_final_amount, 0.0) + nvl(1d_ago.order_final_amount, 0.0) -
       nvl(30d_ago.order_final_amount, 0.0),
       nvl(old.order_count, 0) + nvl(1d_ago.order_count, 0),
       nvl(old.order_original_amount, 0.0) + nvl(1d_ago.order_original_amount, 0.0),
       nvl(old.order_final_amount, 0.0) + nvl(1d_ago.order_final_amount, 0.0),
       nvl(1d_ago.payment_count, 0),
       nvl(1d_ago.payment_amount, 0.0),
       nvl(old.payment_last_7d_count, 0) + nvl(1d_ago.payment_count, 0) - nvl(7d_ago.payment_count, 0),
       nvl(old.payment_last_7d_amount, 0.0) + nvl(1d_ago.payment_amount, 0.0) - nvl(7d_ago.payment_amount, 0.0),
       nvl(old.payment_last_30d_count, 0) + nvl(1d_ago.payment_count, 0) - nvl(30d_ago.payment_count, 0),
       nvl(old.payment_last_30d_amount, 0.0) + nvl(1d_ago.payment_amount, 0.0) - nvl(30d_ago.payment_amount, 0.0),
       nvl(old.payment_count, 0) + nvl(1d_ago.payment_count, 0),
       nvl(old.payment_amount, 0.0) + nvl(1d_ago.payment_amount, 0.0),
       nvl(1d_ago.refund_order_count, 0),
       nvl(1d_ago.refund_order_amount, 0.0),
       nvl(old.refund_order_last_7d_count, 0) + nvl(1d_ago.refund_order_count, 0) - nvl(7d_ago.refund_order_count, 0),
       nvl(old.refund_order_last_7d_amount, 0.0) + nvl(1d_ago.refund_order_amount, 0.0) -
       nvl(7d_ago.refund_order_amount, 0.0),
       nvl(old.refund_order_last_30d_count, 0) + nvl(1d_ago.refund_order_count, 0) - nvl(30d_ago.refund_order_count, 0),
       nvl(old.refund_order_last_30d_amount, 0.0) + nvl(1d_ago.refund_order_amount, 0.0) -
       nvl(30d_ago.refund_order_amount, 0.0),
       nvl(old.refund_order_count, 0) + nvl(1d_ago.refund_order_count, 0),
       nvl(old.refund_order_amount, 0.0) + nvl(1d_ago.refund_order_amount, 0.0),
       nvl(1d_ago.refund_payment_count, 0),
       nvl(1d_ago.refund_payment_amount, 0.0),
       nvl(old.refund_payment_last_7d_count, 0) + nvl(1d_ago.refund_payment_count, 0) -
       nvl(7d_ago.refund_payment_count, 0),
       nvl(old.refund_payment_last_7d_amount, 0.0) + nvl(1d_ago.refund_payment_amount, 0.0) -
       nvl(7d_ago.refund_payment_amount, 0.0),
       nvl(old.refund_payment_last_30d_count, 0) + nvl(1d_ago.refund_payment_count, 0) -
       nvl(30d_ago.refund_payment_count, 0),
       nvl(old.refund_payment_last_30d_amount, 0.0) + nvl(1d_ago.refund_payment_amount, 0.0) -
       nvl(30d_ago.refund_payment_amount, 0.0),
       nvl(old.refund_payment_count, 0) + nvl(1d_ago.refund_payment_count, 0),
       nvl(old.refund_payment_amount, 0.0) + nvl(1d_ago.refund_payment_amount, 0.0)

from (
         select province_id,
                visit_last_1d_count,
                login_last_1d_count,
                visit_last_7d_count,
                login_last_7d_count,
                visit_last_30d_count,
                login_last_30d_count,
                visit_count,
                login_count,
                order_last_1d_count,
                order_last_1d_original_amount,
                order_last_1d_final_amount,
                order_last_7d_count,
                order_last_7d_original_amount,
                order_last_7d_final_amount,
                order_last_30d_count,
                order_last_30d_original_amount,
                order_last_30d_final_amount,
                order_count,
                order_original_amount,
                order_final_amount,
                payment_last_1d_count,
                payment_last_1d_amount,
                payment_last_7d_count,
                payment_last_7d_amount,
                payment_last_30d_count,
                payment_last_30d_amount,
                payment_count,
                payment_amount,
                refund_order_last_1d_count,
                refund_order_last_1d_amount,
                refund_order_last_7d_count,
                refund_order_last_7d_amount,
                refund_order_last_30d_count,
                refund_order_last_30d_amount,
                refund_order_count,
                refund_order_amount,
                refund_payment_last_1d_count,
                refund_payment_last_1d_amount,
                refund_payment_last_7d_count,
                refund_payment_last_7d_amount,
                refund_payment_last_30d_count,
                refund_payment_last_30d_amount,
                refund_payment_count,
                refund_payment_amount
         from dwt_area_topic
         where dt = date_add('2021-10-14', -1)
     ) old
         full outer join
     (
         select province_id,
                visit_count,
                login_count,
                order_count,
                order_original_amount,
                order_final_amount,
                payment_count,
                payment_amount,
                refund_order_count,
                refund_order_amount,
                refund_payment_count,
                refund_payment_amount
         from dws_area_stats_daycount
         where dt = '2021-10-14'
     ) 1d_ago
     on old.province_id = 1d_ago.province_id
         left join
     (
         select province_id,
                visit_count,
                login_count,
                order_count,
                order_original_amount,
                order_final_amount,
                payment_count,
                payment_amount,
                refund_order_count,
                refund_order_amount,
                refund_payment_count,
                refund_payment_amount
         from dws_area_stats_daycount
         where dt = date_add('2021-10-14', -7)
     ) 7d_ago
     on old.province_id = 7d_ago.province_id
         left join
     (
         select province_id,
                visit_count,
                login_count,
                order_count,
                order_original_amount,
                order_final_amount,
                payment_count,
                payment_amount,
                refund_order_count,
                refund_order_amount,
                refund_payment_count,
                refund_payment_amount
         from dws_area_stats_daycount
         where dt = date_add('2021-10-14', -30)
     ) 30d_ago
     on old.province_id = 30d_ago.province_id;
