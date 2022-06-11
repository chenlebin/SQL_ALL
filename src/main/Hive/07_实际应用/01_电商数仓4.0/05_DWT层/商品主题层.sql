--建表语句
DROP TABLE IF EXISTS dwt_sku_topic;
CREATE EXTERNAL TABLE dwt_sku_topic
(
    `sku_id`                                STRING COMMENT 'sku_id',
    `order_last_1d_count`                   BIGINT COMMENT '最近1日被下单次数',
    `order_last_1d_num`                     BIGINT COMMENT '最近1日被下单件数',
    `order_activity_last_1d_count`          BIGINT COMMENT '最近1日参与活动被下单次数',
    `order_coupon_last_1d_count`            BIGINT COMMENT '最近1日使用优惠券被下单次数',
    `order_activity_reduce_last_1d_amount`  DECIMAL(16, 2) COMMENT '最近1日优惠金额(活动)',
    `order_coupon_reduce_last_1d_amount`    DECIMAL(16, 2) COMMENT '最近1日优惠金额(优惠券)',
    `order_last_1d_original_amount`         DECIMAL(16, 2) COMMENT '最近1日被下单原始金额',
    `order_last_1d_final_amount`            DECIMAL(16, 2) COMMENT '最近1日被下单最终金额',
    `order_last_7d_count`                   BIGINT COMMENT '最近7日被下单次数',
    `order_last_7d_num`                     BIGINT COMMENT '最近7日被下单件数',
    `order_activity_last_7d_count`          BIGINT COMMENT '最近7日参与活动被下单次数',
    `order_coupon_last_7d_count`            BIGINT COMMENT '最近7日使用优惠券被下单次数',
    `order_activity_reduce_last_7d_amount`  DECIMAL(16, 2) COMMENT '最近7日优惠金额(活动)',
    `order_coupon_reduce_last_7d_amount`    DECIMAL(16, 2) COMMENT '最近7日优惠金额(优惠券)',
    `order_last_7d_original_amount`         DECIMAL(16, 2) COMMENT '最近7日被下单原始金额',
    `order_last_7d_final_amount`            DECIMAL(16, 2) COMMENT '最近7日被下单最终金额',
    `order_last_30d_count`                  BIGINT COMMENT '最近30日被下单次数',
    `order_last_30d_num`                    BIGINT COMMENT '最近30日被下单件数',
    `order_activity_last_30d_count`         BIGINT COMMENT '最近30日参与活动被下单次数',
    `order_coupon_last_30d_count`           BIGINT COMMENT '最近30日使用优惠券被下单次数',
    `order_activity_reduce_last_30d_amount` DECIMAL(16, 2) COMMENT '最近30日优惠金额(活动)',
    `order_coupon_reduce_last_30d_amount`   DECIMAL(16, 2) COMMENT '最近30日优惠金额(优惠券)',
    `order_last_30d_original_amount`        DECIMAL(16, 2) COMMENT '最近30日被下单原始金额',
    `order_last_30d_final_amount`           DECIMAL(16, 2) COMMENT '最近30日被下单最终金额',
    `order_count`                           BIGINT COMMENT '累积被下单次数',
    `order_num`                             BIGINT COMMENT '累积被下单件数',
    `order_activity_count`                  BIGINT COMMENT '累积参与活动被下单次数',
    `order_coupon_count`                    BIGINT COMMENT '累积使用优惠券被下单次数',
    `order_activity_reduce_amount`          DECIMAL(16, 2) COMMENT '累积优惠金额(活动)',
    `order_coupon_reduce_amount`            DECIMAL(16, 2) COMMENT '累积优惠金额(优惠券)',
    `order_original_amount`                 DECIMAL(16, 2) COMMENT '累积被下单原始金额',
    `order_final_amount`                    DECIMAL(16, 2) COMMENT '累积被下单最终金额',
    `payment_last_1d_count`                 BIGINT COMMENT '最近1日被支付次数',
    `payment_last_1d_num`                   BIGINT COMMENT '最近1日被支付件数',
    `payment_last_1d_amount`                DECIMAL(16, 2) COMMENT '最近1日被支付金额',
    `payment_last_7d_count`                 BIGINT COMMENT '最近7日被支付次数',
    `payment_last_7d_num`                   BIGINT COMMENT '最近7日被支付件数',
    `payment_last_7d_amount`                DECIMAL(16, 2) COMMENT '最近7日被支付金额',
    `payment_last_30d_count`                BIGINT COMMENT '最近30日被支付次数',
    `payment_last_30d_num`                  BIGINT COMMENT '最近30日被支付件数',
    `payment_last_30d_amount`               DECIMAL(16, 2) COMMENT '最近30日被支付金额',
    `payment_count`                         BIGINT COMMENT '累积被支付次数',
    `payment_num`                           BIGINT COMMENT '累积被支付件数',
    `payment_amount`                        DECIMAL(16, 2) COMMENT '累积被支付金额',
    `refund_order_last_1d_count`            BIGINT COMMENT '最近1日退单次数',
    `refund_order_last_1d_num`              BIGINT COMMENT '最近1日退单件数',
    `refund_order_last_1d_amount`           DECIMAL(16, 2) COMMENT '最近1日退单金额',
    `refund_order_last_7d_count`            BIGINT COMMENT '最近7日退单次数',
    `refund_order_last_7d_num`              BIGINT COMMENT '最近7日退单件数',
    `refund_order_last_7d_amount`           DECIMAL(16, 2) COMMENT '最近7日退单金额',
    `refund_order_last_30d_count`           BIGINT COMMENT '最近30日退单次数',
    `refund_order_last_30d_num`             BIGINT COMMENT '最近30日退单件数',
    `refund_order_last_30d_amount`          DECIMAL(16, 2) COMMENT '最近30日退单金额',
    `refund_order_count`                    BIGINT COMMENT '累积退单次数',
    `refund_order_num`                      BIGINT COMMENT '累积退单件数',
    `refund_order_amount`                   DECIMAL(16, 2) COMMENT '累积退单金额',
    `refund_payment_last_1d_count`          BIGINT COMMENT '最近1日退款次数',
    `refund_payment_last_1d_num`            BIGINT COMMENT '最近1日退款件数',
    `refund_payment_last_1d_amount`         DECIMAL(16, 2) COMMENT '最近1日退款金额',
    `refund_payment_last_7d_count`          BIGINT COMMENT '最近7日退款次数',
    `refund_payment_last_7d_num`            BIGINT COMMENT '最近7日退款件数',
    `refund_payment_last_7d_amount`         DECIMAL(16, 2) COMMENT '最近7日退款金额',
    `refund_payment_last_30d_count`         BIGINT COMMENT '最近30日退款次数',
    `refund_payment_last_30d_num`           BIGINT COMMENT '最近30日退款件数',
    `refund_payment_last_30d_amount`        DECIMAL(16, 2) COMMENT '最近30日退款金额',
    `refund_payment_count`                  BIGINT COMMENT '累积退款次数',
    `refund_payment_num`                    BIGINT COMMENT '累积退款件数',
    `refund_payment_amount`                 DECIMAL(16, 2) COMMENT '累积退款金额',
    `cart_last_1d_count`                    BIGINT COMMENT '最近1日被加入购物车次数',
    `cart_last_7d_count`                    BIGINT COMMENT '最近7日被加入购物车次数',
    `cart_last_30d_count`                   BIGINT COMMENT '最近30日被加入购物车次数',
    `cart_count`                            BIGINT COMMENT '累积被加入购物车次数',
    `favor_last_1d_count`                   BIGINT COMMENT '最近1日被收藏次数',
    `favor_last_7d_count`                   BIGINT COMMENT '最近7日被收藏次数',
    `favor_last_30d_count`                  BIGINT COMMENT '最近30日被收藏次数',
    `favor_count`                           BIGINT COMMENT '累积被收藏次数',
    `appraise_last_1d_good_count`           BIGINT COMMENT '最近1日好评数',
    `appraise_last_1d_mid_count`            BIGINT COMMENT '最近1日中评数',
    `appraise_last_1d_bad_count`            BIGINT COMMENT '最近1日差评数',
    `appraise_last_1d_default_count`        BIGINT COMMENT '最近1日默认评价数',
    `appraise_last_7d_good_count`           BIGINT COMMENT '最近7日好评数',
    `appraise_last_7d_mid_count`            BIGINT COMMENT '最近7日中评数',
    `appraise_last_7d_bad_count`            BIGINT COMMENT '最近7日差评数',
    `appraise_last_7d_default_count`        BIGINT COMMENT '最近7日默认评价数',
    `appraise_last_30d_good_count`          BIGINT COMMENT '最近30日好评数',
    `appraise_last_30d_mid_count`           BIGINT COMMENT '最近30日中评数',
    `appraise_last_30d_bad_count`           BIGINT COMMENT '最近30日差评数',
    `appraise_last_30d_default_count`       BIGINT COMMENT '最近30日默认评价数',
    `appraise_good_count`                   BIGINT COMMENT '累积好评数',
    `appraise_mid_count`                    BIGINT COMMENT '累积中评数',
    `appraise_bad_count`                    BIGINT COMMENT '累积差评数',
    `appraise_default_count`                BIGINT COMMENT '累积默认评价数'
) COMMENT '商品主题宽表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwt/dwt_sku_topic/'
    TBLPROPERTIES ("parquet.compression" = "lzo");



----数据装载
--首日装载
insert overwrite table dwt_sku_topic partition (dt = '2021-10-13')
select id,
       nvl(order_last_1d_count, 0),
       nvl(order_last_1d_num, 0),
       nvl(order_activity_last_1d_count, 0),
       nvl(order_coupon_last_1d_count, 0),
       nvl(order_activity_reduce_last_1d_amount, 0),
       nvl(order_coupon_reduce_last_1d_amount, 0),
       nvl(order_last_1d_original_amount, 0),
       nvl(order_last_1d_final_amount, 0),
       nvl(order_last_7d_count, 0),
       nvl(order_last_7d_num, 0),
       nvl(order_activity_last_7d_count, 0),
       nvl(order_coupon_last_7d_count, 0),
       nvl(order_activity_reduce_last_7d_amount, 0),
       nvl(order_coupon_reduce_last_7d_amount, 0),
       nvl(order_last_7d_original_amount, 0),
       nvl(order_last_7d_final_amount, 0),
       nvl(order_last_30d_count, 0),
       nvl(order_last_30d_num, 0),
       nvl(order_activity_last_30d_count, 0),
       nvl(order_coupon_last_30d_count, 0),
       nvl(order_activity_reduce_last_30d_amount, 0),
       nvl(order_coupon_reduce_last_30d_amount, 0),
       nvl(order_last_30d_original_amount, 0),
       nvl(order_last_30d_final_amount, 0),
       nvl(order_count, 0),
       nvl(order_num, 0),
       nvl(order_activity_count, 0),
       nvl(order_coupon_count, 0),
       nvl(order_activity_reduce_amount, 0),
       nvl(order_coupon_reduce_amount, 0),
       nvl(order_original_amount, 0),
       nvl(order_final_amount, 0),
       nvl(payment_last_1d_count, 0),
       nvl(payment_last_1d_num, 0),
       nvl(payment_last_1d_amount, 0),
       nvl(payment_last_7d_count, 0),
       nvl(payment_last_7d_num, 0),
       nvl(payment_last_7d_amount, 0),
       nvl(payment_last_30d_count, 0),
       nvl(payment_last_30d_num, 0),
       nvl(payment_last_30d_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_num, 0),
       nvl(payment_amount, 0),
       nvl(refund_order_last_1d_count, 0),
       nvl(refund_order_last_1d_num, 0),
       nvl(refund_order_last_1d_amount, 0),
       nvl(refund_order_last_7d_count, 0),
       nvl(refund_order_last_7d_num, 0),
       nvl(refund_order_last_7d_amount, 0),
       nvl(refund_order_last_30d_count, 0),
       nvl(refund_order_last_30d_num, 0),
       nvl(refund_order_last_30d_amount, 0),
       nvl(refund_order_count, 0),
       nvl(refund_order_num, 0),
       nvl(refund_order_amount, 0),
       nvl(refund_payment_last_1d_count, 0),
       nvl(refund_payment_last_1d_num, 0),
       nvl(refund_payment_last_1d_amount, 0),
       nvl(refund_payment_last_7d_count, 0),
       nvl(refund_payment_last_7d_num, 0),
       nvl(refund_payment_last_7d_amount, 0),
       nvl(refund_payment_last_30d_count, 0),
       nvl(refund_payment_last_30d_num, 0),
       nvl(refund_payment_last_30d_amount, 0),
       nvl(refund_payment_count, 0),
       nvl(refund_payment_num, 0),
       nvl(refund_payment_amount, 0),
       nvl(cart_last_1d_count, 0),
       nvl(cart_last_7d_count, 0),
       nvl(cart_last_30d_count, 0),
       nvl(cart_count, 0),
       nvl(favor_last_1d_count, 0),
       nvl(favor_last_7d_count, 0),
       nvl(favor_last_30d_count, 0),
       nvl(favor_count, 0),
       nvl(appraise_last_1d_good_count, 0),
       nvl(appraise_last_1d_mid_count, 0),
       nvl(appraise_last_1d_bad_count, 0),
       nvl(appraise_last_1d_default_count, 0),
       nvl(appraise_last_7d_good_count, 0),
       nvl(appraise_last_7d_mid_count, 0),
       nvl(appraise_last_7d_bad_count, 0),
       nvl(appraise_last_7d_default_count, 0),
       nvl(appraise_last_30d_good_count, 0),
       nvl(appraise_last_30d_mid_count, 0),
       nvl(appraise_last_30d_bad_count, 0),
       nvl(appraise_last_30d_default_count, 0),
       nvl(appraise_good_count, 0),
       nvl(appraise_mid_count, 0),
       nvl(appraise_bad_count, 0),
       nvl(appraise_default_count, 0)
from (
         select id
         from dim_sku_info
         where dt = '2021-10-13'
     ) t1
         left join
     (
         select sku_id,
                sum(if(dt = '2021-10-13', order_count, 0))                                    order_last_1d_count,
                sum(if(dt = '2021-10-13', order_num, 0))                                      order_last_1d_num,
                sum(if(dt = '2021-10-13', order_activity_count, 0))                           order_activity_last_1d_count,
                sum(if(dt = '2021-10-13', order_coupon_count, 0))                             order_coupon_last_1d_count,
                sum(if(dt = '2021-10-13', order_activity_reduce_amount, 0))                   order_activity_reduce_last_1d_amount,
                sum(if(dt = '2021-10-13', order_coupon_reduce_amount, 0))                     order_coupon_reduce_last_1d_amount,
                sum(if(dt = '2021-10-13', order_original_amount, 0))                          order_last_1d_original_amount,
                sum(if(dt = '2021-10-13', order_final_amount, 0))                             order_last_1d_final_amount,
                sum(if(dt >= date_add('2021-10-13', -6), order_count, 0))                     order_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), order_num, 0))                       order_last_7d_num,
                sum(if(dt >= date_add('2021-10-13', -6), order_activity_count, 0))            order_activity_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), order_coupon_count, 0))              order_coupon_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), order_activity_reduce_amount,
                       0))                                                                    order_activity_reduce_last_7d_amount,
                sum(
                        if(dt >= date_add('2021-10-13', -6), order_coupon_reduce_amount, 0))  order_coupon_reduce_last_7d_amount,
                sum(
                        if(dt >= date_add('2021-10-13', -6), order_original_amount, 0))       order_last_7d_original_amount,
                sum(if(dt >= date_add('2021-10-13', -6), order_final_amount, 0))              order_last_7d_final_amount,
                sum(if(dt >= date_add('2021-10-13', -29), order_count, 0))                    order_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), order_num, 0))                      order_last_30d_num,
                sum(
                        if(dt >= date_add('2021-10-13', -29), order_activity_count, 0))       order_activity_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), order_coupon_count, 0))             order_coupon_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), order_activity_reduce_amount,
                       0))                                                                    order_activity_reduce_last_30d_amount,
                sum(
                        if(dt >= date_add('2021-10-13', -29), order_coupon_reduce_amount, 0)) order_coupon_reduce_last_30d_amount,
                sum(
                        if(dt >= date_add('2021-10-13', -29), order_original_amount, 0))      order_last_30d_original_amount,
                sum(if(dt >= date_add('2021-10-13', -29), order_final_amount, 0))             order_last_30d_final_amount,
                sum(order_count)                                                              order_count,
                sum(order_num)                                                                order_num,
                sum(order_activity_count)                                                     order_activity_count,
                sum(order_coupon_count)                                                       order_coupon_count,
                sum(order_activity_reduce_amount)                                             order_activity_reduce_amount,
                sum(order_coupon_reduce_amount)                                               order_coupon_reduce_amount,
                sum(order_original_amount)                                                    order_original_amount,
                sum(order_final_amount)                                                       order_final_amount,
                sum(if(dt = '2021-10-13', payment_count, 0))                                  payment_last_1d_count,
                sum(if(dt = '2021-10-13', payment_num, 0))                                    payment_last_1d_num,
                sum(if(dt = '2021-10-13', payment_amount, 0))                                 payment_last_1d_amount,
                sum(if(dt >= date_add('2021-10-13', -6), payment_count, 0))                   payment_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), payment_num, 0))                     payment_last_7d_num,
                sum(if(dt >= date_add('2021-10-13', -6), payment_amount, 0))                  payment_last_7d_amount,
                sum(if(dt >= date_add('2021-10-13', -29), payment_count, 0))                  payment_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), payment_num, 0))                    payment_last_30d_num,
                sum(if(dt >= date_add('2021-10-13', -29), payment_amount, 0))                 payment_last_30d_amount,
                sum(payment_count)                                                            payment_count,
                sum(payment_num)                                                              payment_num,
                sum(payment_amount)                                                           payment_amount,
                sum(if(dt = '2021-10-13', refund_order_count, 0))                             refund_order_last_1d_count,
                sum(if(dt = '2021-10-13', refund_order_num, 0))                               refund_order_last_1d_num,
                sum(if(dt = '2021-10-13', refund_order_amount, 0))                            refund_order_last_1d_amount,
                sum(if(dt >= date_add('2021-10-13', -6), refund_order_count, 0))              refund_order_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), refund_order_num, 0))                refund_order_last_7d_num,
                sum(if(dt >= date_add('2021-10-13', -6), refund_order_amount, 0))             refund_order_last_7d_amount,
                sum(if(dt >= date_add('2021-10-13', -29), refund_order_count, 0))             refund_order_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), refund_order_num, 0))               refund_order_last_30d_num,
                sum(if(dt >= date_add('2021-10-13', -29), refund_order_amount, 0))            refund_order_last_30d_amount,
                sum(refund_order_count)                                                       refund_order_count,
                sum(refund_order_num)                                                         refund_order_num,
                sum(refund_order_amount)                                                      refund_order_amount,
                sum(if(dt = '2021-10-13', refund_payment_count, 0))                           refund_payment_last_1d_count,
                sum(if(dt = '2021-10-13', refund_payment_num, 0))                             refund_payment_last_1d_num,
                sum(if(dt = '2021-10-13', refund_payment_amount, 0))                          refund_payment_last_1d_amount,
                sum(if(dt >= date_add('2021-10-13', -6), refund_payment_count, 0))            refund_payment_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -6), refund_payment_num, 0))              refund_payment_last_7d_num,
                sum(
                        if(dt >= date_add('2021-10-13', -6), refund_payment_amount, 0))       refund_payment_last_7d_amount,
                sum(
                        if(dt >= date_add('2021-10-13', -29), refund_payment_count, 0))       refund_payment_last_30d_count,
                sum(if(dt >= date_add('2021-10-13', -29), refund_payment_num, 0))             refund_payment_last_30d_num,
                sum(
                        if(dt >= date_add('2021-10-13', -29), refund_payment_amount, 0))      refund_payment_last_30d_amount,
                sum(refund_payment_count)                                                     refund_payment_count,
                sum(refund_payment_num)                                                       refund_payment_num,
                sum(refund_payment_amount)                                                    refund_payment_amount,
                sum(if(dt = '2021-10-13', cart_count, 0))                                     cart_last_1d_count,
                sum(if(dt >= date_add('2021-10-13', -6), cart_count, 0))                      cart_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -29), cart_count, 0))                     cart_last_30d_count,
                sum(cart_count)                                                               cart_count,
                sum(if(dt = '2021-10-13', favor_count, 0))                                    favor_last_1d_count,
                sum(if(dt >= date_add('2021-10-13', -6), favor_count, 0))                     favor_last_7d_count,
                sum(if(dt >= date_add('2021-10-13', -29), favor_count, 0))                    favor_last_30d_count,
                sum(favor_count)                                                              favor_count,
                sum(if(dt = '2021-10-13', appraise_good_count, 0))                            appraise_last_1d_good_count,
                sum(if(dt = '2021-10-13', appraise_mid_count, 0))                             appraise_last_1d_mid_count,
                sum(if(dt = '2021-10-13', appraise_bad_count, 0))                             appraise_last_1d_bad_count,
                sum(if(dt = '2021-10-13', appraise_default_count, 0))                         appraise_last_1d_default_count,
                sum(if(dt >= date_add('2021-10-13', -6), appraise_good_count, 0))             appraise_last_7d_good_count,
                sum(if(dt >= date_add('2021-10-13', -6), appraise_mid_count, 0))              appraise_last_7d_mid_count,
                sum(if(dt >= date_add('2021-10-13', -6), appraise_bad_count, 0))              appraise_last_7d_bad_count,
                sum(
                        if(dt >= date_add('2021-10-13', -6), appraise_default_count, 0))      appraise_last_7d_default_count,
                sum(if(dt >= date_add('2021-10-13', -29), appraise_good_count, 0))            appraise_last_30d_good_count,
                sum(if(dt >= date_add('2021-10-13', -29), appraise_mid_count, 0))             appraise_last_30d_mid_count,
                sum(if(dt >= date_add('2021-10-13', -29), appraise_bad_count, 0))             appraise_last_30d_bad_count,
                sum(
                        if(dt >= date_add('2021-10-13', -29), appraise_default_count, 0))     appraise_last_30d_default_count,
                sum(appraise_good_count)                                                      appraise_good_count,
                sum(appraise_mid_count)                                                       appraise_mid_count,
                sum(appraise_bad_count)                                                       appraise_bad_count,
                sum(appraise_default_count)                                                   appraise_default_count
         from dws_sku_action_daycount
         group by sku_id
     ) t2
     on t1.id = t2.sku_id;


--每日装载
insert overwrite table dwt_sku_topic partition (dt = '2021-10-14')
select nvl(1d_ago.sku_id, old.sku_id),
       nvl(1d_ago.order_count, 0),
       nvl(1d_ago.order_num, 0),
       nvl(1d_ago.order_activity_count, 0),
       nvl(1d_ago.order_coupon_count, 0),
       nvl(1d_ago.order_activity_reduce_amount, 0.0),
       nvl(1d_ago.order_coupon_reduce_amount, 0.0),
       nvl(1d_ago.order_original_amount, 0.0),
       nvl(1d_ago.order_final_amount, 0.0),
       nvl(old.order_last_7d_count, 0) + nvl(1d_ago.order_count, 0) - nvl(7d_ago.order_count, 0),
       nvl(old.order_last_7d_num, 0) + nvl(1d_ago.order_num, 0) - nvl(7d_ago.order_num, 0),
       nvl(old.order_activity_last_7d_count, 0) + nvl(1d_ago.order_activity_count, 0) -
       nvl(7d_ago.order_activity_count, 0),
       nvl(old.order_coupon_last_7d_count, 0) + nvl(1d_ago.order_coupon_count, 0) - nvl(7d_ago.order_coupon_count, 0),
       nvl(old.order_activity_reduce_last_7d_amount, 0.0) + nvl(1d_ago.order_activity_reduce_amount, 0.0) -
       nvl(7d_ago.order_activity_reduce_amount, 0.0),
       nvl(old.order_coupon_reduce_last_7d_amount, 0.0) + nvl(1d_ago.order_coupon_reduce_amount, 0.0) -
       nvl(7d_ago.order_coupon_reduce_amount, 0.0),
       nvl(old.order_last_7d_original_amount, 0.0) + nvl(1d_ago.order_original_amount, 0.0) -
       nvl(7d_ago.order_original_amount, 0.0),
       nvl(old.order_last_7d_final_amount, 0.0) + nvl(1d_ago.order_final_amount, 0.0) -
       nvl(7d_ago.order_final_amount, 0.0),
       nvl(old.order_last_30d_count, 0) + nvl(1d_ago.order_count, 0) - nvl(30d_ago.order_count, 0),
       nvl(old.order_last_30d_num, 0) + nvl(1d_ago.order_num, 0) - nvl(30d_ago.order_num, 0),
       nvl(old.order_activity_last_30d_count, 0) + nvl(1d_ago.order_activity_count, 0) -
       nvl(30d_ago.order_activity_count, 0),
       nvl(old.order_coupon_last_30d_count, 0) + nvl(1d_ago.order_coupon_count, 0) - nvl(30d_ago.order_coupon_count, 0),
       nvl(old.order_activity_reduce_last_30d_amount, 0.0) + nvl(1d_ago.order_activity_reduce_amount, 0.0) -
       nvl(30d_ago.order_activity_reduce_amount, 0.0),
       nvl(old.order_coupon_reduce_last_30d_amount, 0.0) + nvl(1d_ago.order_coupon_reduce_amount, 0.0) -
       nvl(30d_ago.order_coupon_reduce_amount, 0.0),
       nvl(old.order_last_30d_original_amount, 0.0) + nvl(1d_ago.order_original_amount, 0.0) -
       nvl(30d_ago.order_original_amount, 0.0),
       nvl(old.order_last_30d_final_amount, 0.0) + nvl(1d_ago.order_final_amount, 0.0) -
       nvl(30d_ago.order_final_amount, 0.0),
       nvl(old.order_count, 0) + nvl(1d_ago.order_count, 0),
       nvl(old.order_num, 0) + nvl(1d_ago.order_num, 0),
       nvl(old.order_activity_count, 0) + nvl(1d_ago.order_activity_count, 0),
       nvl(old.order_coupon_count, 0) + nvl(1d_ago.order_coupon_count, 0),
       nvl(old.order_activity_reduce_amount, 0.0) + nvl(1d_ago.order_activity_reduce_amount, 0.0),
       nvl(old.order_coupon_reduce_amount, 0.0) + nvl(1d_ago.order_coupon_reduce_amount, 0.0),
       nvl(old.order_original_amount, 0.0) + nvl(1d_ago.order_original_amount, 0.0),
       nvl(old.order_final_amount, 0.0) + nvl(1d_ago.order_final_amount, 0.0),
       nvl(1d_ago.payment_count, 0),
       nvl(1d_ago.payment_num, 0),
       nvl(1d_ago.payment_amount, 0.0),
       nvl(old.payment_last_7d_count, 0) + nvl(1d_ago.payment_count, 0) - nvl(7d_ago.payment_count, 0),
       nvl(old.payment_last_7d_num, 0) + nvl(1d_ago.payment_num, 0) - nvl(7d_ago.payment_num, 0),
       nvl(old.payment_last_7d_amount, 0.0) + nvl(1d_ago.payment_amount, 0.0) - nvl(7d_ago.payment_amount, 0.0),
       nvl(old.payment_last_30d_count, 0) + nvl(1d_ago.payment_count, 0) - nvl(30d_ago.payment_count, 0),
       nvl(old.payment_last_30d_num, 0) + nvl(1d_ago.payment_num, 0) - nvl(30d_ago.payment_num, 0),
       nvl(old.payment_last_30d_amount, 0.0) + nvl(1d_ago.payment_amount, 0.0) - nvl(30d_ago.payment_amount, 0.0),
       nvl(old.payment_count, 0) + nvl(1d_ago.payment_count, 0),
       nvl(old.payment_num, 0) + nvl(1d_ago.payment_num, 0),
       nvl(old.payment_amount, 0.0) + nvl(1d_ago.payment_amount, 0.0),
       nvl(old.refund_order_last_1d_count, 0) + nvl(1d_ago.refund_order_count, 0) - nvl(1d_ago.refund_order_count, 0),
       nvl(old.refund_order_last_1d_num, 0) + nvl(1d_ago.refund_order_num, 0) - nvl(1d_ago.refund_order_num, 0),
       nvl(old.refund_order_last_1d_amount, 0.0) + nvl(1d_ago.refund_order_amount, 0.0) -
       nvl(1d_ago.refund_order_amount, 0.0),
       nvl(old.refund_order_last_7d_count, 0) + nvl(1d_ago.refund_order_count, 0) - nvl(7d_ago.refund_order_count, 0),
       nvl(old.refund_order_last_7d_num, 0) + nvl(1d_ago.refund_order_num, 0) - nvl(7d_ago.refund_order_num, 0),
       nvl(old.refund_order_last_7d_amount, 0.0) + nvl(1d_ago.refund_order_amount, 0.0) -
       nvl(7d_ago.refund_order_amount, 0.0),
       nvl(old.refund_order_last_30d_count, 0) + nvl(1d_ago.refund_order_count, 0) - nvl(30d_ago.refund_order_count, 0),
       nvl(old.refund_order_last_30d_num, 0) + nvl(1d_ago.refund_order_num, 0) - nvl(30d_ago.refund_order_num, 0),
       nvl(old.refund_order_last_30d_amount, 0.0) + nvl(1d_ago.refund_order_amount, 0.0) -
       nvl(30d_ago.refund_order_amount, 0.0),
       nvl(old.refund_order_count, 0) + nvl(1d_ago.refund_order_count, 0),
       nvl(old.refund_order_num, 0) + nvl(1d_ago.refund_order_num, 0),
       nvl(old.refund_order_amount, 0.0) + nvl(1d_ago.refund_order_amount, 0.0),
       nvl(1d_ago.refund_payment_count, 0),
       nvl(1d_ago.refund_payment_num, 0),
       nvl(1d_ago.refund_payment_amount, 0.0),
       nvl(old.refund_payment_last_7d_count, 0) + nvl(1d_ago.refund_payment_count, 0) -
       nvl(7d_ago.refund_payment_count, 0),
       nvl(old.refund_payment_last_7d_num, 0) + nvl(1d_ago.refund_payment_num, 0) - nvl(7d_ago.refund_payment_num, 0),
       nvl(old.refund_payment_last_7d_amount, 0.0) + nvl(1d_ago.refund_payment_amount, 0.0) -
       nvl(7d_ago.refund_payment_amount, 0.0),
       nvl(old.refund_payment_last_30d_count, 0) + nvl(1d_ago.refund_payment_count, 0) -
       nvl(30d_ago.refund_payment_count, 0),
       nvl(old.refund_payment_last_30d_num, 0) + nvl(1d_ago.refund_payment_num, 0) - nvl(30d_ago.refund_payment_num, 0),
       nvl(old.refund_payment_last_30d_amount, 0.0) + nvl(1d_ago.refund_payment_amount, 0.0) -
       nvl(30d_ago.refund_payment_amount, 0.0),
       nvl(old.refund_payment_count, 0) + nvl(1d_ago.refund_payment_count, 0),
       nvl(old.refund_payment_num, 0) + nvl(1d_ago.refund_payment_num, 0),
       nvl(old.refund_payment_amount, 0.0) + nvl(1d_ago.refund_payment_amount, 0.0),
       nvl(1d_ago.cart_count, 0),
       nvl(old.cart_last_7d_count, 0) + nvl(1d_ago.cart_count, 0) - nvl(7d_ago.cart_count, 0),
       nvl(old.cart_last_30d_count, 0) + nvl(1d_ago.cart_count, 0) - nvl(30d_ago.cart_count, 0),
       nvl(old.cart_count, 0) + nvl(1d_ago.cart_count, 0),
       nvl(1d_ago.favor_count, 0),
       nvl(old.favor_last_7d_count, 0) + nvl(1d_ago.favor_count, 0) - nvl(7d_ago.favor_count, 0),
       nvl(old.favor_last_30d_count, 0) + nvl(1d_ago.favor_count, 0) - nvl(30d_ago.favor_count, 0),
       nvl(old.favor_count, 0) + nvl(1d_ago.favor_count, 0),
       nvl(1d_ago.appraise_good_count, 0),
       nvl(1d_ago.appraise_mid_count, 0),
       nvl(1d_ago.appraise_bad_count, 0),
       nvl(1d_ago.appraise_default_count, 0),
       nvl(old.appraise_last_7d_good_count, 0) + nvl(1d_ago.appraise_good_count, 0) -
       nvl(7d_ago.appraise_good_count, 0),
       nvl(old.appraise_last_7d_mid_count, 0) + nvl(1d_ago.appraise_mid_count, 0) - nvl(7d_ago.appraise_mid_count, 0),
       nvl(old.appraise_last_7d_bad_count, 0) + nvl(1d_ago.appraise_bad_count, 0) - nvl(7d_ago.appraise_bad_count, 0),
       nvl(old.appraise_last_7d_default_count, 0) + nvl(1d_ago.appraise_default_count, 0) -
       nvl(7d_ago.appraise_default_count, 0),
       nvl(old.appraise_last_30d_good_count, 0) + nvl(1d_ago.appraise_good_count, 0) -
       nvl(30d_ago.appraise_good_count, 0),
       nvl(old.appraise_last_30d_mid_count, 0) + nvl(1d_ago.appraise_mid_count, 0) - nvl(30d_ago.appraise_mid_count, 0),
       nvl(old.appraise_last_30d_bad_count, 0) + nvl(1d_ago.appraise_bad_count, 0) - nvl(30d_ago.appraise_bad_count, 0),
       nvl(old.appraise_last_30d_default_count, 0) + nvl(1d_ago.appraise_default_count, 0) -
       nvl(30d_ago.appraise_default_count, 0),
       nvl(old.appraise_good_count, 0) + nvl(1d_ago.appraise_good_count, 0),
       nvl(old.appraise_mid_count, 0) + nvl(1d_ago.appraise_mid_count, 0),
       nvl(old.appraise_bad_count, 0) + nvl(1d_ago.appraise_bad_count, 0),
       nvl(old.appraise_default_count, 0) + nvl(1d_ago.appraise_default_count, 0)
from (
         select sku_id,
                order_last_1d_count,
                order_last_1d_num,
                order_activity_last_1d_count,
                order_coupon_last_1d_count,
                order_activity_reduce_last_1d_amount,
                order_coupon_reduce_last_1d_amount,
                order_last_1d_original_amount,
                order_last_1d_final_amount,
                order_last_7d_count,
                order_last_7d_num,
                order_activity_last_7d_count,
                order_coupon_last_7d_count,
                order_activity_reduce_last_7d_amount,
                order_coupon_reduce_last_7d_amount,
                order_last_7d_original_amount,
                order_last_7d_final_amount,
                order_last_30d_count,
                order_last_30d_num,
                order_activity_last_30d_count,
                order_coupon_last_30d_count,
                order_activity_reduce_last_30d_amount,
                order_coupon_reduce_last_30d_amount,
                order_last_30d_original_amount,
                order_last_30d_final_amount,
                order_count,
                order_num,
                order_activity_count,
                order_coupon_count,
                order_activity_reduce_amount,
                order_coupon_reduce_amount,
                order_original_amount,
                order_final_amount,
                payment_last_1d_count,
                payment_last_1d_num,
                payment_last_1d_amount,
                payment_last_7d_count,
                payment_last_7d_num,
                payment_last_7d_amount,
                payment_last_30d_count,
                payment_last_30d_num,
                payment_last_30d_amount,
                payment_count,
                payment_num,
                payment_amount,
                refund_order_last_1d_count,
                refund_order_last_1d_num,
                refund_order_last_1d_amount,
                refund_order_last_7d_count,
                refund_order_last_7d_num,
                refund_order_last_7d_amount,
                refund_order_last_30d_count,
                refund_order_last_30d_num,
                refund_order_last_30d_amount,
                refund_order_count,
                refund_order_num,
                refund_order_amount,
                refund_payment_last_1d_count,
                refund_payment_last_1d_num,
                refund_payment_last_1d_amount,
                refund_payment_last_7d_count,
                refund_payment_last_7d_num,
                refund_payment_last_7d_amount,
                refund_payment_last_30d_count,
                refund_payment_last_30d_num,
                refund_payment_last_30d_amount,
                refund_payment_count,
                refund_payment_num,
                refund_payment_amount,
                cart_last_1d_count,
                cart_last_7d_count,
                cart_last_30d_count,
                cart_count,
                favor_last_1d_count,
                favor_last_7d_count,
                favor_last_30d_count,
                favor_count,
                appraise_last_1d_good_count,
                appraise_last_1d_mid_count,
                appraise_last_1d_bad_count,
                appraise_last_1d_default_count,
                appraise_last_7d_good_count,
                appraise_last_7d_mid_count,
                appraise_last_7d_bad_count,
                appraise_last_7d_default_count,
                appraise_last_30d_good_count,
                appraise_last_30d_mid_count,
                appraise_last_30d_bad_count,
                appraise_last_30d_default_count,
                appraise_good_count,
                appraise_mid_count,
                appraise_bad_count,
                appraise_default_count
         from dwt_sku_topic
         where dt = date_add('2021-10-14', -1)
     ) old
         full outer join
     (
         select sku_id,
                order_count,
                order_num,
                order_activity_count,
                order_coupon_count,
                order_activity_reduce_amount,
                order_coupon_reduce_amount,
                order_original_amount,
                order_final_amount,
                payment_count,
                payment_num,
                payment_amount,
                refund_order_count,
                refund_order_num,
                refund_order_amount,
                refund_payment_count,
                refund_payment_num,
                refund_payment_amount,
                cart_count,
                favor_count,
                appraise_good_count,
                appraise_mid_count,
                appraise_bad_count,
                appraise_default_count
         from dws_sku_action_daycount
         where dt = '2021-10-14'
     ) 1d_ago
     on old.sku_id = 1d_ago.sku_id
         left join
     (
         select sku_id,
                order_count,
                order_num,
                order_activity_count,
                order_coupon_count,
                order_activity_reduce_amount,
                order_coupon_reduce_amount,
                order_original_amount,
                order_final_amount,
                payment_count,
                payment_num,
                payment_amount,
                refund_order_count,
                refund_order_num,
                refund_order_amount,
                refund_payment_count,
                refund_payment_num,
                refund_payment_amount,
                cart_count,
                favor_count,
                appraise_good_count,
                appraise_mid_count,
                appraise_bad_count,
                appraise_default_count
         from dws_sku_action_daycount
         where dt = date_add('2021-10-14', -7)
     ) 7d_ago
     on old.sku_id = 7d_ago.sku_id
         left join
     (
         select sku_id,
                order_count,
                order_num,
                order_activity_count,
                order_coupon_count,
                order_activity_reduce_amount,
                order_coupon_reduce_amount,
                order_original_amount,
                order_final_amount,
                payment_count,
                payment_num,
                payment_amount,
                refund_order_count,
                refund_order_num,
                refund_order_amount,
                refund_payment_count,
                refund_payment_num,
                refund_payment_amount,
                cart_count,
                favor_count,
                appraise_good_count,
                appraise_mid_count,
                appraise_bad_count,
                appraise_default_count
         from dws_sku_action_daycount
         where dt = date_add('2021-10-14', -30)
     ) 30d_ago
     on old.sku_id = 30d_ago.sku_id;
