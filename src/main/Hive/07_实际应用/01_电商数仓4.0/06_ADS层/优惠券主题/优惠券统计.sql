--该需求要求统计最近30日发布的所有优惠券的领用情况和补贴率，补贴率是指，优惠金额与使用优惠券的订单的原价金额的比值。
--建表语句
DROP TABLE IF EXISTS ads_coupon_stats;
CREATE EXTERNAL TABLE ads_coupon_stats
(
    `dt`                    STRING COMMENT '统计日期',
    `coupon_id`             STRING COMMENT '优惠券ID',
    `coupon_name`           STRING COMMENT '优惠券名称',
    `start_date`            STRING COMMENT '发布日期',
    `rule_name`             STRING COMMENT '优惠规则，例如满100元减10元',
    `get_count`             BIGINT COMMENT '领取次数',
    `order_count`           BIGINT COMMENT '使用(下单)次数',
    `expire_count`          BIGINT COMMENT '过期次数',
    `order_original_amount` DECIMAL(16, 2) COMMENT '使用优惠券订单原始金额',
    `order_final_amount`    DECIMAL(16, 2) COMMENT '使用优惠券订单最终金额',
    `reduce_amount`         DECIMAL(16, 2) COMMENT '优惠金额',
    `reduce_rate`           DECIMAL(16, 2) COMMENT '补贴率'
) COMMENT '商品销售统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_coupon_stats/';


--数据装载
insert overwrite table ads_coupon_stats
select *
from ads_coupon_stats
union
select '2020-06-14' dt,
       t1.id,
       coupon_name,
       start_date,
       rule_name,
       get_count,
       order_count,
       expire_count,
       order_original_amount,
       order_final_amount,
       reduce_amount,
       reduce_rate
from (
         select id,
                coupon_name,
                date_format(start_time, 'yyyy-MM-dd') start_date,
                case
                    when coupon_type = '3201' then concat('满', condition_amount, '元减', benefit_amount, '元')
                    when coupon_type = '3202' then concat('满', condition_num, '件打', (1 - benefit_discount) * 10, '折')
                    when coupon_type = '3203' then concat('减', benefit_amount, '元')
                    end                               rule_name
         from dim_coupon_info
         where dt = '2020-06-14'
           and date_format(start_time, 'yyyy-MM-dd') >= date_add('2020-06-14', -29)
     ) t1
         left join
     (
         select coupon_id,
                get_count,
                order_count,
                expire_count,
                order_original_amount,
                order_final_amount,
                order_reduce_amount                                                 reduce_amount,
                cast(order_reduce_amount / order_original_amount as decimal(16, 2)) reduce_rate
         from dwt_coupon_topic
         where dt = '2020-06-14'
     ) t2
     on t1.id = t2.coupon_id;
