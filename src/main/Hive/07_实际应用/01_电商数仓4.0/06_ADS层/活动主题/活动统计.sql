--该需求要求统计最近30日发布的所有活动的参与情况和补贴率，补贴率是指，优惠金额与参与活动的订单原价金额的比值。
--建表语句
DROP TABLE IF EXISTS ads_activity_stats;
CREATE EXTERNAL TABLE `ads_activity_stats`
(
    `dt`                    STRING COMMENT '统计日期',
    `activity_id`           STRING COMMENT '活动ID',
    `activity_name`         STRING COMMENT '活动名称',
    `start_date`            STRING COMMENT '活动开始日期',
    `order_count`           BIGINT COMMENT '参与活动订单数',
    `order_original_amount` DECIMAL(16, 2) COMMENT '参与活动订单原始金额',
    `order_final_amount`    DECIMAL(16, 2) COMMENT '参与活动订单最终金额',
    `reduce_amount`         DECIMAL(16, 2) COMMENT '优惠金额',
    `reduce_rate`           DECIMAL(16, 2) COMMENT '补贴率'
) COMMENT '商品销售统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_activity_stats/';


--数据装载
insert overwrite table ads_activity_stats
select *
from ads_activity_stats
union
select '2020-06-14' dt,
       t4.activity_id,
       activity_name,
       start_date,
       order_count,
       order_original_amount,
       order_final_amount,
       reduce_amount,
       reduce_rate
from (
         select activity_id,
                activity_name,
                date_format(start_time, 'yyyy-MM-dd') start_date
         from dim_activity_rule_info
         where dt = '2020-06-14'
           and date_format(start_time, 'yyyy-MM-dd') >= date_add('2020-06-14', -29)
         group by activity_id, activity_name, start_time
     ) t4
         left join
     (
         select activity_id,
                sum(order_count)                                                                    order_count,
                sum(order_original_amount)                                                          order_original_amount,
                sum(order_final_amount)                                                             order_final_amount,
                sum(order_reduce_amount)                                                            reduce_amount,
                cast(sum(order_reduce_amount) / sum(order_original_amount) * 100 as decimal(16, 2)) reduce_rate
         from dwt_activity_topic
         where dt = '2020-06-14'
         group by activity_id
     ) t5
     on t4.activity_id = t5.activity_id;
