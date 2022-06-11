--建表语句
DROP TABLE IF EXISTS dwt_activity_topic;
CREATE EXTERNAL TABLE dwt_activity_topic
(
    `activity_rule_id`              STRING COMMENT '活动规则ID',
    `activity_id`                   STRING COMMENT '活动ID',
    `order_last_1d_count`           BIGINT COMMENT '最近1日参与某活动某规则下单次数',
    `order_last_1d_reduce_amount`   DECIMAL(16, 2) COMMENT '最近1日参与某活动某规则下单优惠金额',
    `order_last_1d_original_amount` DECIMAL(16, 2) COMMENT '最近1日参与某活动某规则下单原始金额',
    `order_last_1d_final_amount`    DECIMAL(16, 2) COMMENT '最近1日参与某活动某规则下单最终金额',
    `order_count`                   BIGINT COMMENT '参与某活动某规则累积下单次数',
    `order_reduce_amount`           DECIMAL(16, 2) COMMENT '参与某活动某规则累积下单优惠金额',
    `order_original_amount`         DECIMAL(16, 2) COMMENT '参与某活动某规则累积下单原始金额',
    `order_final_amount`            DECIMAL(16, 2) COMMENT '参与某活动某规则累积下单最终金额',
    `payment_last_1d_count`         BIGINT COMMENT '最近1日参与某活动某规则支付次数',
    `payment_last_1d_reduce_amount` DECIMAL(16, 2) COMMENT '最近1日参与某活动某规则支付优惠金额',
    `payment_last_1d_amount`        DECIMAL(16, 2) COMMENT '最近1日参与某活动某规则支付金额',
    `payment_count`                 BIGINT COMMENT '参与某活动某规则累积支付次数',
    `payment_reduce_amount`         DECIMAL(16, 2) COMMENT '参与某活动某规则累积支付优惠金额',
    `payment_amount`                DECIMAL(16, 2) COMMENT '参与某活动某规则累积支付金额'
) COMMENT '活动主题宽表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwt/dwt_activity_topic/'
    TBLPROPERTIES ("parquet.compression" = "lzo");



-----数据装载
--首日装载
insert overwrite table dwt_activity_topic partition (dt = '2021-10-13')
select t1.activity_rule_id,
       t1.activity_id,
       nvl(order_last_1d_count, 0),
       nvl(order_last_1d_reduce_amount, 0),
       nvl(order_last_1d_original_amount, 0),
       nvl(order_last_1d_final_amount, 0),
       nvl(order_count, 0),
       nvl(order_reduce_amount, 0),
       nvl(order_original_amount, 0),
       nvl(order_final_amount, 0),
       nvl(payment_last_1d_count, 0),
       nvl(payment_last_1d_reduce_amount, 0),
       nvl(payment_last_1d_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_reduce_amount, 0),
       nvl(payment_amount, 0)
from (
         select activity_rule_id,
                activity_id
         from dim_activity_rule_info
         where dt = '2021-10-13'
     ) t1
         left join
     (
         select activity_rule_id,
                activity_id,

                sum(if(dt = '2021-10-13', order_count, 0))           order_last_1d_count,
                sum(if(dt = '2021-10-13', order_reduce_amount, 0))   order_last_1d_reduce_amount,
                sum(if(dt = '2021-10-13', order_original_amount, 0)) order_last_1d_original_amount,
                sum(if(dt = '2021-10-13', order_final_amount, 0))    order_last_1d_final_amount,
                sum(order_count)                                     order_count,
                sum(order_reduce_amount)                             order_reduce_amount,
                sum(order_original_amount)                           order_original_amount,
                sum(order_final_amount)                              order_final_amount,
                sum(if(dt = '2021-10-13', payment_count, 0))         payment_last_1d_count,
                sum(if(dt = '2021-10-13', payment_reduce_amount, 0)) payment_last_1d_reduce_amount,
                sum(if(dt = '2021-10-13', payment_amount, 0))        payment_last_1d_amount,
                sum(payment_count)                                   payment_count,
                sum(payment_reduce_amount)                           payment_reduce_amount,
                sum(payment_amount)                                  payment_amount
         from dws_activity_info_daycount
         group by activity_rule_id, activity_id
     ) t2
     on t1.activity_rule_id = t2.activity_rule_id
         and t1.activity_id = t2.activity_id;
（2）每日装载
insert overwrite table dwt_activity_topic partition (dt = '2020-
insert overwrite table dwt_activity_topic partition(dt='2021-10-14')
select
    nvl(1d_ago.activity_rule_id,old.activity_rule_id),
    nvl(1d_ago.activity_id,old.activity_id),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_reduce_amount,0.0)+nvl(1d_ago.order_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0)
from
(
    select
        activity_rule_id,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from dwt_activity_topic
    where dt=date_add('2021-10-14',-1)
)old
full outer join
(
    select
        activity_rule_id,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from dws_activity_info_daycount
    where dt='2021-10-14'
)1d_ago
on old.activity_rule_id=1d_ago.activity_rule_id;
