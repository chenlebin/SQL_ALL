--建表语句
DROP TABLE IF EXISTS dws_activity_info_daycount;
CREATE EXTERNAL TABLE dws_activity_info_daycount
(
    `activity_rule_id`      STRING COMMENT '活动规则ID',
    `activity_id`           STRING COMMENT '活动ID',
    `order_count`           BIGINT COMMENT '参与某活动某规则下单次数',
    `order_reduce_amount`   DECIMAL(16, 2) COMMENT '参与某活动某规则下单减免金额',
    `order_original_amount` DECIMAL(16, 2) COMMENT '参与某活动某规则下单原始金额',
    `order_final_amount`    DECIMAL(16, 2) COMMENT '参与某活动某规则下单最终金额',
    `payment_count`         BIGINT COMMENT '参与某活动某规则支付次数',
    `payment_reduce_amount` DECIMAL(16, 2) COMMENT '参与某活动某规则支付减免金额',
    `payment_amount`        DECIMAL(16, 2) COMMENT '参与某活动某规则支付金额'
) COMMENT '每日活动统计'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_activity_info_daycount/'
    TBLPROPERTIES ("parquet.compression" = "lzo");
----数据装载
--首日装载
with tmp_order as
         (
             select date_format(create_time, 'yyyy-MM-dd') dt,
                    activity_rule_id,
                    activity_id,
                    count(*)                               order_count,
                    sum(split_activity_amount)             order_reduce_amount,
                    sum(original_amount)                   order_original_amount,
                    sum(split_final_amount)                order_final_amount
             from dwd_order_detail
             where activity_id is not null
             group by date_format(create_time, 'yyyy-MM-dd'), activity_rule_id, activity_id
         ),
     tmp_pay as
         (
             select date_format(callback_time, 'yyyy-MM-dd') dt,
                    activity_rule_id,
                    activity_id,
                    count(*)                                 payment_count,
                    sum(split_activity_amount)               payment_reduce_amount,
                    sum(split_final_amount)                  payment_amount
             from (
                      select activity_rule_id,
                             activity_id,
                             order_id,
                             split_activity_amount,
                             split_final_amount
                      from dwd_order_detail
                      where activity_id is not null
                  ) od
                      join
                  (
                      select order_id,
                             callback_time
                      from dwd_payment_info
                  ) pi
                  on od.order_id = pi.order_id
             group by date_format(callback_time, 'yyyy-MM-dd'), activity_rule_id, activity_id
         )
insert
overwrite
table
dws_activity_info_daycount
partition
(
dt
)
select activity_rule_id,
       activity_id,
       sum(order_count),
       sum(order_reduce_amount),
       sum(order_original_amount),
       sum(order_final_amount),
       sum(payment_count),
       sum(payment_reduce_amount),
       sum(payment_amount),
       dt
from (
         select dt,
                activity_rule_id,
                activity_id,
                order_count,
                order_reduce_amount,
                order_original_amount,
                order_final_amount,
                0 payment_count,
                0 payment_reduce_amount,
                0 payment_amount
         from tmp_order
         union all
         select dt,
                activity_rule_id,
                activity_id,
                0 order_count,
                0 order_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                payment_count,
                payment_reduce_amount,
                payment_amount
         from tmp_pay
     ) t1
group by dt, activity_rule_id, activity_id;


--每日装载
with tmp_order as
         (
             select activity_rule_id,
                    activity_id,
                    count(*)                   order_count,
                    sum(split_activity_amount) order_reduce_amount,
                    sum(original_amount)       order_original_amount,
                    sum(split_final_amount)    order_final_amount
             from dwd_order_detail
             where dt = '2021-10-14'
               and activity_id is not null
             group by activity_rule_id, activity_id
         ),
     tmp_pay as
         (
             select activity_rule_id,
                    activity_id,
                    count(*)                   payment_count,
                    sum(split_activity_amount) payment_reduce_amount,
                    sum(split_final_amount)    payment_amount
             from dwd_order_detail
             where (dt = '2021-10-14'
                 or dt = date_add('2021-10-14', -1))
               and activity_id is not null
               and order_id in
                   (
                       select order_id
                       from dwd_payment_info
                       where dt = '2021-10-14'
                   )
             group by activity_rule_id, activity_id
         )
insert
overwrite
table
dws_activity_info_daycount
partition
(
dt = '2021-10-14'
)
select activity_rule_id,
       activity_id,
       sum(order_count),
       sum(order_reduce_amount),
       sum(order_original_amount),
       sum(order_final_amount),
       sum(payment_count),
       sum(payment_reduce_amount),
       sum(payment_amount)
from (
         select activity_rule_id,
                activity_id,
                order_count,
                order_reduce_amount,
                order_original_amount,
                order_final_amount,
                0 payment_count,
                0 payment_reduce_amount,
                0 payment_amount
         from tmp_order
         union all
         select activity_rule_id,
                activity_id,
                0 order_count,
                0 order_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                payment_count,
                payment_reduce_amount,
                payment_amount
         from tmp_pay
     ) t1
group by activity_rule_id, activity_id;
