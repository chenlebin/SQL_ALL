--建表语句
DROP TABLE IF EXISTS dws_area_stats_daycount;
CREATE EXTERNAL TABLE dws_area_stats_daycount
(
    `province_id`           STRING COMMENT '地区编号',
    `visit_count`           BIGINT COMMENT '访问次数',
    `login_count`           BIGINT COMMENT '登录次数',
    `visitor_count`         BIGINT COMMENT '访客人数',
    `user_count`            BIGINT COMMENT '用户人数',
    `order_count`           BIGINT COMMENT '下单次数',
    `order_original_amount` DECIMAL(16, 2) COMMENT '下单原始金额',
    `order_final_amount`    DECIMAL(16, 2) COMMENT '下单最终金额',
    `payment_count`         BIGINT COMMENT '支付次数',
    `payment_amount`        DECIMAL(16, 2) COMMENT '支付金额',
    `refund_order_count`    BIGINT COMMENT '退单次数',
    `refund_order_amount`   DECIMAL(16, 2) COMMENT '退单金额',
    `refund_payment_count`  BIGINT COMMENT '退款次数',
    `refund_payment_amount` DECIMAL(16, 2) COMMENT '退款金额'
) COMMENT '每日地区统计表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_area_stats_daycount/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


--装载语句
with tmp_vu as
         (
             select dt,
                    id province_id,
                    visit_count,
                    login_count,
                    visitor_count,
                    user_count
             from (
                      select dt,
                             area_code,
                             count(*)                  visit_count,--访客访问次数
                             count(user_id)            login_count,--用户访问次数,等价于sum(if(user_id is not null,1,0))
                             count(distinct (mid_id))  visitor_count,--访客人数
                             count(distinct (user_id)) user_count--用户人数
                      from dwd_page_log
                      where last_page_id is null
                      group by dt, area_code
                  ) tmp
                      left join dim_base_province area
                                on tmp.area_code = area.area_code
         ),
     tmp_order as
         (
             select date_format(create_time, 'yyyy-MM-dd') dt,
                    province_id,
                    count(*)                               order_count,
                    sum(original_amount)                   order_original_amount,
                    sum(final_amount)                      order_final_amount
             from dwd_order_info
             group by date_format(create_time, 'yyyy-MM-dd'), province_id
         ),
     tmp_pay as
         (
             select date_format(callback_time, 'yyyy-MM-dd') dt,
                    province_id,
                    count(*)                                 payment_count,
                    sum(payment_amount)                      payment_amount
             from dwd_payment_info
             group by date_format(callback_time, 'yyyy-MM-dd'), province_id
         ),
     tmp_ro as
         (
             select date_format(create_time, 'yyyy-MM-dd') dt,
                    province_id,
                    count(*)                               refund_order_count,
                    sum(refund_amount)                     refund_order_amount
             from dwd_order_refund_info
             group by date_format(create_time, 'yyyy-MM-dd'), province_id
         ),
     tmp_rp as
         (
             select date_format(callback_time, 'yyyy-MM-dd') dt,
                    province_id,
                    count(*)                                 refund_payment_count,
                    sum(refund_amount)                       refund_payment_amount
             from dwd_refund_payment
             group by date_format(callback_time, 'yyyy-MM-dd'), province_id
         )
insert
overwrite
table
dws_area_stats_daycount
partition
(
dt
)
select province_id,
       sum(visit_count),
       sum(login_count),
       sum(visitor_count),
       sum(user_count),
       sum(order_count),
       sum(order_original_amount),
       sum(order_final_amount),
       sum(payment_count),
       sum(payment_amount),
       sum(refund_order_count),
       sum(refund_order_amount),
       sum(refund_payment_count),
       sum(refund_payment_amount),
       dt
from (
         select dt,
                province_id,
                visit_count,
                login_count,
                visitor_count,
                user_count,
                0 order_count,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_amount
         from tmp_vu
         union all
         select dt,
                province_id,
                0 visit_count,
                0 login_count,
                0 visitor_count,
                0 user_count,
                order_count,
                order_original_amount,
                order_final_amount,
                0 payment_count,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_amount
         from tmp_order
         union all
         select dt,
                province_id,
                0 visit_count,
                0 login_count,
                0 visitor_count,
                0 user_count,
                0 order_count,
                0 order_original_amount,
                0 order_final_amount,
                payment_count,
                payment_amount,
                0 refund_order_count,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_amount
         from tmp_pay
         union all
         select dt,
                province_id,
                0 visit_count,
                0 login_count,
                0 visitor_count,
                0 user_count,
                0 order_count,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_amount,
                refund_order_count,
                refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_amount
         from tmp_ro
         union all
         select dt,
                province_id,
                0 visit_count,
                0 login_count,
                0 visitor_count,
                0 user_count,
                0 order_count,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_amount,
                refund_payment_count,
                refund_payment_amount
         from tmp_rp
     ) t1
group by dt, province_id;


--每日装载
with tmp_vu as
         (
             select id province_id,
                    visit_count,
                    login_count,
                    visitor_count,
                    user_count
             from (
                      select area_code,
                             count(*)                  visit_count,--访客访问次数
                             count(user_id)            login_count,--用户访问次数,等价于sum(if(user_id is not null,1,0))
                             count(distinct (mid_id))  visitor_count,--访客人数
                             count(distinct (user_id)) user_count--用户人数
                      from dwd_page_log
                      where dt = '2020-06-15'
                        and last_page_id is null
                      group by area_code
                  ) tmp
                      left join dim_base_province area
                                on tmp.area_code = area.area_code
         ),
     tmp_order as
         (
             select province_id,
                    count(*)             order_count,
                    sum(original_amount) order_original_amount,
                    sum(final_amount)    order_final_amount
             from dwd_order_info
             where dt = '2020-06-15'
                or dt = '9999-99-99'
                 and date_format(create_time, 'yyyy-MM-dd') = '2020-06-15'
             group by province_id
         ),
     tmp_pay as
         (
             select province_id,
                    count(*)            payment_count,
                    sum(payment_amount) payment_amount
             from dwd_payment_info
             where dt = '2020-06-15'
             group by province_id
         ),
     tmp_ro as
         (
             select province_id,
                    count(*)           refund_order_count,
                    sum(refund_amount) refund_order_amount
             from dwd_order_refund_info
             where dt = '2020-06-15'
             group by province_id
         ),
     tmp_rp as
         (
             select province_id,
                    count(*)           refund_payment_count,
                    sum(refund_amount) refund_payment_amount
             from dwd_refund_payment
             where dt = '2020-06-15'
             group by province_id
         )
insert
overwrite
table
dws_area_stats_daycount
partition
(
dt = '2020-06-15'
)
select province_id,
       sum(visit_count),
       sum(login_count),
       sum(visitor_count),
       sum(user_count),
       sum(order_count),
       sum(order_original_amount),
       sum(order_final_amount),
       sum(payment_count),
       sum(payment_amount),
       sum(refund_order_count),
       sum(refund_order_amount),
       sum(refund_payment_count),
       sum(refund_payment_amount)
from (
         select province_id,
                visit_count,
                login_count,
                visitor_count,
                user_count,
                0 order_count,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_amount
         from tmp_vu
         union all
         select province_id,
                0 visit_count,
                0 login_count,
                0 visitor_count,
                0 user_count,
                order_count,
                order_original_amount,
                order_final_amount,
                0 payment_count,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_amount
         from tmp_order
         union all
         select province_id,
                0 visit_count,
                0 login_count,
                0 visitor_count,
                0 user_count,
                0 order_count,
                0 order_original_amount,
                0 order_final_amount,
                payment_count,
                payment_amount,
                0 refund_order_count,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_amount
         from tmp_pay
         union all
         select province_id,
                0 visit_count,
                0 login_count,
                0 visitor_count,
                0 user_count,
                0 order_count,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_amount,
                refund_order_count,
                refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_amount
         from tmp_ro
         union all
         select province_id,
                0 visit_count,
                0 login_count,
                0 visitor_count,
                0 user_count,
                0 order_count,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_amount,
                refund_payment_count,
                refund_payment_amount
         from tmp_rp
     ) t1
group by province_id;
