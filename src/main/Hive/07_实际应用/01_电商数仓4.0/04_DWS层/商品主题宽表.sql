--商品主题宽表
--每行数据代表一个sku（代表一款商品例如小米11 256g硬盘 12g内存手机）在一天之中的汇总行为。商品维度相关的事实表有
--订单详情、加购、收藏、评价、退单、退款这些事实表，字段中有这些事实表的度量值的聚合值。


--建表语句
DROP TABLE IF EXISTS dws_sku_action_daycount;
CREATE EXTERNAL TABLE dws_sku_action_daycount
(
    `sku_id`                       STRING COMMENT 'sku_id',
    --主键ID
    `order_count`                  BIGINT COMMENT '被下单次数',
    `order_num`                    BIGINT COMMENT '被下单件数',
    `order_activity_count`         BIGINT COMMENT '参与活动被下单次数',
    `order_coupon_count`           BIGINT COMMENT '使用优惠券被下单次数',
    `order_activity_reduce_amount` DECIMAL(16, 2) COMMENT '优惠金额(活动)',
    `order_coupon_reduce_amount`   DECIMAL(16, 2) COMMENT '优惠金额(优惠券)',
    --优惠券事实表的度量值的聚合值
    `order_original_amount`        DECIMAL(16, 2) COMMENT '被下单原价金额',
    `order_final_amount`           DECIMAL(16, 2) COMMENT '被下单最终金额',
    --下单事实表的度量值的聚合值
    `payment_count`                BIGINT COMMENT '被支付次数',
    `payment_num`                  BIGINT COMMENT '被支付件数',
    `payment_amount`               DECIMAL(16, 2) COMMENT '被支付金额',
    --支付事实表的度量值的聚合值
    `refund_order_count`           BIGINT COMMENT '被退单次数',
    `refund_order_num`             BIGINT COMMENT '被退单件数',
    `refund_order_amount`          DECIMAL(16, 2) COMMENT '被退单金额',
    `refund_payment_count`         BIGINT COMMENT '被退款次数',
    `refund_payment_num`           BIGINT COMMENT '被退款件数',
    `refund_payment_amount`        DECIMAL(16, 2) COMMENT '被退款金额',
    --退单退款事实表度量值的聚合值
    `cart_count`                   BIGINT COMMENT '被加入购物车次数',
    --加购事实表度量值的聚合值
    `favor_count`                  BIGINT COMMENT '被收藏次数',
    --收藏事实表度量值的聚合值
    `appraise_good_count`          BIGINT COMMENT '好评数',
    `appraise_mid_count`           BIGINT COMMENT '中评数',
    `appraise_bad_count`           BIGINT COMMENT '差评数',
    `appraise_default_count`       BIGINT COMMENT '默认评价数'
    --评价事实表度量值的聚合值
) COMMENT '每日商品行为'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_sku_action_daycount/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


---分区情况及数据走向和数据装载
--分区还还是按天进行分区，每个分区中存储的数据是当天活跃商品的汇总行为。

----数据装载语句
--首日装载
with tmp_order as
         (
             select date_format(create_time, 'yyyy-MM-dd')   dt,
                    sku_id,
                    count(*)                                 order_count,
                    sum(sku_num)                             order_num,
                    sum(if(split_activity_amount > 0, 1, 0)) order_activity_count,
                    sum(if(split_coupon_amount > 0, 1, 0))   order_coupon_count,
                    sum(split_activity_amount)               order_activity_reduce_amount,
                    sum(split_coupon_amount)                 order_coupon_reduce_amount,
                    sum(original_amount)                     order_original_amount,
                    sum(split_final_amount)                  order_final_amount
             from dwd_order_detail
             group by date_format(create_time, 'yyyy-MM-dd'), sku_id
         ),
     tmp_pay as
         (
             select date_format(callback_time, 'yyyy-MM-dd') dt,
                    sku_id,
                    count(*)                                 payment_count,
                    sum(sku_num)                             payment_num,
                    sum(split_final_amount)                  payment_amount
             from dwd_order_detail od
                      join
                  (
                      select order_id,
                             callback_time
                      from dwd_payment_info
                      where callback_time is not null
                  ) pi on pi.order_id = od.order_id
             group by date_format(callback_time, 'yyyy-MM-dd'), sku_id
         ),
     tmp_ri as
         (
             select date_format(create_time, 'yyyy-MM-dd') dt,
                    sku_id,
                    count(*)                               refund_order_count,
                    sum(refund_num)                        refund_order_num,
                    sum(refund_amount)                     refund_order_amount
             from dwd_order_refund_info
             group by date_format(create_time, 'yyyy-MM-dd'), sku_id
         ),
     tmp_rp as
         (
             select date_format(callback_time, 'yyyy-MM-dd') dt,
                    rp.sku_id,
                    count(*)                                 refund_payment_count,
                    sum(ri.refund_num)                       refund_payment_num,
                    sum(refund_amount)                       refund_payment_amount
             from (
                      select order_id,
                             sku_id,
                             refund_amount,
                             callback_time
                      from dwd_refund_payment
                  ) rp
                      left join
                  (
                      select order_id,
                             sku_id,
                             refund_num
                      from dwd_order_refund_info
                  ) ri
                  on rp.order_id = ri.order_id
                      and rp.sku_id = ri.sku_id
             group by date_format(callback_time, 'yyyy-MM-dd'), rp.sku_id
         ),
     tmp_cf as
         (
             select dt,
                    item                                   sku_id,
                    sum(if(action_id = 'cart_add', 1, 0))  cart_count,
                    sum(if(action_id = 'favor_add', 1, 0)) favor_count
             from dwd_action_log
             where action_id in ('cart_add', 'favor_add')
             group by dt, item
         ),
     tmp_comment as
         (
             select date_format(create_time, 'yyyy-MM-dd') dt,
                    sku_id,
                    sum(if(appraise = '1201', 1, 0))       appraise_good_count,
                    sum(if(appraise = '1202', 1, 0))       appraise_mid_count,
                    sum(if(appraise = '1203', 1, 0))       appraise_bad_count,
                    sum(if(appraise = '1204', 1, 0))       appraise_default_count
             from dwd_comment_info
             group by date_format(create_time, 'yyyy-MM-dd'), sku_id
         )
insert
overwrite
table
dws_sku_action_daycount
partition
(
dt
)
select sku_id,
       sum(order_count),
       sum(order_num),
       sum(order_activity_count),
       sum(order_coupon_count),
       sum(order_activity_reduce_amount),
       sum(order_coupon_reduce_amount),
       sum(order_original_amount),
       sum(order_final_amount),
       sum(payment_count),
       sum(payment_num),
       sum(payment_amount),
       sum(refund_order_count),
       sum(refund_order_num),
       sum(refund_order_amount),
       sum(refund_payment_count),
       sum(refund_payment_num),
       sum(refund_payment_amount),
       sum(cart_count),
       sum(favor_count),
       sum(appraise_good_count),
       sum(appraise_mid_count),
       sum(appraise_bad_count),
       sum(appraise_default_count),
       dt
from (
         select dt,
                sku_id,
                order_count,
                order_num,
                order_activity_count,
                order_coupon_count,
                order_activity_reduce_amount,
                order_coupon_reduce_amount,
                order_original_amount,
                order_final_amount,
                0 payment_count,
                0 payment_num,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_num,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_num,
                0 refund_payment_amount,
                0 cart_count,
                0 favor_count,
                0 appraise_good_count,
                0 appraise_mid_count,
                0 appraise_bad_count,
                0 appraise_default_count
         from tmp_order
         union all
         select dt,
                sku_id,
                0 order_count,
                0 order_num,
                0 order_activity_count,
                0 order_coupon_count,
                0 order_activity_reduce_amount,
                0 order_coupon_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                payment_count,
                payment_num,
                payment_amount,
                0 refund_order_count,
                0 refund_order_num,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_num,
                0 refund_payment_amount,
                0 cart_count,
                0 favor_count,
                0 appraise_good_count,
                0 appraise_mid_count,
                0 appraise_bad_count,
                0 appraise_default_count
         from tmp_pay
         union all
         select dt,
                sku_id,
                0 order_count,
                0 order_num,
                0 order_activity_count,
                0 order_coupon_count,
                0 order_activity_reduce_amount,
                0 order_coupon_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_num,
                0 payment_amount,
                refund_order_count,
                refund_order_num,
                refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_num,
                0 refund_payment_amount,
                0 cart_count,
                0 favor_count,
                0 appraise_good_count,
                0 appraise_mid_count,
                0 appraise_bad_count,
                0 appraise_default_count
         from tmp_ri
         union all
         select dt,
                sku_id,
                0 order_count,
                0 order_num,
                0 order_activity_count,
                0 order_coupon_count,
                0 order_activity_reduce_amount,
                0 order_coupon_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_num,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_num,
                0 refund_order_amount,
                refund_payment_count,
                refund_payment_num,
                refund_payment_amount,
                0 cart_count,
                0 favor_count,
                0 appraise_good_count,
                0 appraise_mid_count,
                0 appraise_bad_count,
                0 appraise_default_count
         from tmp_rp
         union all
         select dt,
                sku_id,
                0 order_count,
                0 order_num,
                0 order_activity_count,
                0 order_coupon_count,
                0 order_activity_reduce_amount,
                0 order_coupon_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_num,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_num,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_num,
                0 refund_payment_amount,
                cart_count,
                favor_count,
                0 appraise_good_count,
                0 appraise_mid_count,
                0 appraise_bad_count,
                0 appraise_default_count
         from tmp_cf
         union all
         select dt,
                sku_id,
                0 order_count,
                0 order_num,
                0 order_activity_count,
                0 order_coupon_count,
                0 order_activity_reduce_amount,
                0 order_coupon_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_num,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_num,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_num,
                0 refund_payment_amount,
                0 cart_count,
                0 favor_count,
                appraise_good_count,
                appraise_mid_count,
                appraise_bad_count,
                appraise_default_count
         from tmp_comment
     ) t1
group by dt, sku_id;


--每日装载
with tmp_order as
         (
             select sku_id,
                    count(*)                                 order_count,
                    sum(sku_num)                             order_num,
                    sum(if(split_activity_amount > 0, 1, 0)) order_activity_count,
                    sum(if(split_coupon_amount > 0, 1, 0))   order_coupon_count,
                    sum(split_activity_amount)               order_activity_reduce_amount,
                    sum(split_coupon_amount)                 order_coupon_reduce_amount,
                    sum(original_amount)                     order_original_amount,
                    sum(split_final_amount)                  order_final_amount
             from dwd_order_detail
             where dt = '2021-10-14'
             group by sku_id
         ),
     tmp_pay as
         (
             select sku_id,
                    count(*)                payment_count,
                    sum(sku_num)            payment_num,
                    sum(split_final_amount) payment_amount
             from dwd_order_detail
             where (dt = '2021-10-14'
                 or dt = date_add('2021-10-14', -1))
               and order_id in
                   (
                       select order_id
                       from dwd_payment_info
                       where dt = '2021-10-14'
                   )
             group by sku_id
         ),
     tmp_ri as
         (
             select sku_id,
                    count(*)           refund_order_count,
                    sum(refund_num)    refund_order_num,
                    sum(refund_amount) refund_order_amount
             from dwd_order_refund_info
             where dt = '2021-10-14'
             group by sku_id
         ),
     tmp_rp as
         (
             select rp.sku_id,
                    count(*)           refund_payment_count,
                    sum(ri.refund_num) refund_payment_num,
                    sum(refund_amount) refund_payment_amount
             from (
                      select order_id,
                             sku_id,
                             refund_amount
                      from dwd_refund_payment
                      where dt = '2021-10-14'
                  ) rp
                      left join
                  (
                      select order_id,
                             sku_id,
                             refund_num
                      from dwd_order_refund_info
                      where dt >= date_add('2021-10-14', -15)
                  ) ri
                  on rp.order_id = ri.order_id
                      and rp.sku_id = ri.sku_id
             group by rp.sku_id
         ),
     tmp_cf as
         (
             select item                                   sku_id,
                    sum(if(action_id = 'cart_add', 1, 0))  cart_count,
                    sum(if(action_id = 'favor_add', 1, 0)) favor_count
             from dwd_action_log
             where dt = '2021-10-14'
               and action_id in ('cart_add', 'favor_add')
             group by item
         ),
     tmp_comment as
         (
             select sku_id,
                    sum(if(appraise = '1201', 1, 0)) appraise_good_count,
                    sum(if(appraise = '1202', 1, 0)) appraise_mid_count,
                    sum(if(appraise = '1203', 1, 0)) appraise_bad_count,
                    sum(if(appraise = '1204', 1, 0)) appraise_default_count
             from dwd_comment_info
             where dt = '2021-10-14'
             group by sku_id
         )
insert
overwrite
table
dws_sku_action_daycount
partition
(
dt = '2021-10-14'
)
select sku_id,
       sum(order_count),
       sum(order_num),
       sum(order_activity_count),
       sum(order_coupon_count),
       sum(order_activity_reduce_amount),
       sum(order_coupon_reduce_amount),
       sum(order_original_amount),
       sum(order_final_amount),
       sum(payment_count),
       sum(payment_num),
       sum(payment_amount),
       sum(refund_order_count),
       sum(refund_order_num),
       sum(refund_order_amount),
       sum(refund_payment_count),
       sum(refund_payment_num),
       sum(refund_payment_amount),
       sum(cart_count),
       sum(favor_count),
       sum(appraise_good_count),
       sum(appraise_mid_count),
       sum(appraise_bad_count),
       sum(appraise_default_count)
from (
         select sku_id,
                order_count,
                order_num,
                order_activity_count,
                order_coupon_count,
                order_activity_reduce_amount,
                order_coupon_reduce_amount,
                order_original_amount,
                order_final_amount,
                0 payment_count,
                0 payment_num,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_num,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_num,
                0 refund_payment_amount,
                0 cart_count,
                0 favor_count,
                0 appraise_good_count,
                0 appraise_mid_count,
                0 appraise_bad_count,
                0 appraise_default_count
         from tmp_order
         union all
         select sku_id,
                0 order_count,
                0 order_num,
                0 order_activity_count,
                0 order_coupon_count,
                0 order_activity_reduce_amount,
                0 order_coupon_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                payment_count,
                payment_num,
                payment_amount,
                0 refund_order_count,
                0 refund_order_num,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_num,
                0 refund_payment_amount,
                0 cart_count,
                0 favor_count,
                0 appraise_good_count,
                0 appraise_mid_count,
                0 appraise_bad_count,
                0 appraise_default_count
         from tmp_pay
         union all
         select sku_id,
                0 order_count,
                0 order_num,
                0 order_activity_count,
                0 order_coupon_count,
                0 order_activity_reduce_amount,
                0 order_coupon_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_num,
                0 payment_amount,
                refund_order_count,
                refund_order_num,
                refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_num,
                0 refund_payment_amount,
                0 cart_count,
                0 favor_count,
                0 appraise_good_count,
                0 appraise_mid_count,
                0 appraise_bad_count,
                0 appraise_default_count
         from tmp_ri
         union all
         select sku_id,
                0 order_count,
                0 order_num,
                0 order_activity_count,
                0 order_coupon_count,
                0 order_activity_reduce_amount,
                0 order_coupon_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_num,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_num,
                0 refund_order_amount,
                refund_payment_count,
                refund_payment_num,
                refund_payment_amount,
                0 cart_count,
                0 favor_count,
                0 appraise_good_count,
                0 appraise_mid_count,
                0 appraise_bad_count,
                0 appraise_default_count
         from tmp_rp
         union all
         select sku_id,
                0 order_count,
                0 order_num,
                0 order_activity_count,
                0 order_coupon_count,
                0 order_activity_reduce_amount,
                0 order_coupon_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_num,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_num,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_num,
                0 refund_payment_amount,
                cart_count,
                favor_count,
                0 appraise_good_count,
                0 appraise_mid_count,
                0 appraise_bad_count,
                0 appraise_default_count
         from tmp_cf
         union all
         select sku_id,
                0 order_count,
                0 order_num,
                0 order_activity_count,
                0 order_coupon_count,
                0 order_activity_reduce_amount,
                0 order_coupon_reduce_amount,
                0 order_original_amount,
                0 order_final_amount,
                0 payment_count,
                0 payment_num,
                0 payment_amount,
                0 refund_order_count,
                0 refund_order_num,
                0 refund_order_amount,
                0 refund_payment_count,
                0 refund_payment_num,
                0 refund_payment_amount,
                0 cart_count,
                0 favor_count,
                appraise_good_count,
                appraise_mid_count,
                appraise_bad_count,
                appraise_default_count
         from tmp_comment
     ) t1
group by sku_id;



