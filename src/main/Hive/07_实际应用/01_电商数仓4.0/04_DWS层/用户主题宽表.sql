--DWS层建表
--服务层，建表的思想是宽表策略，主要存放的使用次数较为频繁，可能多次使用的数据，例如需要聚合的值，
--需要排序分组的字段等。DWS层按照主题将DIM层或DWD层中的数据进行按天聚合操作，DIM层中有几个维度表在DWS层
--中就有几个维度宽表，将计算数据保存起来，减少重复计算。分区按天进行分区，每行数据代表的是一个维度对象的
--当日汇总行为。字段主要有两种，一种是主键ID，一种是与维度相关的事实表中的度量值的聚合值。
--因为维度层当中有六个维度分别是用户、地区、商品、优惠券、活动、访客六个维度，这里只讲解其中一个维度层进行
--聚合操作形成DWS层的维度宽表。
--      用户主题宽表
--每行数据代表，一个用户在某天的汇总行为，字段主要包括主键ID和维度相关的事实表的度量值的聚合值。

DROP TABLE IF EXISTS dws_user_action_daycount;
CREATE EXTERNAL TABLE dws_user_action_daycount
(
    `user_id`                      STRING COMMENT '用户id',
    `login_count`                  BIGINT COMMENT '登录次数',
    `cart_count`                   BIGINT COMMENT '加入购物车次数',
    `favor_count`                  BIGINT COMMENT '收藏次数',
    `order_count`                  BIGINT COMMENT '下单次数',
    --加购事实表度量值
    `order_activity_count`         BIGINT COMMENT '订单参与活动次数',
    `order_activity_reduce_amount` DECIMAL(16, 2) COMMENT '订单减免金额(活动)',
    `order_coupon_count`           BIGINT COMMENT '订单用券次数',
    `order_coupon_reduce_amount`   DECIMAL(16, 2) COMMENT '订单减免金额(优惠券)',
    `order_original_amount`        DECIMAL(16, 2) COMMENT '订单单原始金额',
    `order_final_amount`           DECIMAL(16, 2) COMMENT '订单总金额',
    --订单和订单明细事实表的度量值
    `payment_count`                BIGINT COMMENT '支付次数',
    `payment_amount`               DECIMAL(16, 2) COMMENT '支付金额',
    --支付事实表的度量值
    `refund_order_count`           BIGINT COMMENT '退单次数',
    `refund_order_num`             BIGINT COMMENT '退单件数',
    `refund_order_amount`          DECIMAL(16, 2) COMMENT '退单金额',
    `refund_payment_count`         BIGINT COMMENT '退款次数',
    `refund_payment_num`           BIGINT COMMENT '退款件数',
    `refund_payment_amount`        DECIMAL(16, 2) COMMENT '退款金额',
    --退单事实表的度量值
    `coupon_get_count`             BIGINT COMMENT '优惠券领取次数',
    `coupon_using_count`           BIGINT COMMENT '优惠券使用(下单)次数',
    `coupon_used_count`            BIGINT COMMENT '优惠券使用(支付)次数',
    --优惠券事实表的度量值
    `appraise_good_count`          BIGINT COMMENT '好评数',
    `appraise_mid_count`           BIGINT COMMENT '中评数',
    `appraise_bad_count`           BIGINT COMMENT '差评数',
    `appraise_default_count`       BIGINT COMMENT '默认评价数',
    --评价事实表的度量值
    `order_detail_stats`           array<struct<sku_id :string,sku_num :bigint, order_count :bigint,activity_reduce_amount
                                                :decimal(16, 2), coupon_reduce_amount :decimal(16, 2),original_amount
                                                :decimal(16, 2), final_amount :decimal(16, 2)>> COMMENT '下单明细统计'
) COMMENT '每日用户行为'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_user_action_daycount/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


---数据装载
with tmp_login as
         (
             select dt,
                    user_id,
                    count(*) login_count
             from dwd_page_log
             where user_id is not null
               and last_page_id is null
             group by dt, user_id
         ),
     tmp_cf as
         (
             select dt,
                    user_id,
                    sum(if(action_id = 'cart_add', 1, 0))  cart_count,
                    sum(if(action_id = 'favor_add', 1, 0)) favor_count
             from dwd_action_log
             where user_id is not null
               and action_id in ('cart_add', 'favor_add')
             group by dt, user_id
         ),
     tmp_order as
         (
             select date_format(create_time, 'yyyy-MM-dd')    dt,
                    user_id,
                    count(*)                                  order_count,
                    sum(if(activity_reduce_amount > 0, 1, 0)) order_activity_count,
                    sum(if(coupon_reduce_amount > 0, 1, 0))   order_coupon_count,
                    sum(activity_reduce_amount)               order_activity_reduce_amount,
                    sum(coupon_reduce_amount)                 order_coupon_reduce_amount,
                    sum(original_amount)                      order_original_amount,
                    sum(final_amount)                         order_final_amount
             from dwd_order_info
             group by date_format(create_time, 'yyyy-MM-dd'), user_id
         ),
     tmp_pay as
         (
             select date_format(callback_time, 'yyyy-MM-dd') dt,
                    user_id,
                    count(*)                                 payment_count,
                    sum(payment_amount)                      payment_amount
             from dwd_payment_info
             group by date_format(callback_time, 'yyyy-MM-dd'), user_id
         ),
     tmp_ri as
         (
             select date_format(create_time, 'yyyy-MM-dd') dt,
                    user_id,
                    count(*)                               refund_order_count,
                    sum(refund_num)                        refund_order_num,
                    sum(refund_amount)                     refund_order_amount
             from dwd_order_refund_info
             group by date_format(create_time, 'yyyy-MM-dd'), user_id
         ),
     tmp_rp as
         (
             select date_format(callback_time, 'yyyy-MM-dd') dt,
                    rp.user_id,
                    count(*)                                 refund_payment_count,
                    sum(ri.refund_num)                       refund_payment_num,
                    sum(rp.refund_amount)                    refund_payment_amount
             from (
                      select user_id,
                             order_id,
                             sku_id,
                             refund_amount,
                             callback_time
                      from dwd_refund_payment
                  ) rp
                      left join
                  (
                      select user_id,
                             order_id,
                             sku_id,
                             refund_num
                      from dwd_order_refund_info
                  ) ri
                  on rp.order_id = ri.order_id
                      and rp.sku_id = rp.sku_id
             group by date_format(callback_time, 'yyyy-MM-dd'), rp.user_id
         ),
     tmp_coupon as
         (
             select coalesce(coupon_get.dt, coupon_using.dt, coupon_used.dt)                dt,
                    coalesce(coupon_get.user_id, coupon_using.user_id, coupon_used.user_id) user_id,
                    nvl(coupon_get_count, 0)                                                coupon_get_count,
                    nvl(coupon_using_count, 0)                                              coupon_using_count,
                    nvl(coupon_used_count, 0)                                               coupon_used_count
             from (
                      select date_format(get_time, 'yyyy-MM-dd') dt,
                             user_id,
                             count(*)                            coupon_get_count
                      from dwd_coupon_use
                      where get_time is not null
                      group by user_id, date_format(get_time, 'yyyy-MM-dd')
                  ) coupon_get
                      full outer join
                  (
                      select date_format(using_time, 'yyyy-MM-dd') dt,
                             user_id,
                             count(*)                              coupon_using_count
                      from dwd_coupon_use
                      where using_time is not null
                      group by user_id, date_format(using_time, 'yyyy-MM-dd')
                  ) coupon_using
                  on coupon_get.dt = coupon_using.dt
                      and coupon_get.user_id = coupon_using.user_id
                      full outer join
                  (
                      select date_format(used_time, 'yyyy-MM-dd') dt,
                             user_id,
                             count(*)                             coupon_used_count
                      from dwd_coupon_use
                      where used_time is not null
                      group by user_id, date_format(used_time, 'yyyy-MM-dd')
                  ) coupon_used
                  on nvl(coupon_get.dt, coupon_using.dt) = coupon_used.dt
                      and nvl(coupon_get.user_id, coupon_using.user_id) = coupon_used.user_id
         ),
     tmp_comment as
         (
             select date_format(create_time, 'yyyy-MM-dd') dt,
                    user_id,
                    sum(if(appraise = '1201', 1, 0))       appraise_good_count,
                    sum(if(appraise = '1202', 1, 0))       appraise_mid_count,
                    sum(if(appraise = '1203', 1, 0))       appraise_bad_count,
                    sum(if(appraise = '1204', 1, 0))       appraise_default_count
             from dwd_comment_info
             group by date_format(create_time, 'yyyy-MM-dd'), user_id
         ),
     tmp_od as
         (
             select dt,
                    user_id,
                    collect_set(named_struct('sku_id', sku_id, 'sku_num', sku_num, 'order_count', order_count,
                                             'activity_reduce_amount', activity_reduce_amount, 'coupon_reduce_amount',
                                             coupon_reduce_amount, 'original_amount', original_amount, 'final_amount',
                                             final_amount)) order_detail_stats
             from (
                      select date_format(create_time, 'yyyy-MM-dd')             dt,
                             user_id,
                             sku_id,
                             sum(sku_num)                                       sku_num,
                             count(*)                                           order_count,
                             cast(sum(split_activity_amount) as decimal(16, 2)) activity_reduce_amount,
                             cast(sum(split_coupon_amount) as decimal(16, 2))   coupon_reduce_amount,
                             cast(sum(original_amount) as decimal(16, 2))       original_amount,
                             cast(sum(split_final_amount) as decimal(16, 2))    final_amount
                      from dwd_order_detail
                      group by date_format(create_time, 'yyyy-MM-dd'), user_id, sku_id
                  ) t1
             group by dt, user_id
         )
insert
overwrite
table
dws_user_action_daycount
partition
(
dt
)
select coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id, tmp_ri.user_id, tmp_rp.user_id,
                tmp_comment.user_id, tmp_coupon.user_id, tmp_od.user_id),
       nvl(login_count, 0),
       nvl(cart_count, 0),
       nvl(favor_count, 0),
       nvl(order_count, 0),
       nvl(order_activity_count, 0),
       nvl(order_activity_reduce_amount, 0),
       nvl(order_coupon_count, 0),
       nvl(order_coupon_reduce_amount, 0),
       nvl(order_original_amount, 0),
       nvl(order_final_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_amount, 0),
       nvl(refund_order_count, 0),
       nvl(refund_order_num, 0),
       nvl(refund_order_amount, 0),
       nvl(refund_payment_count, 0),
       nvl(refund_payment_num, 0),
       nvl(refund_payment_amount, 0),
       nvl(coupon_get_count, 0),
       nvl(coupon_using_count, 0),
       nvl(coupon_used_count, 0),
       nvl(appraise_good_count, 0),
       nvl(appraise_mid_count, 0),
       nvl(appraise_bad_count, 0),
       nvl(appraise_default_count, 0),
       order_detail_stats,
       coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt, tmp_comment.dt, tmp_coupon.dt,
                tmp_od.dt)
from tmp_login
         full outer join tmp_cf
                         on tmp_login.user_id = tmp_cf.user_id
                             and tmp_login.dt = tmp_cf.dt
         full outer join tmp_order
                         on coalesce(tmp_login.user_id, tmp_cf.user_id) = tmp_order.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt) = tmp_order.dt
         full outer join tmp_pay
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id) = tmp_pay.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt) = tmp_pay.dt
         full outer join tmp_ri
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id) =
                            tmp_ri.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt) = tmp_ri.dt
         full outer join tmp_rp
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id) = tmp_rp.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt) = tmp_rp.dt
         full outer join tmp_comment
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id, tmp_rp.user_id) = tmp_comment.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt) =
                                 tmp_comment.dt
         full outer join tmp_coupon
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id) = tmp_coupon.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt,
                                          tmp_comment.dt) = tmp_coupon.dt
         full outer join tmp_od
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id, tmp_coupon.user_id) =
                            tmp_od.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt,
                                          tmp_comment.dt, tmp_coupon.dt) = tmp_od.dt;


--每日装载
with tmp_login as
         (
             select user_id,
                    count(*) login_count
             from dwd_page_log
             where dt = '2021-10-14'
               and user_id is not null
               and last_page_id is null
             group by user_id
         ),
     tmp_cf as
         (
             select user_id,
                    sum(if(action_id = 'cart_add', 1, 0))  cart_count,
                    sum(if(action_id = 'favor_add', 1, 0)) favor_count
             from dwd_action_log
             where dt = '2021-10-14'
               and user_id is not null
               and action_id in ('cart_add', 'favor_add')
             group by user_id
         ),
     tmp_order as
         (
             select user_id,
                    count(*)                                  order_count,
                    sum(if(activity_reduce_amount > 0, 1, 0)) order_activity_count,
                    sum(if(coupon_reduce_amount > 0, 1, 0))   order_coupon_count,
                    sum(activity_reduce_amount)               order_activity_reduce_amount,
                    sum(coupon_reduce_amount)                 order_coupon_reduce_amount,
                    sum(original_amount)                      order_original_amount,
                    sum(final_amount)                         order_final_amount
             from dwd_order_info
             where (dt = '2021-10-14'
                 or dt = '9999-99-99')
               and date_format(create_time, 'yyyy-MM-dd') = '2021-10-14'
             group by user_id
         ),
     tmp_pay as
         (
             select user_id,
                    count(*)            payment_count,
                    sum(payment_amount) payment_amount
             from dwd_payment_info
             where dt = '2021-10-14'
             group by user_id
         ),
     tmp_ri as
         (
             select user_id,
                    count(*)           refund_order_count,
                    sum(refund_num)    refund_order_num,
                    sum(refund_amount) refund_order_amount
             from dwd_order_refund_info
             where dt = '2021-10-14'
             group by user_id
         ),
     tmp_rp as
         (
             select rp.user_id,
                    count(*)              refund_payment_count,
                    sum(ri.refund_num)    refund_payment_num,
                    sum(rp.refund_amount) refund_payment_amount
             from (
                      select user_id,
                             order_id,
                             sku_id,
                             refund_amount
                      from dwd_refund_payment
                      where dt = '2021-10-14'
                  ) rp
                      left join
                  (
                      select user_id,
                             order_id,
                             sku_id,
                             refund_num
                      from dwd_order_refund_info
                      where dt >= date_add('2021-10-14', -15)
                  ) ri
                  on rp.order_id = ri.order_id
                      and rp.sku_id = rp.sku_id
             group by rp.user_id
         ),
     tmp_coupon as
         (
             select user_id,
                    sum(if(date_format(get_time, 'yyyy-MM-dd') = '2021-10-14', 1, 0))   coupon_get_count,
                    sum(if(date_format(using_time, 'yyyy-MM-dd') = '2021-10-14', 1, 0)) coupon_using_count,
                    sum(if(date_format(used_time, 'yyyy-MM-dd') = '2021-10-14', 1, 0))  coupon_used_count
             from dwd_coupon_use
             where (dt = '2021-10-14' or dt = '9999-99-99')
               and (date_format(get_time, 'yyyy-MM-dd') = '2021-10-14'
                 or date_format(using_time, 'yyyy-MM-dd') = '2021-10-14'
                 or date_format(used_time, 'yyyy-MM-dd') = '2021-10-14')
             group by user_id
         ),
     tmp_comment as
         (
             select user_id,
                    sum(if(appraise = '1201', 1, 0)) appraise_good_count,
                    sum(if(appraise = '1202', 1, 0)) appraise_mid_count,
                    sum(if(appraise = '1203', 1, 0)) appraise_bad_count,
                    sum(if(appraise = '1204', 1, 0)) appraise_default_count
             from dwd_comment_info
             where dt = '2021-10-14'
             group by user_id
         ),
     tmp_od as
         (
             select user_id,
                    collect_set(named_struct('sku_id', sku_id, 'sku_num', sku_num, 'order_count', order_count,
                                             'activity_reduce_amount', activity_reduce_amount, 'coupon_reduce_amount',
                                             coupon_reduce_amount, 'original_amount', original_amount, 'final_amount',
                                             final_amount)) order_detail_stats
             from (
                      select user_id,
                             sku_id,
                             sum(sku_num)                                       sku_num,
                             count(*)                                           order_count,
                             cast(sum(split_activity_amount) as decimal(16, 2)) activity_reduce_amount,
                             cast(sum(split_coupon_amount) as decimal(16, 2))   coupon_reduce_amount,
                             cast(sum(original_amount) as decimal(16, 2))       original_amount,
                             cast(sum(split_final_amount) as decimal(16, 2))    final_amount
                      from dwd_order_detail
                      where dt = '2021-10-14'
                      group by user_id, sku_id
                  ) t1
             group by user_id
         )
insert
overwrite
table
dws_user_action_daycount
partition
(
dt = '2021-10-14'
)
select coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id, tmp_ri.user_id, tmp_rp.user_id,
                tmp_comment.user_id, tmp_coupon.user_id, tmp_od.user_id),
       nvl(login_count, 0),
       nvl(cart_count, 0),
       nvl(favor_count, 0),
       nvl(order_count, 0),
       nvl(order_activity_count, 0),
       nvl(order_activity_reduce_amount, 0),
       nvl(order_coupon_count, 0),
       nvl(order_coupon_reduce_amount, 0),
       nvl(order_original_amount, 0),
       nvl(order_final_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_amount, 0),
       nvl(refund_order_count, 0),
       nvl(refund_order_num, 0),
       nvl(refund_order_amount, 0),
       nvl(refund_payment_count, 0),
       nvl(refund_payment_num, 0),
       nvl(refund_payment_amount, 0),
       nvl(coupon_get_count, 0),
       nvl(coupon_using_count, 0),
       nvl(coupon_used_count, 0),
       nvl(appraise_good_count, 0),
       nvl(appraise_mid_count, 0),
       nvl(appraise_bad_count, 0),
       nvl(appraise_default_count, 0),
       order_detail_stats
from tmp_login
         full outer join tmp_cf on tmp_login.user_id = tmp_cf.user_id
         full outer join tmp_order on coalesce(tmp_login.user_id, tmp_cf.user_id) = tmp_order.user_id
         full outer join tmp_pay on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id) = tmp_pay.user_id
         full outer join tmp_ri on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id) =
                                   tmp_ri.user_id
         full outer join tmp_rp on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                            tmp_ri.user_id) = tmp_rp.user_id
         full outer join tmp_comment on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                                 tmp_ri.user_id, tmp_rp.user_id) = tmp_comment.user_id
         full outer join tmp_coupon on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                                tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id) =
                                       tmp_coupon.user_id
         full outer join tmp_od on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                            tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id, tmp_coupon.user_id) =
                                   tmp_od.user_id;







