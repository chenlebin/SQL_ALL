--漏斗分析是一个数据分析模型，它能够科学反映一个业务过程从起点到终点各阶段用户转化情况。由于其能将各阶段环节都展示出来，故哪个阶段存在问题，就能一目了然。
--该需求要求统计一个完整的购物流程各个阶段的人数。


--建表语句
DROP TABLE IF EXISTS ads_user_action;
CREATE EXTERNAL TABLE `ads_user_action`
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加入购物车人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
) COMMENT '漏斗分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_action/';


--数据装载
with tmp_page as
         (
             select '2020-06-14'                                        dt,
                    recent_days,
                    sum(if(array_contains(pages, 'home'), 1, 0))        home_count,
                    sum(if(array_contains(pages, 'good_detail'), 1, 0)) good_detail_count
             from (
                      select recent_days,
                             mid_id,
                             collect_set(page_id) pages
                      from (
                               select dt,
                                      mid_id,
                                      page.page_id
                               from dws_visitor_action_daycount lateral view explode(page_stats) tmp as page
                               where dt >= date_add('2020-06-14', -29)
                                 and page.page_id in ('home', 'good_detail')
                           ) t1 lateral view explode(Array(1, 7, 30)) tmp as recent_days
                      where dt >= date_add('2020-06-14', -recent_days + 1)
                      group by recent_days, mid_id
                  ) t2
             group by recent_days
         ),
     tmp_cop as
         (
             select '2020-06-14'                     dt,
                    recent_days,
                    sum(if(cart_count > 0, 1, 0))    cart_count,
                    sum(if(order_count > 0, 1, 0))   order_count,
                    sum(if(payment_count > 0, 1, 0)) payment_count
             from (
                      select recent_days,
                             user_id,
                             case
                                 when recent_days = 1 then cart_last_1d_count
                                 when recent_days = 7 then cart_last_7d_count
                                 when recent_days = 30 then cart_last_30d_count
                                 end cart_count,
                             case
                                 when recent_days = 1 then order_last_1d_count
                                 when recent_days = 7 then order_last_7d_count
                                 when recent_days = 30 then order_last_30d_count
                                 end order_count,
                             case
                                 when recent_days = 1 then payment_last_1d_count
                                 when recent_days = 7 then payment_last_7d_count
                                 when recent_days = 30 then payment_last_30d_count
                                 end payment_count
                      from dwt_user_topic lateral view explode(Array(1, 7, 30)) tmp as recent_days
                      where dt = '2020-06-14'
                  ) t1
             group by recent_days
         )
insert
overwrite
table
ads_user_action
select *
from ads_user_action
union
select tmp_page.dt,
       tmp_page.recent_days,
       home_count,
       good_detail_count,
       cart_count,
       order_count,
       payment_count
from tmp_page
         join tmp_cop
              on tmp_page.recent_days = tmp_cop.recent_days;
