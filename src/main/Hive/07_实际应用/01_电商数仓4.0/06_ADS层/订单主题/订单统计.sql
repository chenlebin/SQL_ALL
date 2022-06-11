--该需求包含订单总数，订单总金额和下单总人数。


--建表语句
DROP TABLE IF EXISTS ads_order_total;
CREATE EXTERNAL TABLE `ads_order_total`
(
    `dt`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `order_count`      BIGINT COMMENT '订单数',
    `order_amount`     DECIMAL(16, 2) COMMENT '订单金额',
    `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '订单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_total/';


--数据装载
insert overwrite table ads_order_total
select *
from ads_order_total
union
select '2020-06-14',
       recent_days,
       sum(order_count),
       sum(order_final_amount)               order_final_amount,
       sum(if(order_final_amount > 0, 1, 0)) order_user_count
from (
         select recent_days,
                user_id,
                case
                    when recent_days = 0 then order_count
                    when recent_days = 1 then order_last_1d_count
                    when recent_days = 7 then order_last_7d_count
                    when recent_days = 30 then order_last_30d_count
                    end order_count,
                case
                    when recent_days = 0 then order_final_amount
                    when recent_days = 1 then order_last_1d_final_amount
                    when recent_days = 7 then order_last_7d_final_amount
                    when recent_days = 30 then order_last_30d_final_amount
                    end order_final_amount
         from dwt_user_topic lateral view explode(Array(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
     ) t1
group by recent_days;
