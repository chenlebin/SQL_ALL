--建表语句
DROP TABLE IF EXISTS ads_user_total;
CREATE EXTERNAL TABLE `ads_user_total`
(
    `dt`                   STRING COMMENT '统计日期',
    `recent_days`          BIGINT COMMENT '最近天数,0:累积值,1:最近1天,7:最近7天,30:最近30天',
    `new_user_count`       BIGINT COMMENT '新注册用户数',
    `new_order_user_count` BIGINT COMMENT '新增下单用户数',
    `order_final_amount`   DECIMAL(16, 2) COMMENT '下单总金额',
    `order_user_count`     BIGINT COMMENT '下单用户数',
    `no_order_user_count`  BIGINT COMMENT '未下单用户数(具体指活跃用户中未下单用户)'
) COMMENT '用户统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_total/';


--数据装载
insert overwrite table ads_user_total
select *
from ads_user_total
union
select '2020-06-14',
       recent_days,
       sum(if(login_date_first >= recent_days_ago, 1, 0))                           new_user_count,
       sum(if(order_date_first >= recent_days_ago, 1, 0))                           new_order_user_count,
       sum(order_final_amount)                                                      order_final_amount,
       sum(if(order_final_amount > 0, 1, 0))                                        order_user_count,
       sum(if(login_date_last >= recent_days_ago and order_final_amount = 0, 1, 0)) no_order_user_count
from (
         select recent_days,
                user_id,
                login_date_first,
                login_date_last,
                order_date_first,
                case
                    when recent_days = 0 then order_final_amount
                    when recent_days = 1 then order_last_1d_final_amount
                    when recent_days = 7 then order_last_7d_final_amount
                    when recent_days = 30 then order_last_30d_final_amount
                    end                                                                     order_final_amount,
                if(recent_days = 0, '1970-01-01', date_add('2020-06-14', -recent_days + 1)) recent_days_ago
         from dwt_user_topic lateral view explode(Array(0, 1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
     ) t1
group by recent_days;
