DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE `ads_user_change`
(
    `dt`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_user_change/';



--数据装载
--思路分析：
--流失用户：末次活跃时间为7日前的用户即为流失用户。
--回流用户：末次活跃时间为今日，上次活跃时间在8日前的用户即为回流用户。
insert overwrite table ads_user_change
select *
from ads_user_change
union
select churn.dt,
       user_churn_count,
       user_back_count
from (
         select '2020-06-14' dt,
                count(*)     user_churn_count
         from dwt_user_topic
         where dt = '2020-06-14'
           and login_date_last = date_add('2020-06-14', -7)
     ) churn
         join
     (
         select '2020-06-14' dt,
                count(*)     user_back_count
         from (
                  select user_id,
                         login_date_last
                  from dwt_user_topic
                  where dt = '2020-06-14'
                    and login_date_last = '2020-06-14'
              ) t1
                  join
              (
                  select user_id,
                         login_date_last login_date_previous
                  from dwt_user_topic
                  where dt = date_add('2020-06-14', -1)
              ) t2
              on t1.user_id = t2.user_id
         where datediff(login_date_last, login_date_previous) >= 8
     ) back
     on churn.dt = back.dt;
