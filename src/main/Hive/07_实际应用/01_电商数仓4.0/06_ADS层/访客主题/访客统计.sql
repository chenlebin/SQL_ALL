--建表语句
DROP TABLE IF EXISTS ads_visit_stats;
CREATE EXTERNAL TABLE ads_visit_stats
(
    `dt`               STRING COMMENT '统计日期',
    `is_new`           STRING COMMENT '新老标识,1:新,0:老',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          STRING COMMENT '渠道',
    `uv_count`         BIGINT COMMENT '日活(访问人数)',
    `duration_sec`     BIGINT COMMENT '页面停留总时长',
    `avg_duration_sec` BIGINT COMMENT '一次会话，页面停留平均时长,单位为描述',
    `page_count`       BIGINT COMMENT '页面总浏览数',
    `avg_page_count`   BIGINT COMMENT '一次会话，页面平均浏览数',
    `sv_count`         BIGINT COMMENT '会话次数',
    `bounce_count`     BIGINT COMMENT '跳出数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
) COMMENT '访客统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_visit_stats/';
--数据装载
--思路分析：该需求的关键点为会话的划分，总体实现思路可分为以下几步：
--第一步：对所有页面访问记录进行会话的划分。
--第二步：统计每个会话的浏览时长和浏览页面数。
--第三步：统计上述各指标。
insert overwrite table ads_visit_stats
select *
from ads_visit_stats
union
select '2020-06-14'                                                           dt,
       is_new,
       recent_days,
       channel,
       count(distinct (mid_id))                                               uv_count,
       cast(sum(duration) / 1000 as bigint)                                   duration_sec,
       cast(avg(duration) / 1000 as bigint)                                   avg_duration_sec,
       sum(page_count)                                                        page_count,
       cast(avg(page_count) as bigint)                                        avg_page_count,
       count(*)                                                               sv_count,
       sum(if(page_count = 1, 1, 0))                                          bounce_count,
       cast(sum(if(page_count = 1, 1, 0)) / count(*) * 100 as decimal(16, 2)) bounce_rate
from (
         select session_id,
                mid_id,
                is_new,
                recent_days,
                channel,
                count(*)         page_count,
                sum(during_time) duration
         from (
                  select mid_id,
                         channel,
                         recent_days,
                         is_new,
                         last_page_id,
                         page_id,
                         during_time,
                         concat(mid_id, '-', last_value(if(last_page_id is null, ts, null), true)
                                                        over (partition by recent_days,mid_id order by ts)) session_id
                  from (
                           select mid_id,
                                  channel,
                                  last_page_id,
                                  page_id,
                                  during_time,
                                  ts,
                                  recent_days,
                                  if(visit_date_first >= date_add('2020-06-14', -recent_days + 1), '1', '0') is_new
                           from (
                                    select t1.mid_id,
                                           t1.channel,
                                           t1.last_page_id,
                                           t1.page_id,
                                           t1.during_time,
                                           t1.dt,
                                           t1.ts,
                                           t2.visit_date_first
                                    from (
                                             select mid_id,
                                                    channel,
                                                    last_page_id,
                                                    page_id,
                                                    during_time,
                                                    dt,
                                                    ts
                                             from dwd_page_log
                                             where dt >= date_add('2020-06-14', -30)
                                         ) t1
                                             left join
                                         (
                                             select mid_id,
                                                    visit_date_first
                                             from dwt_visitor_topic
                                             where dt = '2020-06-14'
                                         ) t2
                                         on t1.mid_id = t2.mid_id
                                ) t3 lateral view explode(Array(1, 7, 30)) tmp as recent_days
                           where dt >= date_add('2020-06-14', -recent_days + 1)
                       ) t4
              ) t5
         group by session_id, mid_id, is_new, recent_days, channel
     ) t6
group by is_new, recent_days, channel;
