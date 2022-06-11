--建表语句
DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt`          STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source`      STRING COMMENT '跳转起始页面ID',
    `target`      STRING COMMENT '跳转终到页面ID',
    `path_count`  BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_page_path/';


--数据装载
insert overwrite table ads_page_path
select *
from ads_page_path
union
select '2021-10-13',
       recent_days,
       source,
       target,
       count(*)
from (
         select recent_days,
                concat('step-', step, ':', source)     source,
                concat('step-', step + 1, ':', target) target
         from (
                  select recent_days,
                         page_id                                                                       source,
                         lead(page_id, 1, null) over (partition by recent_days,session_id order by ts) target,
                         row_number() over (partition by recent_days,session_id order by ts)           step
                  from (
                           select recent_days,
                                  last_page_id,
                                  page_id,
                                  ts,
                                  concat(mid_id, '-', last_value(if(last_page_id is null, ts, null), true)
                                                                 over (partition by mid_id,recent_days order by ts)) session_id
                           from dwd_page_log lateral view explode(Array(1, 7, 30)) tmp as recent_days
                           where dt >= date_add('2021-10-13', -30)
                             and dt >= date_add('2021-10-13', -recent_days + 1)
                       ) t2
              ) t3
     ) t4
group by recent_days, source, target;
