--访客主题宽表
--每条数据代表一个访客在一天之内的汇总行为。比较特殊没有相关的事实表，


--建表语句
DROP TABLE IF EXISTS dws_visitor_action_daycount;
CREATE EXTERNAL TABLE dws_visitor_action_daycount
(
    `mid_id`       STRING COMMENT '设备id',
    --主键
    `brand`        STRING COMMENT '设备品牌',
    `model`        STRING COMMENT '设备型号',
    `is_new`       STRING COMMENT '是否首次访问',
    `channel`      ARRAY<STRING> COMMENT '渠道',
    `os`           ARRAY<STRING> COMMENT '操作系统',
    `area_code`    ARRAY<STRING> COMMENT '地区ID',
    `version_code` ARRAY<STRING> COMMENT '应用版本',
    --以上均是维度字段，没有建立专门的维度表，所以只好维度退化到宽表中
    `visit_count`  BIGINT COMMENT '访问次数',
    `page_stats`   ARRAY<STRUCT<page_id:STRING,page_count:BIGINT,during_time:BIGINT>> COMMENT '页面访问统计'
    --需要聚合度量值
) COMMENT '每日设备行为表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_visitor_action_daycount'
    TBLPROPERTIES ("parquet.compression" = "lzo");


---分区情况及数据走向和数据装载
--分区上仍然是按天分区，每个分区中存储的是所有活跃访客用户一天之中所有行为的汇总数据。从日志事实表中选取字段
--进行聚合操作，之后将两个表中进行join选取所需字段插入到访客宽表中，然后进行静态分区，手动指定分区，dt=同步时间。

--数据装载
insert overwrite table dws_visitor_action_daycount partition (dt = '2021-10-13')
select t1.mid_id,
       t1.brand,
       t1.model,
       t1.is_new,
       t1.channel,
       t1.os,
       t1.area_code,
       t1.version_code,
       t1.visit_count,
       t3.page_stats
from (
         select mid_id,
                brand,
                model,
                if(array_contains(collect_set(is_new), '0'), '0', '1') is_new,--ods_page_log中，同一天内，同一设备的is_new字段，可能全部为1，可能全部为0，也可能部分为0，部分为1(卸载重装),故做该处理
                collect_set(channel)                                   channel,
                collect_set(os)                                        os,
                collect_set(area_code)                                 area_code,
                collect_set(version_code)                              version_code,
                sum(if(last_page_id is null, 1, 0))                    visit_count
         from dwd_page_log
         where dt = '2021-10-13'
           and last_page_id is null
         group by mid_id, model, brand
     ) t1
         join
     (
         select mid_id,
                brand,
                model,
                collect_set(named_struct('page_id', page_id, 'page_count', page_count, 'during_time',
                                         during_time)) page_stats
         from (
                  select mid_id,
                         brand,
                         model,
                         page_id,
                         count(*)         page_count,
                         sum(during_time) during_time
                  from dwd_page_log
                  where dt = '2021-10-13'
                  group by mid_id, model, brand, page_id
              ) t2
         group by mid_id, model, brand
     ) t3
     on t1.mid_id = t3.mid_id
         and t1.brand = t3.brand
         and t1.model = t3.model;
