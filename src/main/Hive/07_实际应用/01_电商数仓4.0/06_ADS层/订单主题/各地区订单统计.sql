--该需求包含各省份订单总数和订单总金额。


--建表语句
DROP TABLE IF EXISTS ads_order_by_province;
CREATE EXTERNAL TABLE `ads_order_by_province`
(
    `dt`              STRING COMMENT '统计日期',
    `recent_days`     BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`     STRING COMMENT '省份ID',
    `province_name`   STRING COMMENT '省份名称',
    `area_code`       STRING COMMENT '地区编码',
    `iso_code`        STRING COMMENT '国际标准地区编码',
    `iso_code_3166_2` STRING COMMENT '国际标准地区编码',
    `order_count`     BIGINT COMMENT '订单数',
    `order_amount`    DECIMAL(16, 2) COMMENT '订单金额'
) COMMENT '各地区订单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_by_province/';


--数据装载
insert overwrite table ads_order_by_province
select *
from ads_order_by_province
union
select dt,
       recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count,
       order_amount
from (
         select '2020-06-14'      dt,
                recent_days,
                province_id,
                sum(order_count)  order_count,
                sum(order_amount) order_amount
         from (
                  select recent_days,
                         province_id,
                         case
                             when recent_days = 1 then order_last_1d_count
                             when recent_days = 7 then order_last_7d_count
                             when recent_days = 30 then order_last_30d_count
                             end order_count,
                         case
                             when recent_days = 1 then order_last_1d_final_amount
                             when recent_days = 7 then order_last_7d_final_amount
                             when recent_days = 30 then order_last_30d_final_amount
                             end order_amount
                  from dwt_area_topic lateral view explode(Array(1, 7, 30)) tmp as recent_days
                  where dt = '2020-06-14'
              ) t1
         group by recent_days, province_id
     ) t2
         join dim_base_province t3
              on t2.province_id = t3.id;
