--该指标为商品综合统计，包含每个spu被下单总次数和被下单总金额。
--建表语句
DROP TABLE IF EXISTS ads_order_spu_stats;
CREATE EXTERNAL TABLE `ads_order_spu_stats`
(
    `dt`             STRING COMMENT '统计日期',
    `recent_days`    BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `spu_id`         STRING COMMENT '商品ID',
    `spu_name`       STRING COMMENT '商品名称',
    `tm_id`          STRING COMMENT '品牌ID',
    `tm_name`        STRING COMMENT '品牌名称',
    `category3_id`   STRING COMMENT '三级品类ID',
    `category3_name` STRING COMMENT '三级品类名称',
    `category2_id`   STRING COMMENT '二级品类ID',
    `category2_name` STRING COMMENT '二级品类名称',
    `category1_id`   STRING COMMENT '一级品类ID',
    `category1_name` STRING COMMENT '一级品类名称',
    `order_count`    BIGINT COMMENT '订单数',
    `order_amount`   DECIMAL(16, 2) COMMENT '订单金额'
) COMMENT '商品销售统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_spu_stats/';


--数据装载
insert overwrite table ads_order_spu_stats
select *
from ads_order_spu_stats
union
select '2020-06-14' dt,
       recent_days,
       spu_id,
       spu_name,
       tm_id,
       tm_name,
       category3_id,
       category3_name,
       category2_id,
       category2_name,
       category1_id,
       category1_name,
       sum(order_count),
       sum(order_amount)
from (
         select recent_days,
                sku_id,
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
         from dwt_sku_topic lateral view explode(Array(1, 7, 30)) tmp as recent_days
         where dt = '2020-06-14'
     ) t1
         left join
     (
         select id,
                spu_id,
                spu_name,
                tm_id,
                tm_name,
                category3_id,
                category3_name,
                category2_id,
                category2_name,
                category1_id,
                category1_name
         from dim_sku_info
         where dt = '2020-06-14'
     ) t2
     on t1.sku_id = t2.id
group by recent_days, spu_id, spu_name, tm_id, tm_name, category3_id, category3_name, category2_id, category2_name,
         category1_id, category1_name;
