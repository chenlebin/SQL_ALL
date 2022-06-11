--品牌复购率是指一段时间内重复购买某品牌的人数与购买过该品牌的人数的比值。重复购买即购买次数大于等于2，购买过即购买次数大于1。
--此处要求统计最近1,7,30天的各品牌复购率。
--建表语句
DROP TABLE IF EXISTS ads_repeat_purchase;
CREATE EXTERNAL TABLE `ads_repeat_purchase`
(
    `dt`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`             STRING COMMENT '品牌ID',
    `tm_name`           STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
) COMMENT '品牌复购率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_repeat_purchase/';



--数据装载
--思路分析：该需求可分两步实现：
--第一步：统计每个用户购买每个品牌的次数。
--第二步：分别统计购买次数大于1的人数和大于2的人数。
insert overwrite table ads_repeat_purchase
select *
from ads_repeat_purchase
union
select '2020-06-14' dt,
       recent_days,
       tm_id,
       tm_name,
       cast(sum(if(order_count >= 2, 1, 0)) / sum(if(order_count >= 1, 1, 0)) * 100 as decimal(16, 2))
from (
         select recent_days,
                user_id,
                tm_id,
                tm_name,
                sum(order_count) order_count
         from (
                  select recent_days,
                         user_id,
                         sku_id,
                         count(*) order_count
                  from dwd_order_detail lateral view explode(Array(1, 7, 30)) tmp as recent_days
                  where dt >= date_add('2020-06-14', -29)
                    and dt >= date_add('2020-06-14', -recent_days + 1)
                  group by recent_days, user_id, sku_id
              ) t1
                  left join
              (
                  select id,
                         tm_id,
                         tm_name
                  from dim_sku_info
                  where dt = '2020-06-14'
              ) t2
              on t1.sku_id = t2.id
         group by recent_days, user_id, tm_id, tm_name
     ) t3
group by recent_days, tm_id, tm_name;
