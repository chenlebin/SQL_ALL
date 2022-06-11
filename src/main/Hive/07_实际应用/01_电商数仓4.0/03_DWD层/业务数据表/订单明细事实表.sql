/*DIM，DWD采用维度建模，维度建模模型详情如下
注释：√代表事实表和维度表之间有关联关系也就意味着这张事实表存在这张维度表的维度外键
		时间	用户	地区	商品	优惠券	活动				度量值
订单	√	     √	     √										运费/优惠金额/原始金额/最终金额
订单详情√	     √	     √	     √	      √	     √				件数/优惠金额/原始金额/最终金额
支付	√	     √	     √										支付金额
加购	√	     √		         √								件数/金额
收藏	√	     √		         √								次数
评价	√	     √		         √								次数
退单	√	     √	     √	     √								件数/金额
退款	√	     √	     √	     √								件数/金额
优惠券领用√	     √			              √						次数
通过维度建模模型我们可以得到事实表的维度外键和度量值，而每一行的数据代表的就是这一个业务的一条信息。
维度表我们可以参考业务数据库中的表的字段，然后从ODS层中相关维度的表进行Join之后抽取出所需的字段，形成一张
只有该维度信息的维度表。一个维度就需要包含业务数据库表中的相关维度的所有字段，
每一行数据代表的就是这一个维度的详细描述信息。
*/
--订单明细事实表（事务型事实表）
--订单明细事实表中每条数据代表一个订单中的一个商品的信息，一个订单最终可能有多个商品。也就是订单事实表还可以和
--订单明细事实表进行关联。订单明细表相关的维度，如上面的维度建模模型所示，有时间、用户、地区、商品、优惠券、活动
--维度，所以包含这些维度外键，相关的度量值有件数/优惠金额/原始金额/最终金额，所以有优惠券分摊、活动优惠分摊、最终价格分摊
--和原始价格、商品数量这几个度量值字段，还有两个特殊字段是来源类型和来源编号，这两个字段准确来说其实既不是度量值
--也不是维度外键，他们都是维度表中的字段，但是由于来源维度的字段太少，所以直接将两个维度字段加入到事实表中省去建
--一张维度表，这种操作叫做维度退化（退化的意思我的理解是从高粒度退化成了低粒度）。如下是订单详细事实表的建表语句。
DROP TABLE IF EXISTS dwd_order_detail;
CREATE EXTERNAL TABLE dwd_order_detail
(
    `id`                    STRING COMMENT '订单编号',
    `order_id`              STRING COMMENT '订单号',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT 'sku商品id',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '活动ID',
    `activity_rule_id`      STRING COMMENT '活动规则ID',
    `coupon_id`             STRING COMMENT '优惠券ID',
    `create_time`           STRING COMMENT '创建时间',
    --以上均是维度外键
    `source_type`           STRING COMMENT '来源类型',
    `source_id`             STRING COMMENT '来源编号',
    --这两个是维度字段，进行了维度退化
    `sku_num`               BIGINT COMMENT '商品数量',
    `original_amount`       DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_final_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
    --这5个是度量值字段
) COMMENT '订单明细事实表表'
    PARTITIONED BY (`dt` STRING)
--以下是parquet文件存储并进行Lzo压缩处理，外部表存储的路径
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_order_detail/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


--数据装载，joinODS层中订单详细事实表中关联的几张表，将这些表按ID进行Left join之后插入到订单详细事实表中
--首日装载，动态分区
insert overwrite table dwd_order_detail partition (dt)
select od.id,
       od.order_id,
       oi.user_id,
       od.sku_id,
       oi.province_id,
       oda.activity_id,
       oda.activity_rule_id,
       odc.coupon_id,
       od.create_time,
       od.source_type,
       od.source_id,
       od.sku_num,
       od.order_price * od.sku_num,
       od.split_activity_amount,
       od.split_coupon_amount,
       od.split_final_amount,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select *
         from ods_order_detail
         where dt = '2021-10-13'
     ) od
         left join
     (
         select id,
                user_id,
                province_id
         from ods_order_info
         where dt = '2021-10-13'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select order_detail_id,
                activity_id,
                activity_rule_id
         from ods_order_detail_activity
         where dt = '2021-10-13'
     ) oda
     on od.id = oda.order_detail_id
         left join
     (
         select order_detail_id,
                coupon_id
         from ods_order_detail_coupon
         where dt = '2021-10-13'
     ) odc
     on od.id = odc.order_detail_id;


--每日装载 静态分区
insert overwrite table dwd_order_detail partition (dt = '2021-10-14')
select od.id,
       od.order_id,
       oi.user_id,
       od.sku_id,
       oi.province_id,
       oda.activity_id,
       oda.activity_rule_id,
       odc.coupon_id,
       od.create_time,
       od.source_type,
       od.source_id,
       od.sku_num,
       od.order_price * od.sku_num,
       od.split_activity_amount,
       od.split_coupon_amount,
       od.split_final_amount
from (
         select *
         from ods_order_detail
         where dt = '2021-10-14'
     ) od
         left join
     (
         select id,
                user_id,
                province_id
         from ods_order_info
         where dt = '2021-10-14'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select order_detail_id,
                activity_id,
                activity_rule_id
         from ods_order_detail_activity
         where dt = '2021-10-14'
     ) oda
     on od.id = oda.order_detail_id
         left join
     (
         select order_detail_id,
                coupon_id
         from ods_order_detail_coupon
         where dt = '2021-10-14'
     ) odc
     on od.id = odc.order_detail_id;
