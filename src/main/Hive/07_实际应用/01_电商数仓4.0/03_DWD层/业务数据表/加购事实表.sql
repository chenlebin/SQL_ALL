--加购事实表（周期型快照事实表，每日快照）
--每行数据是一个用户一次购物车中的一个商品，加购事实表中关联的维度有时间、用户、商品维度，度量值有件数/金额
--就能确定加购事实表中的字段有哪些，如下是加购事实表的建表语句。

DROP TABLE IF EXISTS dwd_cart_info;
CREATE EXTERNAL TABLE dwd_cart_info
(
    `id`           STRING COMMENT '编号',
    `user_id`      STRING COMMENT '用户ID',
    `sku_id`       STRING COMMENT '商品ID',
    --维度外键，还有一个时间维度外键是创建时间
    `source_type`  STRING COMMENT '来源类型',
    `source_id`    STRING COMMENT '来源编号',
    --维度字段，进行了维度退化
    `cart_price`   DECIMAL(16, 2) COMMENT '加入购物车时的价格',
    `is_ordered`   STRING COMMENT '是否已下单',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `order_time`   STRING COMMENT '下单时间',
    `sku_num`      BIGINT COMMENT '加购数量'
    ---度量值
) COMMENT '加购事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_cart_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

---分区情况及数据走向和数据装载
--按天分区，每个分区存储的是一个快照也就是这一天的全量购物车数据，使用全量同步。
--数据装载也相对简单，每日装载和首日装载因为均是全量同步所以都是查询出ODS层中相关表对应分区即dt=同步时间
--的数据之后插入到加购事实表，然后指定分区进行静态分区。
--首日装载和每日装载思路一致只需要改时间dt即可
insert overwrite table dwd_cart_info partition (dt = '2021-10-13')
select id,
       user_id,
       sku_id,
       source_type,
       source_id,
       cart_price,
       is_ordered,
       create_time,
       operate_time,
       order_time,
       sku_num
from ods_cart_info
where dt = '2021-10-13';