--收藏事实表（周期型快照事实表，每日快照）
--收藏事实表每条数据代表的是一个用户对一个商品的一次收藏，它相关的维度有时间、用户、商品，度量值是次数，
--不过表中的字段没有次数这个字段，次数是通过相同用户的同一个商品在这张表中数据的条数推断出来的，
--一个用户同一商品写入的条数有几条就说明对这件商品收藏了几次。


DROP TABLE IF EXISTS dwd_favor_info;
CREATE EXTERNAL TABLE dwd_favor_info
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户id',
    `sku_id`      STRING COMMENT 'skuid',
    `spu_id`      STRING COMMENT 'spuid',
    --维度外键
    `is_cancel`   STRING COMMENT '是否取消',
    --是否取消是维度字段进行了维度退化
    `create_time` STRING COMMENT '收藏时间',
    --收藏时间是时间维度的外键
    --这里没有直接指明度量值，每条数据就代表的是收藏一次（不过可能是不同用户）
    `cancel_time` STRING COMMENT '取消时间'
) COMMENT '收藏事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_favor_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");


---分区情况及数据走向和数据装载
--分区情况，按天分区，每个分区存储的是一个快照也就是这一天的全量收藏数据，使用全量同步。
--数据装载，因为是进行全量同步，并且每个分区存这一天的全量收藏数据，所以每日装载和首日装载的逻辑是一样的
--就是在ODS层中找到对应的表过滤出dt=同步时间的数据，查询出事实表中所需的字段然后insert到事实表并使用静态分区指定
--dt=同步时间。

insert overwrite table dwd_favor_info partition (dt = '2021-10-13')
select id,
       user_id,
       sku_id,
       spu_id,
       is_cancel,
       create_time,
       cancel_time
from ods_favor_info
where dt = '2021-10-13';

