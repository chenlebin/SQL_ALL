-- todo ORC文件索引
--在使用ORC文件时，为了加快读取ORC文件中的数据内容，ORC提供了两种索引机制：
-- Row Group Index 行组索引
-- Bloom Filter Index 布姆索引
--可以帮助提高查询ORC文件的性能

--当用户创建表时，可以指定构建索引，当用户查询数据时，
--可以根据索引提前对数据进行过滤，避免不必要的数据扫描。

--todo Row Group Index  行组索引   ><= 时启用索引
--一个ORC文件包含一个或多个stripes(groups of row data)，每个stripe中包含了每个column的min/max值的索引数据；
--当查询中有大于等于小于的操作时，会根据min/max值，跳过扫描不包含的stripes。
--而其中为每个stripe建立的包含min/max值的索引，就称为Row Group Index行组索引，
--也叫min-max Index大小对比索引，或者Storage Index。

--建立ORC格式表时，指定表参数"orc.create.index""="true"之后，便会建立Row Group Index；
--为了使Row Group Index有效利用，向表中加载数据时，必须对需要使用索引的字段进行排序
use db_df2;
--1、开启索引配置
set hive.optimize.index.filter=true;
--2、创建表并制定构建行组索引
create table tb_sogou_orc_index
    stored as orc tblproperties ("orc.create.index" = "true")
as
select *
from tb_sogou_source
    distribute by stime
    sort by stime;
--todo 3、当进行范围或者等值查询（<,>,=）时就可以基于构建的索引进行查询
select count(*)
from tb_sogou_orc_index
where stime > '12:00:00'
  and stime < '18:00:00';



--todo Bloom Filter Index  布隆索引  = 时启用索引
--建表时候通过表参数"orc.bloom.filter.columns"="columnName……"来指定为哪些字段建立BloomFilter索引，
--在生成数据的时候，会在每个stripe中，为该字段建立BloomFilter的数据结构；
--当查询条件中包含对该字段的等值过滤时候，先从BloomFilter中获取以下是否包含该值，如果不包含，则跳过该stripe。

--创建表指定创建布隆索引
create table tb_sogou_orc_bloom
    stored as orc tblproperties ("orc.create.index" = "true","orc.bloom.filter.columns" = "stime,userid")
as
select *
from tb_sogou_source
    distribute by stime
    sort by stime;

--todo stime的范围过滤可以走row group index，userid的过滤可以走bloom filter index
select count(*)
from tb_sogou_orc_index
where stime > '12:00:00'
  and stime < '18:00:00'
  and userid = '3933365481995287';