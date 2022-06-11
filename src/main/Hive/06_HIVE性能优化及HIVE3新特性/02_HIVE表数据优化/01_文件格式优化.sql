-----------------------2、Hive表数据优化-----------------------------------
--todo 文件格式
-- 插入数据时要使用insert+select的方式将数据插入否则无法完成数据的格式转换，load加载时只是复制/移动
--主流的文件格式：TextFile SequenceFile Parquet ORC
-- 创建原始数据表
-- todo TextFile 适用于小数据的存储，一般用于第一层的数据存储，或者用于测试
use db_df2;
create table tb_sogou_source
(
    stime      string,
    userid     string,
    keyword    string,
    clickorder string,
    url        string
)
    row format delimited fields terminated by '\t';

load data local inpath '/root/hivedata/SogouQ.reduced' into table tb_sogou_source;

--创建textfile数据表
create table tb_sogou_text
(
    stime      string,
    userid     string,
    keyword    string,
    clickorder string,
    url        string
)
    row format delimited fields terminated by '\t'
    stored as textfile;
--	写入TextFile数据表
insert into table tb_sogou_text
select *
from tb_sogou_source;

--todo SequenceFile 适用于小量数据，但查询的列比较多的场景
create table tb_sogou_seq
(
    stime      string,
    userid     string,
    keyword    string,
    clickorder string,
    url        string
)
    row format delimited fields terminated by '\t'
    stored as sequencefile;

insert into table tb_sogou_seq
select *
from tb_sogou_source;

--todo Parquet格式 适用于字段数比较多无更新，只取部分列的查询，不支持事务操作
create table tb_sogou_parquet
(
    stime      string,
    userid     string,
    keyword    string,
    clickorder string,
    url        string
)
    row format delimited fields terminated by '\t'
    stored as parquet;

insert into table tb_sogou_parquet
select *
from tb_sogou_source;

--todo ORC格式 强烈推荐使用不仅 压缩比高、查询效率也高 、支持索引、支持矢量查询、并且是事务操作必须的文件格式
create table tb_sogou_orc
(
    stime      string,
    userid     string,
    keyword    string,
    clickorder string,
    url        string
)
    row format delimited fields terminated by '\t'
    stored as orc;

--todo 直接create table as select也是一个直接转化格式的好方法还不需要指定列名和数据类型
create table tb_sogou_orc1
    row format delimited fields terminated by '\t'
    stored as orc
as
select *
from tb_sogou_source;

insert into table tb_sogou_orc
select *
from tb_sogou_source;
