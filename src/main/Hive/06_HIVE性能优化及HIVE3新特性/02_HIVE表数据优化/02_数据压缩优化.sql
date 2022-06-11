--todo 数据压缩
use db_df2;
--开启hive中间传输数据压缩功能
--1）开启hive中间传输数据压缩功能
set hive.exec.compress.intermediate=true;
--2）开启mapreduce中map输出压缩功能
set mapreduce.map.output.compress=true;
--3）todo 设置mapreduce中map输出数据的压缩方式
set mapreduce.map.output.compress.codec= org.apache.hadoop.io.compress.SnappyCodec;


--开启Reduce输出阶段压缩
--1）开启hive最终输出数据压缩功能
set hive.exec.compress.output=true;
--2）开启mapreduce最终输出数据压缩
set mapreduce.output.fileoutputformat.compress=true;
--3）todo 设置mapreduce最终数据输出压缩方式
set mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;
--4）设置mapreduce最终数据输出压缩为块压缩
set mapreduce.output.fileoutputformat.compress.type=BLOCK;


--创建表，指定为textfile格式，并使用snappy压缩
drop table tb_sogou_snappy;
create table tb_sogou_snappy
    stored as textfile
as
select *
from tb_sogou_source;

--创建表，指定为orc格式，并使用snappy压缩
create table tb_sogou_orc_snappy
    stored as orc tblproperties ("orc.compress" = "SNAPPY")
as
select *
from tb_sogou_source;
