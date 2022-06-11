-----------------导出数据-------------------
--标准语法:
use db_df2;
--todo 导出数据为覆盖重写，所以导出数据时务必注意导出的路径，不能有重要文件，否则后果很严重！！！
INSERT OVERWRITE [LOCAL] DIRECTORY directory1
    [ROW FORMAT row_format] [STORED AS file_format] (Note: Only available starting with Hive 0.11.0)
SELECT... FROM...

--row_format
    : DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]] [COLLECTION ITEMS TERMINATED BY char]
    [MAP KEYS TERMINATED BY char] [LINES TERMINATED BY char]


--导出操作演示
--当前库下已有一张表student
select *
from student;

--1、导出查询结果到HDFS指定目录下
explain extended
insert overwrite directory '/tmp/hive_output/e1'
    row format delimited
    fields terminated by '  '
    stored as orc
select num, name, age
from student
limit 2;

--2、导出时指定分隔符和文件存储格式
insert overwrite directory '/tmp/hive_export/e2'
    row format delimited fields terminated by ','
    stored as orc
select *
from student;

--3、导出数据到本地文件系统指定目录下
insert overwrite local directory '/root/hive_export/e1'
select *
from student;
