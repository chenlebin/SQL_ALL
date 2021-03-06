show databases;

------load语法规则----
LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
    [PARTITION (partcol1=val1, partcol2=val2...)];
-- hive3.0新特性指定输入类型和序列化/反序列化方法
LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
    [PARTITION (partcol1=val1, partcol2=val2...)]
    [INPUTFORMAT 'inputformat' SERDE 'serde'] (3.0 or later);


--------练习:Load Data From Local FS or HDFS---------
--step1:建表
--建表student_local 用于演示从本地加载数据
create table student_local
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
) row format delimited fields terminated by ',';
--建表student_HDFS  用于演示从HDFS加载数据
create external table student_HDFS
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
) row format delimited fields terminated by ',';
--建表student_HDFS_p 用于演示从HDFS加载数据到分区表
create table student_HDFS_p
(
    num  int,
    name string,
    sex  string,
    age  int,
    dept string
) partitioned by (country string) row format delimited fields terminated by ',';


--建议使用beeline客户端 可以显示出加载过程日志信息
--step2:加载数据
-- 从本地加载数据  数据位于HS2（node1）本地文件系统  本质是hadoop fs -put上传操作
LOAD DATA LOCAL INPATH '/root/hivedata/students.txt' INTO TABLE student_local;

--从HDFS加载数据  数据位于HDFS文件系统根目录下  本质是hadoop fs -mv 移动操作
--先把数据上传到HDFS上  hadoop fs -put /root/hivedata/students.txt /
LOAD DATA INPATH '/students.txt' INTO TABLE student_HDFS;

----从HDFS加载数据到分区表中并制定分区  数据位于HDFS文件系统根目录下 todo 注意这样设定的分区是静态分区
--先把数据上传到HDFS上 hadoop fs -put /root/hivedata/students.txt /
LOAD DATA INPATH '/students.txt' INTO TABLE student_HDFS_p partition (country = "China");


-------hive 3.0 load命令新特性------------------
-- 当满足条件时会自动将剩下的列当做是分区列进行分区表的数据加载 转化为inset+select
CREATE TABLE if not exists tab1
(
    col1 int,
    col2 int
)
    PARTITIONED BY (col3 int)
    row format delimited fields terminated by ',';

--正常情况下  数据格式如下
-- 11,22
-- 33,44
LOAD DATA LOCAL INPATH '/root/hivedata/xxx.txt' INTO TABLE tab1 partition (col3 = "1");

--在hive3.0之后 新特性可以帮助我们把load改写为insert as select
LOAD DATA LOCAL INPATH '/root/hivedata/tab1.txt' overwrite INTO TABLE tab1;

--tab1.txt内容如下
-- 11,22,1
-- 33,44,2

select *
from tab1;


select *
from tab1;


























