---------------动态分区插入--------------------
use db_df2;
--背景：静态分区
drop table if exists student_HDFS_p;
create table student_HDFS_p
(
    Sno   int,
    Sname string,
    Sex   string,
    Sage  int,
    Sdept string
) partitioned by (country string) row format delimited fields terminated by ',';
--注意 分区字段country的值是在导入数据的时候手动指定的 China
--人为指定的为静态分区
LOAD DATA INPATH '/students.txt' INTO TABLE student_HDFS_p partition (country = "China");


FROM page_view_stg pvs
INSERT
OVERWRITE
TABLE
page_view
PARTITION
(
dt = '2008-06-08'
,
country
)
SELECT pvs.viewTime,
       pvs.userid,
       pvs.page_url,
       pvs.referrer_url,
       null,
       null,
       pvs.ip,
       pvs.cnt;
--在这里，country分区将由SELECT子句（即pvs.cnt）的最后一列动态创建。
--而dt分区是手动指定写死的。
--如果是nonstrict模式下，dt分区也可以动态创建。

-----------案例：动态分区插入-----------
--1、首先设置动态分区模式为非严格模式 默认已经开启了动态分区功能
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;

--2、当前库下已有一张表student
select *
from student;

--3、创建分区表 以sdept作为分区字段
create table student_partition
(
    Sno   int,
    Sname string,
    Sex   string,
    Sage  int
) partitioned by (Sdept string);

--4、执行动态分区插入操作
insert into table student_partition partition (Sdept)
select num, name, sex, age, dept
from student;
--其中，num,name,sex,age作为表的字段内容插入表中
--dept作为分区字段值

select *
from student_partition;

show partitions student_partition;
